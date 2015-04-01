-module(iorioc_shard).

-export([init/1, stop/1, ping/1, get/5, put/6, list_buckets/1, list_streams/2,
         bucket_size/2, subscribe/5, unsubscribe/4, partition/1]).

-ignore_xref([init/1, stop/1, ping/1, get/5, put/6, list_buckets/1,
              list_streams/2, bucket_size/2, subscribe/5, unsubscribe/4,
              partition/1]).

-include_lib("sblob/include/sblob.hrl").

-record(state, {partition, partition_str, partition_dir, gblobs, chans, base_dir}).

init(Opts) ->
    {shard_lib_partition, Partition} = proplists:lookup(shard_lib_partition, Opts),

    GBlobsOpts = [{resource_handler, iorioc_gblob_server_rhandler}, {kv_mod, rscbag_ets}],
    {ok, Gblobs} = rscbag:init(GBlobsOpts),

    ChansOpts = [{resource_handler, iorioc_smc_rhandler}, {kv_mod, rscbag_ets}],
    {ok, Chans} = rscbag:init(ChansOpts),

    BaseDir0 = proplists:get_value(base_dir, Opts, "."),
    BaseDir = filename:absname(BaseDir0),
    PartitionStr = integer_to_list(Partition),
    PartitionDir = filename:join([BaseDir, PartitionStr]),
    State = #state{partition=Partition, partition_str=PartitionStr,
                   partition_dir=PartitionDir,
                   gblobs=Gblobs, chans=Chans, base_dir=BaseDir},
    {ok, State}.

stop(#state{gblobs=Gblobs, chans=Chans}) ->
    rscbag:stop(Gblobs),
    rscbag:stop(Chans),
    ok.

partition(#state{partition=Partition}) -> Partition.

get(State, Bucket, Stream, From, Count) ->
    Fun = fun (Gblob) -> gblob_server:get(Gblob, From, Count) end,
    {_, Reply, State1} = with_bucket(State, Bucket, Stream, Fun),
    {reply, Reply, State1}.

put(State=#state{chans=Chans}, ReqId, Bucket, Stream, Timestamp, Data) ->
    Fun = fun (Gblob) ->
                  Entry = gblob_server:put(Gblob, Timestamp, Data),
                  {PubR, Chans1} = publish(Chans, Bucket, Stream, Entry),
                  if PubR /= ok ->
                         lager:warning("Publish error ~s/~s: ~p", [Bucket, Stream, PubR]);
                     true -> ok
                  end,
                  {Entry, Chans1}
          end,

    case with_bucket(State, Bucket, Stream, Fun) of
        {ok, {R, Chans1}, S1} ->
            {reply, {ReqId, R}, S1#state{chans=Chans1}};
        {error, R, S1} ->
            {reply, {ReqId, R}, S1}
    end.

list_buckets(State=#state{partition_dir=PartitionDir}) ->
    Buckets = list_bucket_names(PartitionDir),
    {reply, Buckets, State}.

list_streams(State=#state{partition_dir=PartitionDir}, Bucket) ->
    Streams = list_stream_names(PartitionDir, Bucket),
    {reply, Streams, State}.

bucket_size(State=#state{partition_dir=PartitionDir}, Bucket) ->
    Streams = list_stream_names(PartitionDir, Bucket),
    R = lists:foldl(fun (Stream, {TotalSize, Sizes}) ->
                            StreamSize = stream_size(PartitionDir, Bucket, Stream),
                            NewTotalSize = TotalSize + StreamSize,
                            NewSizes = [{Stream, StreamSize}|Sizes],
                            {NewTotalSize, NewSizes}
                    end, {0, []}, Streams),
    {reply, R, State}.

subscribe(State=#state{chans=Chans}, Bucket, Stream, FromSeqNum, Pid) ->
    {Reply, Chans1} = with_channel(Chans, Bucket, Stream,
                          fun (Chann) ->
                                  if FromSeqNum == nil -> ok;
                                     true -> smc:replay(Chann, Pid, FromSeqNum)
                                  end,
                                  smc:subscribe(Chann, Pid),
                                  ok
                          end),
    {reply, Reply, State#state{chans=Chans1}}.

unsubscribe(State=#state{chans=Chans}, Bucket, Stream, Pid) ->
    {Reply, Chans1} = with_channel(Chans, Bucket, Stream,
                                   fun (Chann) ->
                                           smc:unsubscribe(Chann, Pid),
                                           ok
                                   end),
    {reply, Reply, State#state{chans=Chans1}}.

ping(State=#state{partition=Partition}) ->
    {reply, {pong, Partition}, State}.

%% private functions

make_get_bucket_opts(PartitionStr, Bucket, Stream) ->
    fun () ->
            Path = filename:join([PartitionStr, Bucket, Stream]),
            GblobOpts = [],
            GblobServerOpts = [],
            [{path, Path},
             {gblob_opts, GblobOpts},
             {gblob_server_opts, GblobServerOpts}]
    end.

list_dir(Path) ->
    case file:list_dir(Path) of
        {error, enoent} -> [];
        {ok, Names} -> Names
    end.

list_bucket_names(Path) ->
    lists:map(fun list_to_binary/1, list_dir(Path)).

stream_size(Path, Bucket, Stream) ->
    FullPath = filename:join([Path, Bucket, Stream]),
    sblob_util:deep_size(FullPath).

list_stream_names(Path, Bucket) ->
    FullPath = filename:join([Path, Bucket]),
    lists:map(fun list_to_binary/1, list_dir(FullPath)).

get_seqnum(#sblob_entry{seqnum=SeqNum}) -> SeqNum.

with_channel(Chans, Bucket, Stream, Fun) ->
    BufferSize = 50,
    ChanName = list_to_binary(io_lib:format("~s/~s", [Bucket, Stream])),
    GetSeqNum = fun get_seqnum/1,
    ChanOpts = [{buffer_size, BufferSize},
                {name, ChanName},
                {get_seqnum, GetSeqNum}],
    case rscbag:get(Chans, {Bucket, Stream}, ChanOpts) of
        {{ok, _, Chan}, Chans1} ->
            {Fun(Chan), Chans1};
        Error ->
            {Error, Chans}
    end.

publish(Chans, Bucket, Stream, Entry) ->
    with_channel(Chans, Bucket, Stream,
                 fun (Chan) ->
                         try
                             smc:send(Chan, Entry),
                             ok
                         catch
                             Type:Error -> {error, {Type, Error}}
                         end
                 end).

with_bucket(State=#state{partition_dir=PartitionDir, gblobs=Gblobs}, Bucket,
            Stream, Fun) ->
    GetBucketOpts = make_get_bucket_opts(PartitionDir, Bucket, Stream),
    case rscbag:get(Gblobs, {Bucket, Stream}, GetBucketOpts) of
        {{ok, _, Gblob}, Gblobs1} -> {ok, Fun(Gblob), State#state{gblobs=Gblobs1}};
        Error -> {error, Error, State}
    end.

