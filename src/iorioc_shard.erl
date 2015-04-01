-module(iorioc_shard).

-export([init/1, stop/1, ping/1, get/5, put/6, put/7, list_buckets/1, list_streams/2,
         bucket_size/2, subscribe/5, unsubscribe/4, partition/1, is_empty/1,
         delete/1, has_bucket/2, maybe_evict/2, foldl_gblobs/3]).

-ignore_xref([init/1, stop/1, ping/1, get/5, put/6, put/7, list_buckets/1,
              list_streams/2, bucket_size/2, subscribe/5, unsubscribe/4,
              partition/1, is_empty/1, delete/1, has_bucket/2, maybe_evict/2,
              foldl_gblobs/3]).

-include_lib("sblob/include/sblob.hrl").

-record(state, {partition, partition_str, partition_dir, gblobs, chans,
                base_dir,
                writer,
                next_bucket_index=1,
                max_bucket_time_no_evict_ms=60000,
                max_bucket_size_bytes=52428800}).

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

    % TODO: use a real process here and have a supervisor
    WriterPid = spawn(fun task_queue_runner/0),

    State = #state{partition=Partition, partition_str=PartitionStr,
                   partition_dir=PartitionDir, writer=WriterPid,
                   gblobs=Gblobs, chans=Chans, base_dir=BaseDir},
    {ok, State}.

stop(#state{gblobs=Gblobs, chans=Chans}) ->
    rscbag:stop(Gblobs),
    rscbag:stop(Chans),
    ok.

partition(#state{partition=Partition}) -> Partition.
is_empty(State=#state{partition_dir=Path}) ->
        (not filelib:is_dir(Path)) orelse (length(list_bucket_names(State)) == 0).

delete(#state{partition_dir=Path}) ->
    sblob_util:remove(Path).

has_bucket(#state{partition_dir=Path}, Bucket) ->
    BucketPath = filename:join([Path, Bucket]),
    filelib:is_dir(BucketPath).

get(State, Bucket, Stream, From, Count) ->
    Fun = fun (Gblob) -> gblob_server:get(Gblob, From, Count) end,
    {_, Reply, State1} = with_bucket(State, Bucket, Stream, Fun),
    {reply, Reply, State1}.

put(State, ReqId, Bucket, Stream, Timestamp, Data) ->
    put(State, ReqId, Bucket, Stream, Timestamp, Data, true).

put(State=#state{chans=Chans}, ReqId, Bucket, Stream, Timestamp, Data, Publish) ->
    Fun = fun (Gblob) ->
                  Entry = gblob_server:put(Gblob, Timestamp, Data),
                  Chans1 = if Publish ->
                                 {PubR, Chans11} = publish(Chans, Bucket,
                                                           Stream, Entry),
                                 if PubR /= ok ->
                                        lager:warning("Publish error ~s/~s: ~p",
                                                      [Bucket, Stream, PubR]);
                                    true -> ok
                                 end,
                                 Chans11;
                             true -> Chans
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

maybe_evict(State=#state{partition=Partition,
                         next_bucket_index=NextBucketIndex,
                         max_bucket_time_no_evict_ms=MaxTimeMsNoEviction,
                         max_bucket_size_bytes=MaxBucketSize},
            EvictFun) ->

    BucketNames = lists:sort(list_bucket_names(State)),
    BucketCount = length(BucketNames),
    NewNextIndex = if BucketCount == 0 ->
                          NextBucketIndex;
                      BucketCount > NextBucketIndex ->
                          1;
                      true ->
                          NextBucketIndex + 1
                   end,

    Result = if NextBucketIndex > BucketCount ->
                    lager:debug("no eviction, no buckets in vnode ~p",
                                [Partition]),
                    ok;
                true ->
                    BucketName = lists:nth(NextBucketIndex, BucketNames),
                    EvictFun(BucketName, Partition, MaxBucketSize,
                             MaxTimeMsNoEviction)
             end,
    NewState = State#state{next_bucket_index=NewNextIndex},
    {Result, NewState}.

foldl_gblobs(#state{partition_dir=Path}, Fun, Acc0) ->
    GblobNames = list_gblob_names(Path),
    lists:foldl(fun ({BucketName, GblobName}, AccIn) ->
                        BucketPath = filename:join([Path, BucketName, GblobName]),
                        Gblob = gblob:open(BucketPath, []),
                        AccOut = Fun({BucketName, Gblob}, AccIn),
                        gblob:close(Gblob),

                        AccOut
                end, Acc0, GblobNames).

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

list_gblob_names(Path) ->
    BucketNames = list_bucket_names(Path),
    lists:foldl(fun (BucketName, AccIn) ->
                        BucketPath = filename:join([Path, BucketName]),
                        GblobNames = list_dir(BucketPath),
                        lists:foldl(fun (StreamName, Items) ->
                                            StreamNameBin = list_to_binary(StreamName),
                                            [{BucketName, StreamNameBin}|Items]
                                    end, AccIn, GblobNames)
                end, [], BucketNames).

task_queue_runner() ->
    receive F ->
                try F()
                catch T:E -> lager:warning("error running task ~p ~p", [T, E])
                after task_queue_runner()
                end
    end.
