-module(iorioc_shard).

-export([ping/1, get/5, put/5, list_buckets/1, list_streams/2, bucket_size/2,
         subscribe/5, unsubscribe/4,
         stop/1, start_link/1]).

-ignore_xref([get/5, put/5, list_buckets/1, list_streams/2, bucket_size/2,
              stop/1, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-behaviour(gen_server).

-include_lib("sblob/include/sblob.hrl").

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

get(Pid, Bucket, Stream, From, Count) ->
    gen_server:call(Pid, {get, Bucket, Stream, From, Count}).

put(Pid, Bucket, Stream, Timestamp, Data) ->
    gen_server:call(Pid, {put, Bucket, Stream, Timestamp, Data}).

list_buckets(Pid) ->
    gen_server:call(Pid, list_buckets).

list_streams(Pid, Bucket) ->
    gen_server:call(Pid, {list_streams, Bucket}).

bucket_size(Pid, Bucket) ->
    gen_server:call(Pid, {size, Bucket}).

subscribe(Pid, Bucket, Stream, FromSeqNum, SubPid) ->
    gen_server:call(Pid, {subscribe, Bucket, Stream, FromSeqNum, SubPid}).

unsubscribe(Pid, Bucket, Stream, SubPid) ->
    gen_server:call(Pid, {unsubscribe, Bucket, Stream, SubPid}).

ping(Pid) -> gen_server:call(Pid, ping).

stop(Pid) -> gen_server:call(Pid, stop).

-record(state, {partition, partition_str, partition_dir, gblobs, chans, base_dir}).

%% gen_server callbacks

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

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call({get, Bucket, Stream, From, Count}, _From,
            State=#state{partition_str=PartitionStr, gblobs=Gblobs}) ->
    GetBucketOpts = make_get_bucket_opts(PartitionStr, Bucket, Stream),
    {Reply, Gblobs1} = case rscbag:get(Gblobs, {Bucket, Stream}, GetBucketOpts) of
                           {{ok, _, Gblob}, Gblobs11} ->
                               R = gblob_server:get(Gblob, From, Count),
                               {R, Gblobs11};
                           Error -> Error
                       end,
    {reply, Reply, State#state{gblobs=Gblobs1}};

handle_call({put, Bucket, Stream, Timestamp, Data}, _From,
            State=#state{partition_str=PartitionStr,
                         chans=Chans, gblobs=Gblobs}) ->
    GetBucketOpts = make_get_bucket_opts(PartitionStr, Bucket, Stream),
    Temp = case rscbag:get(Gblobs, {Bucket, Stream}, GetBucketOpts) of
               {{ok, _, Gblob}, Gblobs11} ->
                   R = gblob_server:put(Gblob, Timestamp, Data),
                   {PubR, Chans11} = publish(Chans, Bucket, Stream, R),
                   if PubR /= ok ->
                          lager:warning("Publish error ~s/~s: ~p", [Bucket, Stream, PubR]);
                      true -> ok
                   end,
                   {R, Gblobs11, Chans11};
               Error -> {Error, Gblobs, Chans}
           end,
    {Reply, Gblobs1, Chans1} = Temp,
    {reply, Reply, State#state{gblobs=Gblobs1, chans=Chans1}};

handle_call({subscribe, Bucket, Stream, FromSeqNum, Pid}, _From,
            State=#state{chans=Chans}) ->
    {Reply, Chans1} = with_channel(Chans, Bucket, Stream,
                          fun (Chann) ->
                                  if FromSeqNum == nil -> ok;
                                     true -> smc:replay(Chann, Pid, FromSeqNum)
                                  end,
                                  smc:subscribe(Chann, Pid),
                                  ok
                          end),
    {reply, Reply, State#state{chans=Chans1}};

handle_call({unsubscribe, Bucket, Stream, Pid}, _From,
            State=#state{chans=Chans}) ->
    {Reply, Chans1} = with_channel(Chans, Bucket, Stream,
                                   fun (Chann) ->
                                           smc:unsubscribe(Chann, Pid),
                                           ok
                                   end),
    {reply, Reply, State#state{chans=Chans1}};

handle_call({size, Bucket}, _From, State=#state{partition_dir=PartitionDir}) ->
    Streams = list_stream_names(PartitionDir, Bucket),
    R = lists:foldl(fun (Stream, {TotalSize, Sizes}) ->
                            StreamSize = stream_size(PartitionDir, Bucket, Stream),
                            NewTotalSize = TotalSize + StreamSize,
                            NewSizes = [{Stream, StreamSize}|Sizes],
                            {NewTotalSize, NewSizes}
                    end, {0, []}, Streams),
    {reply, R, State};

handle_call(ping, _From, State=#state{partition=Partition}) ->
    {reply, {pong, Partition}, State};

handle_call(list_buckets, _From, State=#state{partition_dir=PartitionDir}) ->
    Buckets = list_bucket_names(PartitionDir),
    {reply, Buckets, State};

handle_call({list_streams, Bucket}, _From, State=#state{partition_dir=PartitionDir}) ->
    Streams = list_stream_names(PartitionDir, Bucket),
    {reply, Streams, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
