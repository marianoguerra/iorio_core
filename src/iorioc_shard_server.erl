-module(iorioc_shard_server).

-export([ping/1, get/5, put/6, list_buckets/1, list_streams/2, bucket_size/2,
         subscribe/5, unsubscribe/4, stop/1, start_link/1]).

-ignore_xref([ping/1, get/5, put/6, list_buckets/1, list_streams/2,
              bucket_size/2, subscribe/5, unsubscribe/4, stop/1,
              start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-behaviour(gen_server).

-include_lib("sblob/include/sblob.hrl").

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

get(Pid, Bucket, Stream, From, Count) ->
    gen_server:call(Pid, {get, Bucket, Stream, From, Count}).

put(Pid, ReqId, Bucket, Stream, Timestamp, Data) ->
    gen_server:call(Pid, {put, ReqId, Bucket, Stream, Timestamp, Data}).

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

%% gen_server callbacks

init(Opts) ->
    iorioc_shard:init(Opts).

handle_call(stop, _From, State) ->
    iorioc_shard:stop(State),
    {stop, normal, stopped, State};

handle_call({get, Bucket, Stream, From, Count}, _From, State) ->
    iorioc_shard:get(State, Bucket, Stream, From, Count);

handle_call({put, ReqId, Bucket, Stream, Timestamp, Data}, _From, State) ->
    iorioc_shard:put(State, ReqId, Bucket, Stream, Timestamp, Data);

handle_call({subscribe, Bucket, Stream, FromSeqNum, Pid}, _From, State) ->
    iorioc_shard:subscribe(State, Bucket, Stream, FromSeqNum, Pid);

handle_call({unsubscribe, Bucket, Stream, Pid}, _From, State) ->
    iorioc_shard:unsubscribe(State, Bucket, Stream, Pid);

handle_call({size, Bucket}, _From, State) ->
    iorioc_shard:bucket_size(State, Bucket);

handle_call(ping, _From, State) ->
    io:format("ping ~p~n", [State]),
    iorioc_shard:ping(State);

handle_call(list_buckets, _From, State) ->
    iorioc_shard:list_buckets(State);

handle_call({list_streams, Bucket}, _From, State) ->
    iorioc_shard:list_streams(State, Bucket).

handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
