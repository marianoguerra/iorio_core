-module(iorioc).
-export([ping/1, get/4, get/5, put/6, list_buckets/1, list_streams/2,
         subscribe/4, subscribe/5, unsubscribe/4,
         bucket_size/2, start_link/1, stop/1]).

-ignore_xref([ping/1, get/4, get/5, put/6, list_buckets/1, list_streams/2,
              subscribe/4, subscribe/5, unsubscribe/4,
              bucket_size/2, start_link/1, stop/1]).

start_link(Opts) ->
    Partitions = proplists:get_value(partitions, Opts, 64),
    SeedNode = proplists:get_value(seednode, Opts, iorioc),
    HashFun = shard_util:new_chash_fun(Partitions, SeedNode),
    ResourceOpts = proplists:get_value(rscbag_opts, Opts,
                                       [{resource_handler, iorioc_shard_rhandler}]),
    ShardOpts = proplists:get_value(shard_opts, Opts, []),
    shard:start_link([{rscbag_opts, ResourceOpts},
                      {shard_opts, ShardOpts},
                      {hash_fun, HashFun}]).

stop(Shard) ->
    shard:stop(Shard).

get(Shard, Bucket, Stream, From) ->
    get(Shard, Bucket, Stream, From, 1).

get(Shard, Bucket, Stream, From, Count) ->
    MFA = {iorioc_shard_server, get, [Bucket, Stream, From, Count]},
    shard:handle(Shard, key(Bucket, Stream), MFA).

put(Shard, ReqId, Bucket, Stream, Timestamp, Data) ->
    MFA = {iorioc_shard_server, put, [ReqId, Bucket, Stream, Timestamp, Data]},
    shard:handle(Shard, key(Bucket, Stream), MFA).

list_buckets(Shard) ->
    MFA = {iorioc_shard_server, list_buckets, []},
    shard:handle_all(Shard, MFA).

list_streams(Shard, Bucket) ->
    MFA = {iorioc_shard_server, list_streams, [Bucket]},
    shard:handle_all(Shard, MFA).

bucket_size(Shard, Bucket) ->
    MFA = {iorioc_shard_server, bucket_size, [Bucket]},
    shard:handle_all(Shard, MFA).

subscribe(Shard, Bucket, Stream, Pid) ->
    subscribe(Shard, Bucket, Stream, nil, Pid).

subscribe(Shard, Bucket, Stream, FromSeqNum, Pid) ->
    MFA = {iorioc_shard_server, subscribe, [Bucket, Stream, FromSeqNum, Pid]},
    shard:handle(Shard, key(Bucket, Stream), MFA).

unsubscribe(Shard, Bucket, Stream, Pid) ->
    MFA = {iorioc_shard_server, unsubscribe, [Bucket, Stream, Pid]},
    shard:handle(Shard, key(Bucket, Stream), MFA).

ping(Shard) ->
    MFA = {iorioc_shard_server, ping, []},
    shard:handle(Shard, {<<"ping">>, now()}, MFA).

% private
key(Bucket, Stream) when is_list(Bucket) ->
    key(list_to_binary(Bucket), Stream);
key(Bucket, Stream) when is_list(Stream) ->
    key(Bucket, list_to_binary(Stream));
key(Bucket, Stream) ->
    {Bucket, Stream}.

