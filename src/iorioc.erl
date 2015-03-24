-module(iorioc).
-export([get/4, get/5, put/5, list_buckets/1, list_streams/2,
         subscribe/4, subscribe/5, unsubscribe/4,
         bucket_size/2, start_link/1]).

-ignore_xref([get/4, get/5, put/5, list_buckets/1, list_streams/2,
              bucket_size/2, start_link/1]).

start_link(Opts) ->
    Partitions = proplists:get_value(partitions, Opts, 64),
    SeedNode = proplists:get_value(seednode, Opts, iorioc),
    HashFun = shard_util:new_chash_fun(Partitions, SeedNode),
    ResourceOpts = proplists:get_value(resource_opts, Opts,
                                       [{resource_handler, iorioc_shard_rhandler}]),
    shard:start_link([{resource_opts, ResourceOpts}, {hash_fun, HashFun}]).

get(Shard, Bucket, Stream, From) ->
    get(Shard, Bucket, Stream, From, 1).

get(Shard, Bucket, Stream, From, Count) ->
    MFA = {iorioc_shard, get, [Bucket, Stream, From, Count]},
    shard:handle(Shard, {Bucket, Stream}, MFA).

put(Shard, Bucket, Stream, Timestamp, Data) ->
    MFA = {iorioc_shard, put, [Bucket, Stream, Timestamp, Data]},
    shard:handle(Shard, {Bucket, Stream}, MFA).

list_buckets(Shard) ->
    MFA = {iorioc_shard, list_buckets, []},
    shard:handle_all(Shard, MFA).

list_streams(Shard, Bucket) ->
    MFA = {iorioc_shard, list_streams, [Bucket]},
    shard:handle_all(Shard, MFA).

bucket_size(Shard, Bucket) ->
    MFA = {iorioc_shard, bucket_size, [Bucket]},
    shard:handle_all(Shard, MFA).

subscribe(Shard, Bucket, Stream, Pid) ->
    subscribe(Shard, Bucket, Stream, nil, Pid).

subscribe(Shard, Bucket, Stream, FromSeqNum, Pid) ->
    MFA = {iorioc_shard, subscribe, [Bucket, Stream, FromSeqNum, Pid]},
    shard:handle(Shard, {Bucket, Stream}, MFA).
    
unsubscribe(Shard, Bucket, Stream, Pid) ->
    MFA = {iorioc_shard, unsubscribe, [Bucket, Stream, Pid]},
    shard:handle(Shard, {Bucket, Stream}, MFA).
