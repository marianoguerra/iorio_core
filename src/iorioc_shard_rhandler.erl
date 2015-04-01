-module(iorioc_shard_rhandler).
-export([init/1, stop/1]).
-ignore_xref([init/1, stop/1]).
-behaviour(rscbag_resource_handler).

init(Opts) ->
    iorioc_shard_server:start_link(Opts).

stop(Pid) ->
    iorioc_shard_server:stop(Pid).
