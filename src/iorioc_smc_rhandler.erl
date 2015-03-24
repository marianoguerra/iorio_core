-module(iorioc_smc_rhandler).
-export([init/1, stop/1]).
-ignore_xref([init/1, stop/1]).
-behaviour(rscbag_resource_handler).

init(Opts) ->
    smc:history(Opts).

stop(Smc) ->
    smc:stop(Smc).
