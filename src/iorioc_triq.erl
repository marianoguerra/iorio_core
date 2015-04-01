-module(iorioc_triq).
-export([exec_commands/0, run_commands/1]).

-include_lib("triq/include/triq.hrl").
-include_lib("sblob/include/sblob.hrl").

char_data() -> oneof([int($a, $z), int($A, $Z), $ ]).

name_data() -> oneof([int($a, $z), int($A, $Z), int($0, $9)]).

data() -> ?LET(Length, int(0, 16), begin vector(Length, char_data()) end).

name() -> ?LET(Length, int(1, 32), begin vector(Length, name_data()) end).

count () -> ?LET(Int, pos_integer(),
                 begin
                     if
                         Int == 0 -> Int + 1;
                         true -> Int
                     end
                 end).

%timestamp() -> int(0, 1500000000000).

stream() -> {name(), name()}.

put_existing_stream() -> {put, data()}.
put_new_stream() -> {put_new, data()}.

get_existing_stream() -> {get, count()}.
get_existing_stream_out_of_bounds() -> {get_out_of_bounds, count()}.

get_non_existing_stream() -> {get_non_existing, count()}.

new_state(Streams, Intervals) ->
    #{current => [],
      free => Streams,
      seqnums => #{},
      ts => Intervals,
      t => 0}.

command() ->
    oneof([
           put_existing_stream(),
           put_new_stream(),
           get_existing_stream(),
           get_existing_stream_out_of_bounds(),
           get_non_existing_stream()
          ]).

streams(Length) -> vector(Length rem 256, stream()).

commands(Length) -> vector(Length, command()).

intervals(Length) -> vector(Length, pos_integer()).

gblob_opts() ->
    ?LET({PosInteger1, PosInteger2, MaxAgeMs}, {pos_integer(), pos_integer(), pos_integer()},
         begin
             [{max_items, PosInteger1 + 1}, 
              {max_age_ms, MaxAgeMs},
              {max_size_bytes, PosInteger2 + 1}]
         end).


gblob_server_opts() ->
    [{check_interval_ms, pos_integer()},
     {max_interval_no_eviction_ms, pos_integer()}].

commands_data() ->
    ?LET(Length, pos_integer(),
        begin
            {commands(Length), streams(Length), intervals(Length),
             gblob_opts(), gblob_server_opts()}
        end).

exec_commands() ->
    ?FORALL(CommandsData, commands_data(), begin run_commands(CommandsData) end).

run_commands({Commands, Streams, Intervals, GblobOpts, GblobServerOpts}) ->
    file_handle_cache:start_link(),
    NowStr = integer_to_list(sblob_util:now()),
    Path = filename:join(["tmp", NowStr]),
    GblobConfigFun = fun (_PartitionStr, _Bucket, _Stream, SPath) ->
                             [{path, SPath},
                              {gblob_opts, GblobOpts},
                              {gblob_server_opts, GblobServerOpts}]
                     end,
    ShardOpts = [{base_dir, Path}, {gblob_config_fun, GblobConfigFun}],
    {ok, Shard} = iorioc:start_link([{shard_opts, ShardOpts}]),
    UniqueStreams = sets:to_list(sets:from_list(Streams)),
    State = new_state(UniqueStreams, Intervals),
    R = exec_commands(Commands, State, Shard),
    iorioc:stop(Shard),
    R.

exec_commands([], _State, _Shard) ->
    true;
exec_commands([Command|Commands], State, Shard) ->
    case exec_command(Command, State, Shard) of
        {true, NewState} -> exec_commands(Commands, NewState, Shard);
        {false, _LastState} -> false
    end.

%p(Thing) -> io:format("~p~n", [Thing]).

get_existing_stream(State=#{ts := [Int|Ints], t := T, current := Current}) ->
    NewT = T + Int,
    Len = length(Current),
    Index = (T rem Len) + 1,
    {Bucket, Stream} = lists:nth(Index, Current),
    NewState = State#{ts => Ints, t => NewT},
    {NewState, NewT, Int, Bucket, Stream}.

get_new_stream(State=#{ts := [Int|Ints], t := T, current := Current, free :=
                       [StreamId|Free]}) ->
    NewT = T + Int,
    NewState = State#{current => [StreamId|Current], free := Free, ts => Ints,
                      t => NewT},
    {Bucket, Stream} = StreamId,
    {NewState, NewT, Int, Bucket, Stream}.

get_expected_seqnum(#{seqnums := SeqNums}, Bucket, Stream) ->
    case maps:find({Bucket, Stream}, SeqNums) of
        {ok, CurrentSeqNum} -> CurrentSeqNum + 1;
        error -> 1
    end.

get_last_seqnum(#{seqnums := SeqNums}, Bucket, Stream) ->
    case maps:find({Bucket, Stream}, SeqNums) of
        {ok, CurrentSeqNum} -> CurrentSeqNum;
        error -> 0
    end.

update_seqnum(State=#{seqnums := SeqNums}, Bucket, Stream, SeqNum) ->
    NewSeqNums = maps:put({Bucket, Stream}, SeqNum, SeqNums),
    State#{seqnums => NewSeqNums}.

exec_command(nop, State, _Shard) ->
    {true, State};
exec_command({put_new, _Data}, State=#{free := []}, _Shard) ->
    {true, State};
exec_command({put_new, Data}, State, Shard) ->
    {NewState, Ts, _Int, Bucket, Stream} = get_new_stream(State),
    Ref = make_ref(),
    BData = list_to_binary(Data),
    SeqNum = 1,
    case iorioc:put(Shard, Ref, Bucket, Stream, Ts, BData) of
        % TODO: seqnum
        {Ref, #sblob_entry{timestamp=Ts, data=BData, seqnum=SeqNum}} ->
            {true, update_seqnum(NewState, Bucket, Stream, SeqNum)};
        _Other ->
            {false, State}
    end;
% if there are no current streams the commands from below can't execute
exec_command(_, State=#{current := []}, _Shard) ->
    {true, State};
exec_command({put, Data}, State, Shard) ->
    {NewState, Ts, _Int, Bucket, Stream} = get_existing_stream(State),
    Ref = make_ref(),
    BData = list_to_binary(Data),
    SeqNum = get_expected_seqnum(State, Bucket, Stream),
    case iorioc:put(Shard, Ref, Bucket, Stream, Ts, BData) of
        {Ref, #sblob_entry{timestamp=Ts, data=BData, seqnum=SeqNum}} ->
            {true, update_seqnum(NewState, Bucket, Stream, SeqNum)};
        {Ref, #sblob_entry{timestamp=Ts, data=BData, seqnum=DiffSeqNum}} ->
            io:format("seqnum ~p != ~p~n", [SeqNum, DiffSeqNum]),
            {false, State};
        _Other ->
            {false, State}
    end;
exec_command({get, Count}, State, Shard) ->
    {NewState, _Ts, _Int, Bucket, Stream} = get_existing_stream(State),
    LastSeqNum = get_last_seqnum(State, Bucket, Stream),
    From0 = LastSeqNum - Count,
    {From, ExpectedCount, Branch} = if
                                From0 < 0 ->
                                    {0, LastSeqNum, from_lt_0};
                                true ->
                                    if Count > (LastSeqNum - From0) ->
                                           {From0, LastSeqNum - From0 - 1, c_lt_lsn};
                                       true ->
                                           {From0, Count, c_ge_lsn}
                                    end
                            end,
    R = iorioc:get(Shard, Bucket, Stream, From, Count),
    GotCount = length(R),
    IsOk = if GotCount == ExpectedCount -> true;
              GotCount < ExpectedCount andalso GotCount > 0 ->
                  #sblob_entry{seqnum=LastSeqNumReceived} = lists:last(R),
                  if LastSeqNum == LastSeqNumReceived ->
                         io:format("*"),
                         true;
                     true -> false
                  end;
              true -> false
           end,
    if not IsOk ->
           io:format("error getting from ~p count ~p, expected ~p, got ~p (~p)~n",
                     [From, Count, ExpectedCount, GotCount, Branch]);
       true ->
           ok
    end,
    {IsOk, NewState};
exec_command({get_out_of_bounds, Count}, State, Shard) ->
    {NewState, _Ts, _Int, Bucket, Stream} = get_existing_stream(State),
    LastSeqNum = get_last_seqnum(State, Bucket, Stream),
    From = LastSeqNum + Count,
    R = iorioc:get(Shard, Bucket, Stream, From, Count),
    {length(R) == 0, NewState};
exec_command({get_non_existing, Count}, State=#{free := []}, Shard) ->
    From = 0,
    R = iorioc:get(Shard, "this cant be a bucket", "this cant be a stream", From, Count),
    {length(R) == 0, State};
exec_command({get_non_existing, Count}, State=#{free := [StreamId|_]}, Shard) ->
    {Bucket, Stream} = StreamId,
    From = 0,
    R = iorioc:get(Shard, Bucket, Stream, From, Count),
    {length(R) == 0, State}.
