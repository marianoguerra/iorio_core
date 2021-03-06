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

subscriber() -> subscriber(#{subs => #{}}).

check_subscriber({_Bucket, _Stream}, [], notfound) ->
    true;
check_subscriber({_Bucket, _Stream}, [], LastSeqNum) when LastSeqNum /= notfound ->
    io:format("X"),
    true;
check_subscriber({_Bucket, _Stream}, [#sblob_entry{seqnum=LastSeqNum}|_]=Entries, LastSeqNum) ->
    {_, Status} = lists:foldl(fun
                                  (#sblob_entry{seqnum=SeqNum}, {nil, Status}) ->
                                      {SeqNum, Status};
                                  (#sblob_entry{seqnum=SeqNum}, {PrevSeqNum, Status}) ->
                                      if PrevSeqNum == SeqNum + 1 ->
                                             {SeqNum, Status};
                                         true ->
                                             io:format("~p != ~p + 1", [PrevSeqNum, SeqNum]),
                                             {SeqNum, false}
                                      end
                  end, {nil, true}, Entries),
    Status;
check_subscriber({_Bucket, _Stream}, [#sblob_entry{seqnum=SeqNum}|_], LastSeqNum) ->
    io:format("last seqnums don't match ~p vs ~p~n", [SeqNum, LastSeqNum]),
    false.

check_subscribers(Subs, SeqNums) ->
    maps:fold(fun (K={Bucket, Stream}, V, IsOk) ->
                      KL = {binary_to_list(Bucket), binary_to_list(Stream)},
                      LastSeqNum = maps:get(KL, SeqNums, notfound),
                      if IsOk -> check_subscriber(K, V, LastSeqNum);
                         true -> IsOk
                      end
              end, true, Subs).


subscriber(State=#{subs:=Subs}) ->
    receive
        {subscribe, {{BucketL, StreamL}, Shard}} ->
            Bucket = list_to_binary(BucketL),
            Stream = list_to_binary(StreamL),
            Key = {Bucket, Stream},
            %io:format("subscribing ~p/~p~n", [Bucket, Stream]),
            iorioc:subscribe(Shard, Bucket, Stream, self()),
            Subs1 = maps:put(Key, [], Subs),
            subscriber(State#{subs => Subs1});
        {entry, Bucket, Stream, Entry} ->
            Key = {Bucket, Stream},
            %io:format("got entry for ~p/~p~n", [Bucket, Stream]),
            case maps:get(Key, Subs, notfound) of
                notfound ->
                    io:format("received entry from unsubscribed channel ~p/~p~n",
                              [Bucket, Stream]),
                    subscriber(State);
                Entries ->
                    NewEntries = [Entry|Entries],
                    Subs1 = maps:put(Key, NewEntries, Subs),
                    subscriber(State#{subs=>Subs1})
            end;
        {check, SeqNums, Pid} ->
            Result = check_subscribers(Subs, SeqNums),
            Pid ! Result,
            subscriber(State);
        'EXIT' ->
            ok;
        Other ->
            io:format("Unknown Msg ~p~n", [Other]),
            subscriber(State)
    end.

new_state(Streams, Intervals) ->
    Pid = spawn(fun subscriber/0),
    #{current => [],
      free => Streams,
      seqnums => #{},
      subscriber => Pid,
      ts => Intervals,
      t => 0}.

command() ->
    oneof([
           put_existing_stream(),
           put_new_stream(),
           get_existing_stream(),
           get_existing_stream_out_of_bounds(),
           get_non_existing_stream(),
           check_subscriptions
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
    try
        exec_commands(Commands, State, Shard)
    catch
        T:E ->
            io:format("ERROR: ~p ~p~n~n", [T, E]),
            io:format("  ~p~n", [file_handle_cache:info()]),
            true
    after
        iorioc:stop(Shard),
        maps:get(subscriber, State) ! 'EXIT'
    end.

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
exec_command({put_new, Data}, State=#{subscriber := Subscriber}, Shard) ->
    {NewState, Ts, _Int, Bucket, Stream} = get_new_stream(State),
    Subscriber ! {subscribe, {{Bucket, Stream}, Shard}},
    Ref = make_ref(),
    BData = list_to_binary(Data),
    SeqNum = 1,
    % give time to subscriber to subscribe
    timer:sleep(1),
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
                         dump_processes(),
                         % code to reproduce this issue below, I think is because eviction
                         % removes old events that's why we get less
                         % iorioc_triq:run_commands({[{put_new,"  X  Ov BRrucDn "}, {put," s"}, {put,"xs"}, {get,40}], [{"498lkhWLRN1O96UE74JkZgO9t8K", "pxiB482kxYv4ftNH7LO292s8X1C"}, {"5Ndr7CgXsUZ5IYi7y7jPSc7N4I", "pH31w2Zg935E97q5fV65dCOcTzr2"}, {"6dhL2","55hSyQm7D2zZp"}, {"ksqlD327g3D2e5K","uwCPwyvtBr3K"}, {"xyEbk3yevPP65K5ol17589eIw59x", "Jh2FxcygGuqqeddREV5Rud3z12pwNr"}], [13,29,13,34,9], [{max_items,15},{max_age_ms,0},{max_size_bytes,14}], [{check_interval_ms,37},{max_interval_no_eviction_ms,35}]}).
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
    {length(R) == 0, State};
exec_command(check_subscriptions, State=#{seqnums := SeqNums, subscriber := Subscriber}, _Shard) ->
    Subscriber ! {check, SeqNums, self()},
    receive R -> {R, State}
    after 1000 ->
              io:format("timeout waiting for sub check~n"),
              {false, State}
    end.

dump_processes() ->
    PCount = erlang:system_info(process_count),
    if PCount rem 10 == 0 ->
           Dict = get_info(erlang:processes(), dict:new()),
           DList = dict:to_list(Dict),
           lists:foreach(fun ({Key, Val}) ->
                                 if Val > 100 ->
                                        io:format("~p: ~p~n", [Key, Val]);
                                    true -> ok
                                 end
                         end, DList);
       true ->
           ok
    end.

get_info([], Dict) ->
    Dict;
get_info([P|L], Dict) ->
    case erlang:process_info(P, dictionary) of
        {dictionary, Info} ->
            Initial_call = proplists:get_value('$initial_call', Info, nil),
            Ancestor = proplists:get_value('$ancestors', Info, nil),
            Key = {Initial_call, Ancestor},
            IncrNb = case dict:find(Key, Dict) of
                {ok, Nb} -> Nb + 1;
                error -> 1
            end,
            case Key of
                {nil, nil} ->
                    %io:format("~p~n", [erlang:process_info(P, current_function)]);
                    ok;
                _ -> ok
            end,
            Dict2 = dict:store({Initial_call, Ancestor}, IncrNb, Dict),
            get_info(L, Dict2);
        undefined ->
            IncrNb = case dict:find(undefined, Dict) of
                {ok, Nb} -> Nb + 1;
                error -> 1
            end,
            Dict2 = dict:store(undefined, IncrNb, Dict),
            get_info(L, Dict2)
    end.
