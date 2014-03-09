-module(examples).

-export([start/0, stop/0, create_schema/0, drop_schema/0, load_test/1,  load_test/2, counters_test/1, counters_test/2,
         load_test_multi/2, load_test_multi/3, load_test_stream/1, load_test_stream/2, load_test_multi_stream/2, load_test_multi_stream/3, prepare_data/2, load_test_multi_stream_multi/3, load_test_multi_stream_multi/4]).

-include_lib("teledamus/include/native_protocol.hrl").

start() ->
  application:ensure_started(teledamus).

stop() ->
  application:stop(teledamus).

create_schema() ->
  Con = teledamus:get_connection(),
  teledamus:query(Con, "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"),
  {keyspace, "examples"} = teledamus:query(Con, "USE examples"),
  {created, "examples", "profiles"} = teledamus:query(Con, "CREATE TABLE IF NOT EXISTS profiles(s_id varchar, c_id varchar, v_id varchar, o_id varchar, PRIMARY KEY(s_id)) WITH caching = 'all' AND compaction =  { 'class' : 'SizeTieredCompactionStrategy' } AND compression = { 'sstable_compression' : 'SnappyCompressor', 'chunk_length_kb' : 32, 'crc_check_chance' : 0.5}" ),
  {created, "examples", "counters"} = teledamus:query(Con, "CREATE TABLE IF NOT EXISTS counters(c1 counter, c2 counter, id1 varchar, id2 varchar, PRIMARY KEY(id1, id2))"),
  teledamus:release_connection(Con),
  ok.

drop_schema() ->
  Con = teledamus:get_connection(),
  {dropped, "examples", []} = teledamus:query(Con, "DROP KEYSPACE examples"),
  teledamus:release_connection(Con),
  ok.

counters_test(M, N) ->
  Cons = lists:map(fun(_) ->
    Con = teledamus:get_connection(),
    {Q, _, _} = teledamus:prepare_query(Con, "UPDATE examples.counters SET c1 = c1 + 1 WHERE id1 = '123' AND id2 = 'ABC'"),
    {Con, Q}
  end, lists:seq(1, M)),
  Parent = self(),
  T0 = millis(),
  lists:foreach(fun({Con, Q}) -> spawn(fun() -> R = counter_test_int(Con, Q, N) / N, Parent ! R end) end, Cons),
  R = wait_for_N(M),
  T1 = millis(),
  lists:foreach(fun({C, _}) -> teledamus:release_connection(C) end, Cons),
  {(M*N) / (T1-T0) * 1000, R}.

counters_test(N) ->
  Con = teledamus:get_connection(),
  teledamus:query(Con, "USE examples"),
  {Q, _, _} = teledamus:prepare_query(Con, "UPDATE counters SET c1 = c1 + 1 WHERE id1 = '123' AND id2 = 'ABC'"),
  T0 = millis(),
  counter_test_int(Con, Q, N),
  T1 = millis(),
  teledamus:release_connection(Con),
  N / (T1-T0) * 1000.

counter_test_int(_, _, 0) ->
  0;
counter_test_int(Con, Q, N) ->
  T0 = millis(),
  R = try
    teledamus:execute_query(Con, Q, #query_params{consistency_level = one}, 1000),
    millis()
  catch
    E: EE ->
      T1 = millis(),
      error_logger:error_msg("~p: Exception ~p:~p, stack=~p", [self(), E, EE, erlang:get_stacktrace()]),
      T1
  end - T0,
  R + counter_test_int(Con, Q, N - 1).

load_test(Count) ->
  load_test(Count, undefined).

load_test(X, BatchSize) ->
  load_test(0, X, BatchSize).

load_test(Id, X, BatchSize)  ->
  Con = case teledamus:get_connection() of
    {error, X} ->
      error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
      throw(connection_error);
    C -> C
  end,
  teledamus:query(Con, "USE examples"),
	Data = case is_integer(X) of
    true -> prepare_data(X, 1);
    _ -> [X]
  end,
  T0 = millis(),
  {PrepStmt, _, _} = teledamus:prepare_query(Con, "INSERT INTO profiles(s_id, c_id, v_id, o_id) VALUES(?, ?, ?, ?)"),
  R = try
    load_test_int(Con, PrepStmt, hd(Data), BatchSize)
  catch
    E:EE ->
      error_logger:error_msg("~p -> ~p:~p, stack=~p~n", [Id, E, EE, erlang:get_stacktrace()])
  end,
  T1 = millis(),
  teledamus:release_connection(Con),
  {{tm, T1 - T0}, R}.


load_test_stream(Count) ->
  load_test_stream(Count, undefined).

load_test_stream(Count, BatchSize) ->
  Con = case teledamus:get_connection() of
          {error, X} ->
            error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
            throw(connection_error);
          C -> C
        end,
	[Data] = prepare_data(Count, 1),
  R = load_test_stream(Data, Con, BatchSize),
  teledamus:release_connection(Con),
  R.

load_test_stream(Data, Con, BatchSize) ->
  Stream = teledamus:new_stream(Con),
  {keyspace, "examples"} = teledamus:query(Stream, "USE examples"),
  {PrepStmt, _, _} = teledamus:prepare_query(Stream, "INSERT INTO profiles(s_id, c_id, v_id, o_id) VALUES(?, ?, ?, ?)"),
	{DT, LAT} = try
		T0 = millis(),
		SUM_LAT = load_test_int(Stream, PrepStmt, Data, BatchSize),
		T1 = millis(),
		{T1 - T0, round(SUM_LAT / length(Data))}
  catch
    E:EE ->
      error_logger:error_msg("~p:~p, stack=~p~n", [E, EE, erlang:get_stacktrace()])
  end,
  teledamus:release_stream(Stream),
  [{tm, DT}, {lat, LAT}].


load_test_multi_stream_multi(StCnt, PrCnt, Count) ->
  load_test_multi_stream_multi(StCnt, PrCnt, Count, undefined).

load_test_multi_stream_multi(StCnt, PrCnt, Count, BatchSize) ->
  Ds = lists:map(fun(_I) ->
    Con = case teledamus:get_connection() of
      {error, X} ->
        error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
        throw(connection_error);
      CN -> CN
    end,
    Data = prepare_data(Count, StCnt),
    {Con, Data}
  end, lists:seq(1, PrCnt)),
  T0 = millis(),
  lists:foreach(fun({C, Dat}) -> spawn_load_tests_stream(Dat, C, BatchSize) end, Ds),
  R = wait_for_N(PrCnt * StCnt),
  T1 = millis(),
  lists:foreach(fun({C, _Dat}) -> teledamus:release_connection(C) end, Ds),
  {{total, T1- T0}, R}.


load_test_multi_stream(N, Count) ->
  load_test_multi_stream(N, Count, undefined).

load_test_multi_stream(N, Count, BatchSize) ->
  Con = case teledamus:get_connection() of
          {error, X} ->
            error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
            throw(connection_error);
          C -> C
        end,
	Data = prepare_data(Count, N),
  T = millis(),
  spawn_load_tests_stream(Data, Con, BatchSize),
  R = wait_for_N(N),
	T1 = millis(),
  teledamus:release_connection(Con),
  {{total, T1 - T}, R}.


%% load_test_multi_stream(Data) ->
%% 	Con = case teledamus:get_connection() of
%% 					{error, X} ->
%% 						error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
%% 						throw(connection_error);
%% 					C -> C
%% 				end,
%% 	T = millis(),
%% 	spawn_load_tests_stream(Data, Con, 1),
%% 	R = wait_for_N(length(Data)),
%% 	T1 = millis(),
%% 	teledamus:release_connection(Con),
%% 	{{total, T1 - T}, R}.

load_test_multi(N, Count) ->
  load_test_multi(N, Count, undefined).

load_test_multi(N, Count, BatchSize) ->
	Data = prepare_data(Count, N),
  T = millis(),
  spawn_load_tests(Data, BatchSize),
  R = wait_for_N(N),
  {{total, millis() - T}, R}.

wait_for_N(0) -> [];
wait_for_N(N) ->
  XX = receive
    X -> X
  end,
  [XX | wait_for_N(N - 1)].

spawn_load_tests(Data) ->
  spawn_load_tests(Data, undefined).

spawn_load_tests([], _BatchSize) -> ok;
spawn_load_tests([Data | T], BatchSize) ->
  Parent = self(),
  spawn(fun() ->
    R = load_test(Data, BatchSize),
    Parent ! R
  end),
  spawn_load_tests(T, BatchSize).

spawn_load_tests_stream([], _Con, _BatchSize) -> ok;
spawn_load_tests_stream([Data | T], Con, BatchSize) ->
  Parent = self(),
  spawn(fun() ->
    R = load_test_stream(Data, Con, BatchSize),
    Parent ! R
  end),
  spawn_load_tests_stream(T, Con, BatchSize).

p(Str, Id, Count) -> {varchar, Str ++ integer_to_list(Id) ++ "_" ++ integer_to_list(Count)}.

mk_batch_query(Stmt, L) ->
  QS = lists:map(fun(P) ->
    #query_params{bind_values = VS} = P,
    {Stmt, VS}
  end, L),
  #batch_query{batch_type = ?BATCH_UNLOGGED, consistency_level = one, queries = QS}.

load_test_int_batch(_Con, _PrepStmt, [], _BatchSize) -> 0;
load_test_int_batch(Con, PrepStmt, L, BatchSize) ->
  {H, T} = case length(L) < BatchSize of
    true -> {L, []};
    false -> lists:split(BatchSize, L)
  end,
  Batch = mk_batch_query(PrepStmt, H),
  T0 = millis(),
  try
    teledamus:batch_query(Con, Batch, 5000)
  catch
    E:EE ->
      error_logger:error_msg("~p: Exception ~p:~p, stack=~p", [self(), E, EE, erlang:get_stacktrace()])
  end,
  T1 = millis(),
  R = T1 - T0,
  R + load_test_int_batch(Con, PrepStmt, T, BatchSize).

load_test_int(Con, PrepStmt, L, undefined) ->
  load_test_int_single(Con, PrepStmt, L);
load_test_int(Con, PrepStmt, L, BatchSize) ->
  load_test_int_batch(Con, PrepStmt, L, BatchSize).

load_test_int_single(_Con, _PrepStmt, []) -> 0;
load_test_int_single(Con, PrepStmt, [H | T]) ->
	T0 = millis(),
  try
    teledamus:execute_query(Con, PrepStmt, H, 5000)
  catch
    E:EE ->
      error_logger:error_msg("~p: Exception ~p:~p, stack=~p", [self(), E, EE, erlang:get_stacktrace()])
  end,
	T1 = millis(),
  R = T1 - T0,
  R + load_test_int_single(Con, PrepStmt, T).

millis() ->
  {X, _} = statistics(wall_clock),
  X.



for_id(Count, Id) ->
  lists:map(fun(X) -> #query_params{consistency_level = one, bind_values = [p("s_id_", Id, X), p("c_id_", Id, X), p("v_id_", Id, X), p("o_id_", Id, X)]} end, lists:seq(1, Count)).

prepare_data(Count, Parts) ->
	lists:map(fun(Id) -> for_id(Count, Id) end, lists:seq(1, Parts)).