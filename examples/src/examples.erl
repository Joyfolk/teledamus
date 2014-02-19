-module(examples).
-author("Mikhail.Turnovskiy").

-export([start/0, stop/0, create_schema/0, drop_schema/0, load_test/1, load_test_multi/2, load_test_stream/1, load_test_multi_stream/2, load_test_multi_stream/1, prepare_data/2]).

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
  teledamus:release_connection(Con),
  ok.

drop_schema() ->
  Con = teledamus:get_connection(),
  {dropped, "examples", []} = teledamus:query(Con, "DROP KEYSPACE examples"),
  teledamus:release_connection(Con),
  ok.

load_test(Count) ->
  load_test(0, Count).

load_test(Id, Count) ->
  Con = case teledamus:get_connection() of
    {error, X} ->
      error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
      throw(connection_error);
    C -> C
  end,
  teledamus:query(Con, "USE examples"),
	Data = prepare_data(Count, 1),
  T0 = millis(),
  {PrepStmt, _, _} = teledamus:prepare_query(Con, "INSERT INTO profiles(s_id, c_id, v_id, o_id) VALUES(?, ?, ?, ?)"),
  try
    load_test_int(Con, PrepStmt, Data)
  catch
    E:EE ->
      error_logger:error_msg("~p -> ~p:~p, stack=~p~n", [Id, E, EE, erlang:get_stacktrace()])
  end,
  T1 = millis(),
  Cnt = case teledamus:query(Con, "SELECT count(*) FROM profiles", #query_params{consistency_level = one}, infinity) of
    {_, _, XX} -> XX;
    Err -> Err
  end,
  T2 = millis(),
  teledamus:release_connection(Con),
  {load_test, [{load_time, T1 - T0}, {count_time, T2 - T1}, {count, Cnt}]}.



load_test_stream(Count) ->
  Con = case teledamus:get_connection() of
          {error, X} ->
            error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
            throw(connection_error);
          C -> C
        end,
	Data = prepare_data(Count, 1),
  R = load_test_stream(Data, Con),
  teledamus:release_connection(Con),
  R.

load_test_stream(Data, Con) ->
  Stream = teledamus:new_stream(Con),
  {keyspace, "examples"} = teledamus:query(Stream, "USE examples"),
  {PrepStmt, _, _} = teledamus:prepare_query(Stream, "INSERT INTO profiles(s_id, c_id, v_id, o_id) VALUES(?, ?, ?, ?)"),
	{DT, LAT} = try
		T0 = millis(),
		SUM_LAT = load_test_int(Stream, PrepStmt, Data),
		T1 = millis(),
		{T1 - T0, round(SUM_LAT / length(Data))}
  catch
    E:EE ->
      error_logger:error_msg("~p:~p, stack=~p~n", [E, EE, erlang:get_stacktrace()])
  end,
%%   Cnt = case teledamus:query(Stream, "SELECT count(*) FROM profiles", #query_params{consistency_level = one}, infinity) of
%%           {_, _, XX} -> XX;
%%           Err -> Err
%%         end,
%%   T2 = millis(),
  teledamus:release_stream(Stream),
  [{tm, DT}, {lat, LAT}].


load_test_multi_stream(N, Count) ->
  Con = case teledamus:get_connection() of
          {error, X} ->
            error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
            throw(connection_error);
          C -> C
        end,
	Data = prepare_data(Count, N),
  T = millis(),
  spawn_load_tests_stream(Data, Con),
  R = wait_for_N(N),
	T1 = millis(),
  teledamus:release_connection(Con),
  {{total, T1 - T}, R}.


load_test_multi_stream(Data) ->
	Con = case teledamus:get_connection() of
					{error, X} ->
						error_logger:error_msg("Connection error: ~p, ~p", [X, erlang:get_stacktrace()]),
						throw(connection_error);
					C -> C
				end,
	T = millis(),
	spawn_load_tests_stream(Data, Con),
	R = wait_for_N(length(Data)),
	T1 = millis(),
	teledamus:release_connection(Con),
	{{total, T1 - T}, R}.

load_test_multi(N, Count) ->
	Data = prepare_data(Count, N),
  T = millis(),
  spawn_load_tests(Data),
  R = wait_for_N(N),
  {{total, millis() - T}, R}.

wait_for_N(0) -> [];
wait_for_N(N) ->
%%   error_logger:info_msg("Waiting for ~p~n", [N]),
  XX = receive
    X ->
%%       error_logger:info_msg("~p: Received ~p~n", [N, X]),
      X
  end,
  [XX | wait_for_N(N - 1)].

spawn_load_tests([]) -> ok;
spawn_load_tests([Data | T]) ->
  Parent = self(),
  spawn(fun() ->
%%     error_logger:info_msg("Spawning ~p~n", [N]),
    R = load_test(Data),
%%     error_logger:info_msg("Completed ~p:~p~n", [N, R]),
    Parent ! R
  end),
  spawn_load_tests(T).

spawn_load_tests_stream([], _Con) -> ok;
spawn_load_tests_stream([Data | T], Con) ->
  Parent = self(),
  spawn(fun() ->
%%     error_logger:info_msg("Spawning ~p~n", [N]),
    R = load_test_stream(Data, Con),
%%     error_logger:info_msg("Completed ~p:~p~n", [N, R]),
    Parent ! R
  end),
  spawn_load_tests_stream(T, Con).

p(Str, Id, Count) -> {varchar, Str ++ integer_to_list(Id) ++ "_" ++ integer_to_list(Count)}.

load_test_int(_Con, _PrepStmt, []) -> 0;
load_test_int(Con, PrepStmt, [H | T]) ->
	T0 = millis(),
  try
%% 		error_logger:info_msg("lti:exec:s id=~p", [Id]),
    teledamus:execute_query(Con, PrepStmt, H, 5000)
  catch
    E:EE ->
      error_logger:error_msg("~p: Exception ~p:~p, stack=~p", [self(), E, EE, erlang:get_stacktrace()])
  end,
	T1 = millis(),
%% 		error_logger:info_msg("lti:exec:f id=~p, dt=~p", [Id, T1 - T0]),
  R = T1 - T0,
  R + load_test_int(Con, PrepStmt, T).

millis() ->
  {X, _} = statistics(wall_clock),
  X.



for_id(Count, Id) ->
  lists:map(fun(X) -> #query_params{consistency_level = one, bind_values = [p("s_id_", Id, X), p("c_id_", Id, X), p("v_id_", Id, X), p("o_id_", Id, X)]} end, lists:seq(1, Count)).

prepare_data(Count, Parts) ->
	lists:map(fun(Id) -> for_id(Count, Id) end, lists:seq(1, Parts)).