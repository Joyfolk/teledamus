-module(examples).
-author("Mikhail.Turnovskiy").

-export([start/0, stop/0, create_schema/0, drop_schema/0, load_test/1, load_test_multi/2]).

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
  T0 = millis(),
  {PrepStmt, _, _} = teledamus:prepare_query(Con, "INSERT INTO profiles(s_id, c_id, v_id, o_id) VALUES(?, ?, ?, ?)"),
  try
    load_test_int(Con, PrepStmt, Id, Count)
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


ceil(X) ->
  T = trunc(X),
  if T =:= X -> X; true -> trunc(X + 1) end.

load_test_multi(N, Count) ->
  T = millis(),
  spawn_load_tests(0, ceil(Count / N), Count),
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

spawn_load_tests(N, Batch, Count) ->
  Parent = self(),
  spawn(fun() ->
%%     error_logger:info_msg("Spawning ~p~n", [N]),
    R = load_test(N, if Batch > Count -> Count; true -> Batch end),
%%     error_logger:info_msg("Completed ~p:~p~n", [N, R]),
    Parent ! R
  end),
  if
    Batch >= Count ->
      started;
    true ->
      spawn_load_tests(N + 1, Batch, Count - Batch)
  end.

p(Str, Id, Count) -> {varchar, Str ++ integer_to_list(Id) ++ "_" ++ integer_to_list(Count)}.

load_test_int(_Con, _PrepStmt, _Id, 0) -> ok;
load_test_int(Con, PrepStmt, Id, Count) ->
  teledamus:execute_query(Con, PrepStmt, #query_params{consistency_level = one, bind_values = [p("s_id_", Id, Count), p("c_id_", Id, Count), p("v_id_", Id, Count), p("o_id_", Id, Count)]}, 5000),
  load_test_int(Con, PrepStmt, Id, Count - 1).

millis() ->
  {X, _} = statistics(wall_clock),
  X.
