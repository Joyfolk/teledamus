-module(teledamus_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("native_protocol.hrl").
-include_lib("cql_types.hrl").

is_registered(_) ->
	?_assertNot(undefined =:= whereis(teledamus_sup)),
	?_assertNot(undefined =:= whereis(teledamus_srv)).


start() ->
	ok = teledamus:start(),
  Con = teledamus:get_connection(),
	#connection{} = Con,
  Con.

stop(Con) ->
	try
		ok = teledamus:release_connection(Con)
  after
		teledamus:stop()
	end.

start_stop_test_() ->
	{"The application can be started, stopped and has a registered name",
  {setup, fun start/0, fun stop/1, fun is_registered/1}}.

connection_test_() ->
	{"The application can connect to Cassandra server and return alive connection process"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		?_assertMatch({connection, _}, Connection),
    #connection{pid = Pid} = Connection,
		?_assert(is_process_alive(Pid))
  end}.

options_test_() ->
	{"The application can send OPTIONS request and parse answer"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		A = teledamus:options(Connection, 3000),
		[?_assert(is_list(A)),
		?_assert(proplists:is_defined("CQL_VERSION", A)),
		?_assert(proplists:is_defined("COMPRESSION", A))]
	end}.


simple_query_test_() ->
	{"Simple query test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		A = teledamus:query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #query_params{}, 3000),
		{Meta, Paging, Rows} = A,
		[?_assertMatch({_, _, _}, A),
		?_assertEqual(Paging, undefined),
		?_assert(is_list(Meta)),
		?_assert(is_list(Rows)),
		?_assert(length(Rows) > 0),
		?_assert(is_list(hd(Rows))),
		?_assertEqual(length(Meta), length(hd(Rows)))]
	end}.

bind_query_test_() ->
	{"Bind query test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		A = teledamus:query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = ?", #query_params{bind_values = [{ascii, "system"}]}, 3000),
		{Meta, Paging, Rows} = A,
		[?_assertMatch({_, _, _}, A),
		?_assertEqual(Paging, undefined),
		?_assert(is_list(Meta)),
		?_assert(is_list(Rows)),
		?_assert(length(Rows) > 0),
		?_assert(is_list(hd(Rows))),
		?_assertEqual(length(Meta), length(hd(Rows)))]
	end}.

prepared_query_test_() ->
	{"Prepared query test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		{Id, _Meta, _ResMeta} = teledamus:prepare_query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = ?", 3000),
		A = teledamus:execute_query(Connection, Id, #query_params{bind_values = [{varchar, "system"}]}, 3000),
		{Meta, Paging, Rows} = A,
		[?_assert(is_binary(Id)),
		?_assertMatch({_, _, _}, A),
		?_assertEqual(Paging, undefined),
		?_assert(is_list(Meta)),
		?_assert(is_list(Rows)),
		?_assert(length(Rows) > 0),
		?_assert(is_list(hd(Rows))),
		?_assertEqual(length(Meta), length(hd(Rows)))]
	end}.

create_drop_ks_test_() ->
  {"Create/drop keyspace test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    KS = teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    U = teledamus:query(Connection, "USE test", #query_params{}, 3000),
    D = teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual({created,"test", []}, KS),
     ?_assertEqual({keyspace, "test"}, U),
     ?_assertEqual({dropped,"test", []}, D)]
  end}.

create_drop_ts_test_() ->
  {"Create/drop table test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    C = teledamus:query(Connection, "CREATE TABLE test (test text PRIMARY KEY)", #query_params{}, 3000),
    D = teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual({created,"test", "test"}, C),
     ?_assertEqual({dropped,"test", "test"}, D)]
  end}.



batch_query_test_() ->
	{"Batch query test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (test text PRIMARY KEY)", #query_params{}, 3000),
    {Id, _, _} = teledamus:prepare_query(Connection, "INSERT INTO test(test) VALUES(?)", 3000),
		A = teledamus:batch_query(Connection, #batch_query{queries =
      [
       {Id, [{text, "1"}]}                                                       ,
       {"INSERT INTO test(test) VALUES(?)", [{text, "2"}]},
       {Id, [{text, "3"}]}
      ]}, 3000),
    B = teledamus:query(Connection, "SELECT test FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		{_, _, Rows} = B,
		[?_assertEqual(ok, A),
		 ?_assertEqual([["1"], ["2"], ["3"]], lists:sort(Rows))]
	end}.


text_datatypes_test_() ->
  {"Text datatypes test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
     teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
     teledamus:query(Connection, "USE test", #query_params{}, 3000),
     teledamus:query(Connection, "CREATE TABLE test (
                                  a ascii PRIMARY KEY,
                                  b text,
                                  c varchar,
                                 )", #query_params{}, 3000),
     teledamus:query(Connection, "INSERT INTO test(a,b,c) values(?,?,?)", #query_params{bind_values = [{ascii, "abc"}, {text, "где"}, {varchar, "ёжз"}]}, 3000),
     {_, _, Rows} = teledamus:query(Connection, "SELECT a, b, c FROM test", #query_params{}, 3000),
     teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
     teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
     [?_assertEqual([["abc", "где", "ёжз"]], Rows)]
end}.

bigint_datatype_test_() ->
	{"bigint datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		teledamus:query(Connection, "USE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a bigint PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{bigint, 10241024}]}, 3000),
		{_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		[?_assertEqual([[10241024]], Rows)]
	end}.


int_datatype_test_() ->
	{"int datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		teledamus:query(Connection, "USE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{int, 65535}]}, 3000),
		{_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		[?_assertEqual([[65535]], Rows)]
	end}.

boolean_datatype_test_() ->
	{"boolean datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		teledamus:query(Connection, "USE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a boolean PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{boolean, true}]}, 3000),
		{_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a boolean PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{boolean, false}]}, 3000),
		{_, _, Rows2} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		[?_assertEqual([[true]], Rows), ?_assertEqual([[false]], Rows2)]
	end}.

blob_datatype_test_() ->
	{"blob datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		teledamus:query(Connection, "USE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a blob PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{blob, <<1,2,3,4,5,6,7,8,9,0>>}]}, 3000),
		{_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		[?_assertEqual([[<<1,2,3,4,5,6,7,8,9,0>>]], Rows)]
	end}.

float_datatype_test_() ->
	{"float datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		teledamus:query(Connection, "USE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a float PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{float, 3.1}]}, 3000),
		{_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
	  [[V]] = Rows,

		[?_assertEqual(0, trunc(3.1 * 1000 - V * 1000))]
	end}.

double_datatype_test_() ->
	{"double datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		teledamus:query(Connection, "USE test", #query_params{}, 3000),
		teledamus:query(Connection, "CREATE TABLE test (
                                  a double PRIMARY KEY,
                                 )", #query_params{}, 3000),
		teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{double, 3.1}]}, 3000),
		{_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		[[V]] = Rows,

		[?_assertEqual(0, trunc(3.1 * 1000 - V * 1000))]
	end}.

timestamp_datatype_test_() ->
  {"timestamp datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a timestamp PRIMARY KEY,
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{timestamp, 165535}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[165535]], Rows)]
  end}.

uuid_datatype_test_() ->
  {"uuid datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a uuid PRIMARY KEY,
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{uuid, <<1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6>>}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[<<1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6>>]], Rows)]
  end}.



timeuuid_datatype_test_() ->
  {"uuid datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    TUUUID = <<1,2,3,4, 5,6, 1:4,7:4,8, 1:4,9:4,10, 1,2,3,4,5,6>>, %% special form to be compatible with timeuuid version
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a timeuuid PRIMARY KEY,
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{timeuuid, TUUUID}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[TUUUID]], Rows)]
  end}.

counter_datatype_test_() ->
  {"counter datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b counter
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "UPDATE test SET b = b + ? WHERE a = 1", #query_params{bind_values = [{counter, 100}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[100]], Rows)]
  end}.


inet_datatype_test_() ->
  {"inet_v4 datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a inet PRIMARY KEY
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{inet, #inet{ip = {127, 0, 0, 1}}}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[#inet{ip = {127, 0, 0, 1}}]], Rows)]
  end}.

inet6_datatype_test_() ->
  {"inet_v6 datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a inet PRIMARY KEY
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{inet, #inet{ip = {1, 2, 3, 4, 5, 6, 7, 8}}}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[#inet{ip = {1, 2, 3, 4, 5, 6, 7, 8}}]], Rows)]
  end}.

varint_datatype_test_() ->
  {"varint datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a varint PRIMARY KEY
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{varint, 123456789012345678901234567890123456789012345678901234567890}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[123456789012345678901234567890123456789012345678901234567890]], Rows)]
  end}.

varint_neg_datatype_test_() ->
  {"varint (negative value) datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a varint PRIMARY KEY
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{varint, -123456789012345678901234567890123456789012345678901234567890}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[-123456789012345678901234567890123456789012345678901234567890]], Rows)]
  end}.

decimal_datatype_test_() ->
  {"decimal datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a decimal PRIMARY KEY
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{decimal, #decimal{scale = -2, value = 123456789012345678901234567890123456789012345678901234567890}}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[#decimal{scale = -2, value = 123456789012345678901234567890123456789012345678901234567890}]], Rows)]
  end}.

decimal_neg_datatype_test_() ->
  {"decimal negative value datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a decimal PRIMARY KEY
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a) values(?)", #query_params{bind_values = [{decimal, #decimal{scale = 2, value = -123456789012345678901234567890123456789012345678901234567890}}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[#decimal{scale = 2, value = -123456789012345678901234567890123456789012345678901234567890}]], Rows)]
  end}.

list_datatype_test_() ->
  {"list datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b list<int>
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #query_params{bind_values = [{{list, int}, [1, 2, 3]}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[[1, 2, 3]]], Rows)]
  end}.

set_datatype_test_() ->
  {"list datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b set<int>
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #query_params{bind_values = [{{set, int}, [1, 2, 3]}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[[1, 2, 3]]], Rows)]
  end}.


map_datatype_test_() ->
  {"list datatype test"},
  {setup, fun start/0, fun stop/1, fun(Connection) ->
    teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
    teledamus:query(Connection, "USE test", #query_params{}, 3000),
    teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b map<ascii, int>
                                 )", #query_params{}, 3000),
    teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #query_params{bind_values = [{{map, ascii, int}, [{"a", 1}, {"b", 2}, {"c", 3}]}]}, 3000),
    {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP TABLE test", #query_params{}, 3000),
    teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
    [?_assertEqual([[[{"a", 1}, {"b", 2}, {"c", 3}]]], Rows)]
  end}.


cassandra_events_test_() ->
	{"list datatype test"},
	{setup, fun start/0, fun stop/1, fun(Connection) ->
		{ok, _Pid} = test_event_handler:start_link(),
		ok = teledamus:subscribe_events(Connection, [schema_change, status_change, topology_change], 3000),
		gen_event:add_handler(cassandra_events, test_event_handler, []),
		timer:sleep(1000),
		teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #query_params{}, 3000),
		timer:sleep(3000),
		teledamus:query(Connection, "DROP KEYSPACE test", #query_params{}, 3000),
		timer:sleep(1000),
		Res = gen_event:call(cassandra_events, test_event_handler, get_events, 3000),
		gen_event:delete_handler(cassandra_events, test_event_handler, []),
		[?_assertEqual([{schema_change, dropped, "test", ""}, {schema_change, created, "test", ""}], Res)]
	end}.


