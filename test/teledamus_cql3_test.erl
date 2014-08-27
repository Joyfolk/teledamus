-module(teledamus_cql3_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("teledamus.hrl").

-export([async_mfa/1, start/0, stop/1, start_mon/0, stop_mon/1, on_call/2, on_reply/2]).

is_registered(_) ->
    ?_assertNot(undefined =:= whereis(teledamus_sup)),
    ?_assertNot(undefined =:= whereis(teledamus_srv)).


start() ->
    application:set_env(teledamus, required_protocol_version, cql3),
    ok = teledamus:start(),
    Con = teledamus:get_connection(),
    #tdm_connection{} = Con,
    Con.

stop(Con) ->
    try
        ok = teledamus:release_connection(Con)
    after
        teledamus:stop(),
        application:unset_env(teledamus, required_protocol_version)
    end.

start_stop_test_() ->
    {"The application can be started, stopped and has a registered name",
        {setup, fun start/0, fun stop/1, fun is_registered/1}}.

connection_test_() ->
    {"The application can connect to Cassandra server and return alive connection process"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        ?_assertMatch({connection, _}, Connection),
        #tdm_connection{pid = Pid} = Connection,
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
        A = teledamus:query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, 3000),
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
        A = teledamus:query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = ?", #tdm_query_params{bind_values = [{ascii, "system"}]}, 3000),
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
        A = teledamus:execute_query(Connection, Id, #tdm_query_params{bind_values = [{varchar, "system"}]}, 3000),
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
        KS = teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        timer:sleep(100),
        U = teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        timer:sleep(100),
        D = teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        timer:sleep(100),
        [?_assertEqual({schema_change, created, keyspace, "test"}, KS),
            ?_assertEqual({keyspace, "test"}, U),
            ?_assertEqual({schema_change, dropped, keyspace, "test"}, D)]
    end}.

create_drop_ts_test_() ->
    {"Create/drop table test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        C = teledamus:query(Connection, "CREATE TABLE test (test text PRIMARY KEY)", #tdm_query_params{}, 3000),
        D = teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual({schema_change, created, table, "test", "test"}, C),
            ?_assertEqual({schema_change, dropped, table,"test", "test"}, D)]
    end}.



batch_query_test_() ->
    {"Batch query test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (test text PRIMARY KEY)", #tdm_query_params{}, 3000),
        {Id, _, _} = teledamus:prepare_query(Connection, "INSERT INTO test(test) VALUES(?)", 3000),
        A = teledamus:batch_query(Connection, #tdm_batch_query{queries =
        [
            {Id, [{text, "1"}]}                                                       ,
            {"INSERT INTO test(test) VALUES(?)", [{text, "2"}]},
            {Id, [{text, "3"}]}
        ]}, 3000),
        B = teledamus:query(Connection, "SELECT test FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        {_, _, Rows} = B,
        [?_assertEqual(ok, A),
            ?_assertEqual([["1"], ["2"], ["3"]], lists:sort(Rows))]
    end}.


text_datatypes_test_() ->
    {"Text datatypes test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a ascii PRIMARY KEY,
                                  b text,
                                  c varchar,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a,b,c) values(?,?,?)", #tdm_query_params{bind_values = [{ascii, "abc"}, {text, "где"}, {varchar, "ёжз"}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a, b, c FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([["abc", "где", "ёжз"]], Rows)]
    end}.

text_datatypes_null_test_() ->
    {"Text datatypes test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a ascii PRIMARY KEY,
                                  b text,
                                  c varchar,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a,b,c) values(?,?,?)", #tdm_query_params{bind_values = [{ascii, "abc"}, {text, undefined}, {varchar, undefined}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a, b, c FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([["abc", undefined, undefined]], Rows)]
    end}.

bigint_datatype_test_() ->
    {"bigint datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a bigint PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{bigint, 10241024}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[10241024]], Rows)]
    end}.


int_datatype_test_() ->
    {"int datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{int, 65535}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[65535]], Rows)]
    end}.

boolean_datatype_test_() ->
    {"boolean datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a boolean PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{boolean, true}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a boolean PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{boolean, false}]}, 3000),
        {_, _, Rows2} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[true]], Rows), ?_assertEqual([[false]], Rows2)]
    end}.

blob_datatype_test_() ->
    {"blob datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a blob PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{blob, <<1,2,3,4,5,6,7,8,9,0>>}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[<<1,2,3,4,5,6,7,8,9,0>>]], Rows)]
    end}.

float_datatype_test_() ->
    {"float datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a float PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{float, 3.1}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [[V]] = Rows,

        [?_assertEqual(0, trunc(3.1 * 1000 - V * 1000))]
    end}.

double_datatype_test_() ->
    {"double datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a double PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{double, 3.1}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [[V]] = Rows,

        [?_assertEqual(0, trunc(3.1 * 1000 - V * 1000))]
    end}.

timestamp_datatype_test_() ->
    {"timestamp datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a timestamp PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{timestamp, 165535}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[165535]], Rows)]
    end}.

uuid_datatype_test_() ->
    {"uuid datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a uuid PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{uuid, <<1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6>>}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[<<1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6>>]], Rows)]
    end}.



timeuuid_datatype_test_() ->
    {"uuid datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        TUUUID = <<1,2,3,4, 5,6, 1:4,7:4,8, 1:4,9:4,10, 1,2,3,4,5,6>>, %% special form to be compatible with timeuuid version
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a timeuuid PRIMARY KEY,
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{timeuuid, TUUUID}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[TUUUID]], Rows)]
    end}.

counter_datatype_test_() ->
    {"counter datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b counter
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "UPDATE test SET b = b + ? WHERE a = 1", #tdm_query_params{bind_values = [{counter, 100}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[100]], Rows)]
    end}.


inet_datatype_test_() ->
    {"inet_v4 datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a inet PRIMARY KEY
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{inet, #tdm_inet{ip = {127, 0, 0, 1}}}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[#tdm_inet{ip = {127, 0, 0, 1}}]], Rows)]
    end}.

inet6_datatype_test_() ->
    {"inet_v6 datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a inet PRIMARY KEY
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{inet, #tdm_inet{ip = {1, 2, 3, 4, 5, 6, 7, 8}}}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[#tdm_inet{ip = {1, 2, 3, 4, 5, 6, 7, 8}}]], Rows)]
    end}.

varint_datatype_null_test_() ->
    {"varint datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  b int PRIMARY KEY,
                                  a varint
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(b, a) values(1, ?)", #tdm_query_params{bind_values = [{varint, undefined}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[undefined]], Rows)]
    end}.

varint_neg_datatype_test_() ->
    {"varint (negative value) datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a varint PRIMARY KEY
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{varint, -123456789012345678901234567890123456789012345678901234567890}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[-123456789012345678901234567890123456789012345678901234567890]], Rows)]
    end}.

decimal_datatype_test_() ->
    {"decimal datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a decimal PRIMARY KEY
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{decimal, #tdm_decimal{scale = -2, value = 123456789012345678901234567890123456789012345678901234567890}}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[#tdm_decimal{scale = -2, value = 123456789012345678901234567890123456789012345678901234567890}]], Rows)]
    end}.

decimal_neg_datatype_test_() ->
    {"decimal negative value datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a decimal PRIMARY KEY
                                 )", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "INSERT INTO test(a) values(?)", #tdm_query_params{bind_values = [{decimal, #tdm_decimal{scale = 2, value = -123456789012345678901234567890123456789012345678901234567890}}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT a FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual([[#tdm_decimal{scale = 2, value = -123456789012345678901234567890123456789012345678901234567890}]], Rows)]
    end}.

list_datatype_read_test_() ->
    {"list datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b list<int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, [1, 2,3])", #tdm_query_params{}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[1, 2, 3]]], Rows)]
    end}.


list_datatype_null_read_test_() ->
    {"list datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b list<int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, [])", #tdm_query_params{}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[undefined]], Rows)]
    end}.


list_datatype_test_() ->
    {"list datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b list<int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{list, int}, [1, 2, 3]}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[1, 2, 3]]], Rows)]
    end}.

list_datatype_null_test_() ->
    {"list datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b list<int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{list, int}, []}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[undefined]], Rows)]
    end}.

set_datatype_test_() ->
    {"set datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b set<int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{set, int}, [1, 2, 3]}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[1, 2, 3]]], Rows)]
    end}.



set_datatype_null_test_() ->
    {"set datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b set<int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{set, int}, undefined}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[undefined]], Rows)]
    end}.

map_datatype_test_() ->
    {"map datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b map<ascii, int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{map, ascii, int}, [{"a", 1}, {"b", 2}, {"c", 3}]}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[{"a", 1}, {"b", 2}, {"c", 3}]]], Rows)]
    end}.

map_datatype_null_test_() ->
    {"map datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b map<ascii, int>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{map, ascii, int}, []}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[undefined]], Rows)]
    end}.


tuple_datatype_test_() ->
    {"tuple datatype test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b tuple<boolean, int, ascii>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{bind_values = [{{tuple, [boolean, int, ascii]}, [true, 2, "test"]}]}, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[true, 2, "test"]]], Rows)]
    end}.

udt_datatype_test_() ->
    {"UDT test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, type, "test", "test_type"} = teledamus:query(Connection, "CREATE TYPE test_type(fld1 boolean, fld2 int, fld3 ascii)"),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b test_type
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{
            bind_values = [
                {
                    #tdm_udt{keyspace = "test", name = "test_type", fields = [{"fld1", boolean}, {"fld2", int}, {"fld3", ascii}]},
                    [{"fld1", true}, {"fld2", 4}, {"fld3", "test"}]
                }
            ]
        }, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TYPE test_type"),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[{"fld1", true}, {"fld2", 4}, {"fld3", "test"}]]], Rows)]
    end}.


udt_datatype_complex_test_() ->
    {"UDT test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        {schema_change, created, type, "test", "test_type"} = teledamus:query(Connection, "CREATE TYPE test_type(fld1 int, fld2 map<int, boolean>)"),
        {schema_change, created, table, "test", "test"} = teledamus:query(Connection, "CREATE TABLE test (
                                  a int PRIMARY KEY,
                                  b map<int, test_type>
                                 )", #tdm_query_params{}, 3000),
        OK = teledamus:query(Connection, "INSERT INTO test(a, b) values(1, ?)", #tdm_query_params{
            bind_values = [
                {
                    {map, int, #tdm_udt{keyspace = "test", name = "test_type", fields = [{"fld1", int}, {"fld2", {map, int, boolean}}]}},
                    [
                        {1, [{"fld1", 4}, {"fld2", [{1, true}, {2, false}]}]},
                        {2, [{"fld1", 3}, {"fld2", [{2, true}, {3, false}]}]}
                    ]
                }
            ]
        }, 3000),
        {_, _, Rows} = teledamus:query(Connection, "SELECT b FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TYPE test_type"),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        [?_assertEqual(ok, OK), ?_assertEqual([[[
            {1, [{"fld1", 4}, {"fld2", [{1, true}, {2, false}]}]},
            {2, [{"fld1", 3}, {"fld2", [{2, true}, {3, false}]}]}
        ]]], Rows)]
    end}.


cassandra_events_test_() ->
    {"events handler test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        {ok, _Pid} = test_event_handler:start_link(),
        ok = teledamus:subscribe_events(Connection, [schema_change, status_change, topology_change], 3000),
        gen_event:add_handler(cassandra_events, test_event_handler, []),
        timer:sleep(1000),
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        timer:sleep(3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        timer:sleep(1000),
        Res = gen_event:call(cassandra_events, test_event_handler, get_events, 3000),
        gen_event:delete_handler(cassandra_events, test_event_handler, []),
        [?_assertEqual([{schema_change, dropped, keyspace, "test"}, {schema_change, created, keyspace, "test"}], Res)]
    end}.


cached_query_connection_test_() ->
    {"Cached query connection test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        A = teledamus:query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, 3000, true),
        {Meta, Paging, Rows} = A,
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(Paging, undefined),
            ?_assert(is_list(Meta)),
            ?_assert(is_list(Rows)),
            ?_assert(length(Rows) > 0),
            ?_assert(is_list(hd(Rows))),
            ?_assertEqual(length(Meta), length(hd(Rows)))]
    end}.

cached_query_stream_test_() ->
    {"Cached query stream test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        Stream = tdm_connection:new_stream(Connection, 1000),
        A = teledamus:query(Stream, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, 3000, true),
        {Meta, Paging, Rows} = A,
        tdm_connection:release_stream(Stream, 1000),
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(Paging, undefined),
            ?_assert(is_list(Meta)),
            ?_assert(is_list(Rows)),
            ?_assert(length(Rows) > 0),
            ?_assert(is_list(hd(Rows))),
            ?_assertEqual(length(Meta), length(hd(Rows)))]
    end}.

stream_new_stream_connection_test_() ->
    {"Cached query new stream / connection test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        Stream = tdm_connection:new_stream(Connection, 1000),
        A = teledamus:query(Stream#tdm_stream.connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, 3000, true),
        {Meta, Paging, Rows} = A,
        tdm_connection:release_stream(Stream, 1000),
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(Paging, undefined),
            ?_assert(is_list(Meta)),
            ?_assert(is_list(Rows)),
            ?_assert(length(Rows) > 0),
            ?_assert(is_list(hd(Rows))),
            ?_assertEqual(length(Meta), length(hd(Rows)))]
    end}.

simple_async_api_pid_test_() ->
    {"Simple async api test / pid"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        ok = teledamus:query_async(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, self()),
        A = {Meta, Paging, Rows} = receive
            X =  {_, _, _} -> X
        after
            1000 -> timeout
        end,
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(Paging, undefined),
            ?_assert(is_list(Meta)),
            ?_assert(is_list(Rows)),
            ?_assert(length(Rows) > 0),
            ?_assert(is_list(hd(Rows))),
            ?_assertEqual(length(Meta), length(hd(Rows)))]
    end}.

simple_async_api_fun_test_() ->
    {"Simple async api test / pid"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        Pid = self(),
        F = fun(X) -> Pid ! {reply, X} end,
        ok = teledamus:query_async(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, F),
        A = {Meta, Paging, Rows} = receive
            {reply, X} -> X
        after
            1000 -> timeout
        end,
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(Paging, undefined),
            ?_assert(is_list(Meta)),
            ?_assert(is_list(Rows)),
            ?_assert(length(Rows) > 0),
            ?_assert(is_list(hd(Rows))),
            ?_assertEqual(length(Meta), length(hd(Rows)))]
    end}.

simple_async_api_mfa_test_() ->
    {"Simple async api test / pid"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        ok = teledamus:query_async(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, {teledamus_cql3_test, async_mfa, []}),
        timer:sleep(1000),
        {ok, A} = application:get_env(test, async_mfa),
        {Meta, Paging, Rows} = A,
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(Paging, undefined),
            ?_assert(is_list(Meta)),
            ?_assert(is_list(Rows)),
            ?_assert(length(Rows) > 0),
            ?_assert(is_list(hd(Rows))),
            ?_assertEqual(length(Meta), length(hd(Rows)))]
    end}.

async_mfa(X) ->
    application:set_env(test, async_mfa, X).




options_test_async_test_() ->
    {"async OPTIONS"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        ok = teledamus:options_async(Connection, self()),
        A = receive
            X = [_H | _T] -> X
        after
            1000 -> timeout
        end,
        [?_assert(is_list(A)),
            ?_assert(proplists:is_defined("CQL_VERSION", A)),
            ?_assert(proplists:is_defined("COMPRESSION", A))]
    end}.


prepared_query_async_test_() ->
    {"Prepared query test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        ok = teledamus:prepare_query_async(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = ?", self()),
        Id = receive
            {X, _Meta, _ResMeta} -> X
        after
            1000 -> timeout
        end,
        ok = teledamus:execute_query_async(Connection, Id, #tdm_query_params{bind_values = [{varchar, "system"}]}, self()),
        A = receive
            XXX = {_, _, _} -> XXX
        after
            1000 -> timeout
        end,
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

batch_query_async_test_() ->
    {"Batch query test"},
    {setup, fun start/0, fun stop/1, fun(Connection) ->
        teledamus:query(Connection, "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "USE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "CREATE TABLE test (test text PRIMARY KEY)", #tdm_query_params{}, 3000),
        Pid = self(),
        {Id, _, _} = teledamus:prepare_query(Connection, "INSERT INTO test(test) VALUES(?)", 3000),
        ok = teledamus:batch_query_async(Connection, #tdm_batch_query{queries =
        [
            {Id, [{text, "1"}]}                                                       ,
            {"INSERT INTO test(test) VALUES(?)", [{text, "2"}]},
            {Id, [{text, "3"}]}
        ]}, fun(X) -> Pid ! {reply, X} end),
        A = receive
            {reply, X} -> X
        after
            1000 -> timeout
        end,
        B = teledamus:query(Connection, "SELECT test FROM test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP TABLE test", #tdm_query_params{}, 3000),
        teledamus:query(Connection, "DROP KEYSPACE test", #tdm_query_params{}, 3000),
        {_, _, Rows} = B,
        [?_assertEqual(ok, A),
            ?_assertEqual([["1"], ["2"], ["3"]], lists:sort(Rows))]
    end}.



simple_query_mon_test_() ->
    {"monitoring api test"},
    {setup, fun start_mon/0, fun stop_mon/1, fun(Connection) ->
        A = teledamus:query(Connection, "SELECT * from system.schema_keyspaces WHERE keyspace_name = 'system'", #tdm_query_params{}, 3000),
        [?_assertMatch({_, _, _}, A),
            ?_assertEqual(2, length(ets:tab2list(teledamus))),
            ?_assertMatch({call, _, _}, hd(tl(ets:tab2list(teledamus)))),
            ?_assertMatch({reply, _, _}, hd(ets:tab2list(teledamus)))
        ]
    end}.




start_mon() ->
    application:set_env(teledamus, channel_monitor, teledamus_cql3_test),
    teledamus = ets:new(teledamus, [public, named_table]),
    ok = teledamus:start(),
    Con = teledamus:get_connection(),
    #tdm_connection{} = Con,
    Con.

stop_mon(Con) ->
    try
        ok = teledamus:release_connection(Con)
    after
        teledamus:stop(),
        ets:delete(teledamus),
        application:unset_env(teledamus, cahnnel_monitor)
    end.

%% channel monitor callbacks
on_call(From, Msg) ->
    ets:insert(teledamus, {call, From, Msg}).

on_reply(Caller, Reply) ->
    ets:insert(teledamus, {reply, Caller, Reply}).