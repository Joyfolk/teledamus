%%%-------------------------------------------------------------------
%%% @author Mikhail Turnovskiy
%%% @doc
%%% teledamus - Cassandra client for Erlang
%%% @end
%%%-------------------------------------------------------------------
-module(teledamus).

-include_lib("native_protocol.hrl").

-export([get_connection/0, release_connection/1, options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, start/0,stop/0]).


%%% @doc
%%% Start application
%%% @end
-spec start() -> ok | {error, any()}.
start() ->
	application:start(teledamus).

%%% @doc
%%% Stop application
%%% @end
-spec stop() -> ok | {error, any()}.
stop() ->
	application:stop(teledamus).

%%% @doc
%%% Get connection to Cassandra
%%% @end
-spec get_connection() -> connection() | {error, any()}.
get_connection() ->
	teledamus_srv:get_connection().

%%% @doc
%%% Release connection.
%%% Сonnection is returned to the connection pool for future use or closed.
%%% Connection - connection to DB, as returned from get_connection()
%%% @end
-spec release_connection(connection()) -> 'ok'.
release_connection(Connection) ->
	teledamus_srv:release_connection(Connection).

%%% @doc
%%% Request DB for options
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Timeout - the number of milliseconds before operation times out.
%%% @end
-spec options(connection(), timeout()) -> timeout | error() | options().
options(Connection, Timeout) ->
	connection:options(Connection, Timeout).

%%% @doc
%%% Execute query
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection(), string(), query_params(), timeout()) -> timeout | ok | error() | rows() | schema_change().
query(Connection, Query, Params, Timeout) ->
	connection:query(Connection, Query, Params, Timeout).

%%% @doc
%%% Create prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query(connection(), string(), timeout()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Connection, Query, Timeout) ->
	connection:prepare_query(Connection, Query, Timeout).

%%% Execute prepad statement
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% ID - prepared query ID
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
-spec execute_query(connection(), binary(), query_params(), timeout()) -> timeout | ok | error() | rows() | schema_change().
execute_query(Connection, ID, Params, Timeout) ->
	connection:execute_query(Connection, ID, Params, Timeout).

%%% Execute batch query
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
-spec batch_query(connection(), binary(),  timeout()) -> timeout | ok.
batch_query(Connection, Batch, Timeout) ->
	connection:batch_query(Connection, Batch, Timeout).

%%% Subscribe to Cassandra cluster events.
%%% Events will be processed via 'cassandra_events' event manager
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% EventType - types of events to subscribe (schema_change, topology_change or status_change)
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - ok | timeout | error
-spec subscribe_events(connection(), list(), timeout()) -> ok | timeout | error().
subscribe_events(Connection, EventTypes, Timeout) ->
	connection:subscribe_events(Connection, EventTypes, Timeout).
