%%%-------------------------------------------------------------------
%%% @author Mikhail Turnovskiy
%%% @doc
%%% teledamus - Cassandra client for Erlang
%%% @end
%%%-------------------------------------------------------------------
-module(teledamus).

-include_lib("native_protocol.hrl").

-export([get_connection/0, get_connection/1, release_connection/1, release_connection/2, options/2, options/1, query/2, query/3, query/4, query/5, prepare_query/2, prepare_query/3, prepare_query/4,
         execute_query/2, execute_query/3, execute_query/4, batch_query/2, batch_query/3, batch_query/4, subscribe_events/2, subscribe_events/3, start/0, stop/0]).

-define(DEFAULT_TIMEOUT, 5000).

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
%%%
%%% Result - connection | {error, Reason}
%%% @end
-spec get_connection() -> connection() | {error, any()}.
get_connection() ->
	teledamus_srv:get_connection(?DEFAULT_TIMEOUT).


%%% @doc
%%% Get connection to Cassandra
%%%
%%% Timeout - connection timeout
%%% Result - connection | {error, Reason}
%%% @end
-spec get_connection(timeout()) -> connection() | {error, any()}.
get_connection(Timeout) ->
  teledamus_srv:get_connection(Timeout).

%%% @doc
%%% Release connection
%%%
%%% Сonnection is returned to the connection pool for future use or closed.
%%% Connection - connection to DB, as returned from get_connection()
%%% @end
-spec release_connection(connection()) -> 'ok'.
release_connection(Connection) ->
	teledamus_srv:release_connection(Connection, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Release connection
%%%
%%% Сonnection is returned to the connection pool for future use or closed.
%%% Timeout - operation timeout
%%% Connection - connection to DB, as returned from get_connection()
%%% @end
-spec release_connection(connection(), timeout()) -> 'ok'.
release_connection(Connection, Timeout) ->
  teledamus_srv:release_connection(Connection, Timeout).

%%% @doc
%%% Request DB for options
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% @end
-spec options(connection()) -> timeout | error() | options().
options(Connection) ->
  connection:options(Connection, ?DEFAULT_TIMEOUT).


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
%%% Execute query with default parameters & default timeout
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection(), string()) -> timeout | ok | error() | rows() | schema_change().
query(Connection, Query) ->
  connection:query(Connection, Query, #query_params{}, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Execute query with default parameters
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection(), string(), timeout()) -> timeout | ok | error() | rows() | schema_change().
query(Connection, Query, Timeout) ->
  connection:query(Connection, Query, #query_params{}, Timeout).


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
%%% Execute query
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% UseCache - use cached preparestatements
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection(), string(), query_params(), timeout(), boolean()) -> timeout | ok | error() | rows() | schema_change().
query(Connection, Query, Params, Timeout, UseCache) ->
  connection:query(Connection, Query, Params, Timeout, UseCache).


%%% @doc
%%% Create prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query(connection(), string()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Connection, Query) ->
  connection:prepare_query(Connection, Query, ?DEFAULT_TIMEOUT).

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

%%% @doc
%%% Create prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Query - string with CQL query
%%% Timeout - the number of milliseconds before operation times out.
%%% UseCache - use cached preparestatements
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query(connection(), string(), timeout(), boolean()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Connection, Query, Timeout, UseCache) ->
  connection:prepare_query(Connection, Query, Timeout, UseCache).


%%% @doc
%%% Execute prepared statement with default parameters & default timeout
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% ID - prepared query ID
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec execute_query(connection(), binary()) -> timeout | ok | error() | rows() | schema_change().
execute_query(Connection, ID) ->
  connection:execute_query(Connection, ID, #query_params{}, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Execute prepared statement with default parameters
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% ID - prepared query ID
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec execute_query(connection(), binary(), timeout()) -> timeout | ok | error() | rows() | schema_change().
execute_query(Connection, ID, Timeout) ->
  connection:execute_query(Connection, ID, #query_params{}, Timeout).

%%% @doc
%%% Execute prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% ID - prepared query ID
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec execute_query(connection(), binary(), query_params(), timeout()) -> timeout | ok | error() | rows() | schema_change().
execute_query(Connection, ID, Params, Timeout) ->
	connection:execute_query(Connection, ID, Params, Timeout).

%%% @doc
%%% Execute batch query
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query(connection(), binary()) -> timeout | ok.
batch_query(Connection, Batch) ->
  connection:batch_query(Connection, Batch, ?DEFAULT_TIMEOUT).


%%% @doc
%%% Execute batch query
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query(connection(), binary(), timeout()) -> timeout | ok.
batch_query(Connection, Batch, Timeout) ->
	connection:batch_query(Connection, Batch, Timeout).


%%% @doc
%%% Execute batch query
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Timeout - the number of milliseconds before operation times out.
%%% UseCache - use cached preparestatements
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query(connection(), binary(), timeout(), boolean()) -> timeout | ok.
batch_query(Connection, Batch, Timeout, UseCache) ->
  connection:batch_query(Connection, Batch, Timeout, UseCache).

%%% @doc
%%% Subscribe to Cassandra cluster events.
%%% Events will be processed via 'cassandra_events' event manager
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% EventType - types of events to subscribe (schema_change, topology_change or status_change)
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - ok | timeout | error
%%% @end
-spec subscribe_events(connection(), list(), timeout()) -> ok | timeout | error().
subscribe_events(Connection, EventTypes, Timeout) ->
	connection:subscribe_events(Connection, EventTypes, Timeout).

%%% @doc
%%% Subscribe to Cassandra cluster events.
%%% Events will be processed via 'cassandra_events' event manager
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% EventType - types of events to subscribe (schema_change, topology_change or status_change)
%%% Result - ok | timeout | error
%%% @end
-spec subscribe_events(connection(), list()) -> ok | timeout | error().
subscribe_events(Connection, EventTypes) ->
  connection:subscribe_events(Connection, EventTypes, ?DEFAULT_TIMEOUT).
