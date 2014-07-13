%%%-------------------------------------------------------------------
%%% @author Mikhail Turnovskiy
%%% @doc
%%% teledamus - Cassandra client for Erlang
%%% @end
%%%-------------------------------------------------------------------
-module(teledamus).

-include_lib("tdm_native_protocol.hrl").

-export([get_connection/0, get_connection/1, release_connection/1, release_connection/2, options/2, options/1, query/2, query/3, query/4, query/5, prepare_query/2, prepare_query/3, prepare_query/4,
         execute_query/2, execute_query/3, execute_query/4, batch_query/2, batch_query/3, batch_query/4, subscribe_events/2, subscribe_events/3, start/0, stop/0,
         new_stream/1, new_stream/2, release_stream/1, release_stream/2, release_stream_async/1]).
-export([options_async/2, query_async/3, query_async/4, query_async/5, prepare_query_async/3, prepare_query_async/4, execute_query_async/3, execute_query_async/4,
         batch_query_async/4, batch_query_async/3, subscribe_events_async/3]).

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
release_connection(Connection = #connection{}) ->
  teledamus_srv:release_connection(Connection, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Release connection
%%%
%%% Сonnection is returned to the connection pool for future use or closed.
%%% Timeout - operation timeout
%%% Connection - connection to DB, as returned from get_connection()
%%% @end
-spec release_connection(connection(), timeout()) -> 'ok'.
release_connection(Connection = #connection{}, Timeout) ->
  teledamus_srv:release_connection(Connection, Timeout).


%%% @doc
%%% Create new stream for given connection
%%%
%%% Connection - connection to cassandra
%%% Result - stream | {error, Reason}
%%% @end
-spec new_stream(connection()) -> stream() | {error, any()}.
new_stream(Connection = #connection{}) ->
  tdm_connection:new_stream(Connection, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Create new stream for given connection
%%%
%%% Connection - connection to cassandra
%%% Timeout - operation timeout
%%% Result - stream | {error, Reason}
%%% @end
-spec new_stream(connection(), timeout()) -> stream() | {error, any()}.
new_stream(Connection = #connection{}, Timeout) ->
  tdm_connection:new_stream(Connection, Timeout).


%%% @doc
%%% Release stream
%%%
%%% Stream - ...
%%% Result - stream | {error, Reason}
%%% @end
-spec release_stream(stream()) -> ok | {error, any()}.
release_stream(Stream) ->
  tdm_connection:release_stream(Stream, ?DEFAULT_TIMEOUT).


%%% @doc
%%% Release stream
%%%
%%% Stream - ...
%%% Result - stream | {error, Reason}
%%% @end
-spec release_stream_async(stream()) -> ok | {error, any()}.
release_stream_async(Stream) ->
    tdm_connection:release_stream_async(Stream).


%%% @doc
%%% Release stream
%%%
%%% Stream - ...
%%% Timeout - operation timeout
%%% Result - stream | {error, Reason}
%%% @end
-spec release_stream(stream(), timeout()) -> ok | {error, any()}.
release_stream(Stream, Timeout) ->
  tdm_connection:release_stream(Stream, Timeout).

%%% @doc
%%% Request DB for options
%%%
%%% Connection or Stream - connection to DB, as returned from get_connection() or stream
%%% @end
-spec options(connection() | stream()) -> timeout | error() | options().
options(Stream = #stream{}) ->
  tdm_connection:options(Stream, ?DEFAULT_TIMEOUT);
options(Connection) ->
  tdm_connection:options(Connection, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Request DB for options (asynchronous version)
%%%
%%% Connection :: connection() | stream() - connection to DB, as returned from get_connection() or stream
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% @end
-spec options_async(Connection :: connection() | stream(), async_target()) -> ok | {error, Reason :: term()}.
options_async(Stream = #stream{}, ReplyTo) ->
	tdm_connection:options_async(Stream, ReplyTo);
options_async(Connection, ReplyTo) ->
	tdm_connection:options_async(Connection, ReplyTo).

%%% @doc
%%% Request DB for options
%%%
%%% Connection or Stream - connection to DB, as returned from get_connection() or stream
%%% Timeout - the number of milliseconds before operation times out.
%%% @end
-spec options(connection() | stream(), timeout()) -> timeout | error() | options().
options(Stream = #stream{}, Timeout) ->
  tdm_stream:options(Stream, Timeout);
options(Connection, Timeout) ->
  tdm_connection:options(Connection, Timeout).


%%% @doc
%%% Execute query with default parameters & default timeout
%%%
%%% Connection or Stream - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection() | stream(), string()) -> timeout | ok | error() | result_rows() | schema_change().
query(Stream = #stream{}, Query) ->
	tdm_stream:query(Stream, Query, #query_params{}, ?DEFAULT_TIMEOUT);
query(Connection, Query) ->
	tdm_connection:query(Connection, Query, #query_params{}, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Execute query with default parameters (asynchronous version)
%%%
%%% Connection :: connection() or stream() - connection to DB, as returned from get_connection() or stream
%%% Query - string() with CQL query
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%%
%%% Result - result of query or error or timeout.
%%% @end
-spec query_async(Connection :: connection() | stream(), Query :: string(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
query_async(Stream = #stream{}, Query, ReplyTo) ->
  tdm_stream:query_async(Stream, Query, #query_params{}, ReplyTo);
query_async(Connection, Query, ReplyTo) ->
  tdm_connection:query_async(Connection, Query, #query_params{}, ReplyTo).

%%% @doc
%%% Execute query with default parameters & default timeout
%%%
%%% Connection or Stream - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Result - result of query or error or timeout.

%%% @doc
%%% Execute query with default parameters
%%%
%%% Connection or Stream - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection() | stream(), string(), timeout()) -> timeout | ok | error() | result_rows() | schema_change().
query(Stream = #stream{}, Query, Timeout) ->
  tdm_stream:query(Stream, Query, #query_params{}, Timeout);
query(Connection, Query, Timeout) ->
  tdm_connection:query(Connection, Query, #query_params{}, Timeout).


%%% @doc
%%% Execute query
%%%
%%% Connection or stream - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection()| stream(), string(), query_params(), timeout()) -> timeout | ok | error() | result_rows() | schema_change().
query(Stream = #stream{}, Query, Params, Timeout) ->
  tdm_stream:query(Stream, Query, Params, Timeout);
query(Connection, Query, Params, Timeout) ->
  tdm_connection:query(Connection, Query, Params, Timeout).

%%% @doc
%%% Execute query (asynchronous version)
%%%
%%% Connection :: connection() or stream() - connection to DB, as returned from get_connection() or stream
%%% Query - string() with CQL query
%%% Params - query parameters
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%%
%%% Result - result of query or error or timeout.
%%% @end
-spec query_async(Connection :: connection() | stream(), Query :: string(), Params :: query_params(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
query_async(Stream = #stream{}, Query, Params, ReplyTo) ->
	tdm_stream:query_async(Stream, Query, Params, ReplyTo);
query_async(Connection, Query, Params, ReplyTo) ->
	tdm_connection:query_async(Connection, Query, Params, ReplyTo).

%%% @doc
%%% Execute query
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% UseCache - use cached preparestatements
%%% Result - result of query or error or timeout.
%%% @end
-spec query(connection() | stream(), string(), query_params(), timeout(), boolean()) -> timeout | ok | error() | result_rows() | schema_change().
query(Stream = #stream{}, Query, Params, Timeout, UseCache) ->
  tdm_stream:query(Stream, Query, Params, Timeout, UseCache);
query(Connection, Query, Params, Timeout, UseCache) ->
  tdm_connection:query(Connection, Query, Params, Timeout, UseCache).


%%% @doc
%%% Execute query (asynchronous version)
%%%
%%% Connection :: connection() or stream() - connection to DB, as returned from get_connection() or stream
%%% Query - string() with CQL query
%%% Params - query parameters
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% UseCache - use cached preparestatements
%%%
%%% Result - result of query or error or timeout.
%%% @end
-spec query_async(Connection :: connection() | stream(), Query :: string(), Params :: query_params(), ReplyTo :: async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
query_async(Stream = #stream{}, Query, Params, ReplyTo, UseCache) ->
	tdm_stream:query_async(Stream, Query, Params, ReplyTo, UseCache);
query_async(Connection, Query, ReplyTo, Params, UseCache) ->
	tdm_connection:query_async(Connection, Query, Params, ReplyTo, UseCache).

%%% @doc
%%% Create prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query(connection() | stream(), string()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Stream = #stream{}, Query) ->
  tdm_stream:prepare_query(Stream, Query, ?DEFAULT_TIMEOUT);
prepare_query(Connection, Query) ->
  tdm_connection:prepare_query(Connection, Query, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Create prepared statement  (asynchronous version)
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query_async(Connection :: connection() | stream(), Query :: string(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
prepare_query_async(Stream = #stream{}, Query, ReplyTo) ->
	tdm_stream:prepare_query_async(Stream, Query, ReplyTo);
prepare_query_async(Connection, Query, ReplyTo) ->
	tdm_connection:prepare_query_async(Connection, Query, ReplyTo).

%%% @doc
%%% Create prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query(connection() | stream, string(), timeout()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Stream = #stream{}, Query, Timeout) ->
  tdm_stream:prepare_query(Stream, Query, Timeout);
prepare_query(Connection, Query, Timeout) ->
  tdm_connection:prepare_query(Connection, Query, Timeout).

%%% @doc
%%% Create prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% Timeout - the number of milliseconds before operation times out.
%%% UseCache - use cached preparestatements
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query(connection() | stream, string(), timeout(), boolean()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Stream = #stream{}, Query, Timeout, UseCache) ->
  tdm_stream:prepare_query(Stream, Query, Timeout, UseCache);
prepare_query(Connection, Query, Timeout, UseCache) ->
  tdm_connection:prepare_query(Connection, Query, Timeout, UseCache).

%%% @doc
%%% Create prepared statement  (asynchronous version)
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Query - string with CQL query
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% UseCache - use cached preparestatements
%%% Result - {prepared_query_id() :: binary()} or error or timeout
%%% @end
-spec prepare_query_async(Connection :: connection() | stream(), Query :: string(), ReplyTo :: async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
prepare_query_async(Stream = #stream{}, Query, ReplyTo, UseCache) ->
	tdm_stream:prepare_query_async(Stream, Query, ReplyTo, UseCache);
prepare_query_async(Connection, Query, ReplyTo, UseCache) ->
	tdm_connection:prepare_query_async(Connection, Query, ReplyTo, UseCache).

%%% @doc
%%% Execute prepared statement with default parameters & default timeout
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% ID - prepared query ID
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec execute_query(connection() | stream(), binary()) -> timeout | ok | error() | result_rows() | schema_change().
execute_query(Stream = #stream{}, ID) ->
  tdm_stream:execute_query(Stream, ID, #query_params{}, ?DEFAULT_TIMEOUT);
execute_query(Connection, ID) ->
  tdm_connection:execute_query(Connection, ID, #query_params{}, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Execute prepared statement with default parameters (asynchronous version)
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% ID - prepared query ID
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% Result - result of query or error or timeout.
-spec execute_query_async(Connection :: connection() | stream(), ID :: binary(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
execute_query_async(Stream = #stream{}, ID, ReplyTo) ->
	tdm_stream:execute_query_async(Stream, ID, #query_params{}, ReplyTo);
execute_query_async(Connection, ID, ReplyTo) ->
	tdm_connection:execute_query_async(Connection, ID, #query_params{}, ReplyTo).

%%% @doc
%%% Execute prepared statement with default parameters
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% ID - prepared query ID
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec execute_query(connection() | stream(), binary(), timeout()) -> timeout | ok | error() | result_rows() | schema_change().
execute_query(Stream = #stream{}, ID, Timeout) ->
  tdm_stream:execute_query(Stream, ID, #query_params{}, Timeout);
execute_query(Connection, ID, Timeout) ->
  tdm_connection:execute_query(Connection, ID, #query_params{}, Timeout).

%%% @doc
%%% Execute prepared statement
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% ID - prepared query ID
%%% Params - query parameters
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec execute_query(connection() | stream(), binary(), query_params(), timeout()) -> timeout | ok | error() | result_rows() | schema_change().
execute_query(Stream = #stream{}, ID, Params, Timeout) ->
  tdm_stream:execute_query(Stream, ID, Params, Timeout);
execute_query(Connection, ID, Params, Timeout) ->
  tdm_connection:execute_query(Connection, ID, Params, Timeout).


%%% @doc
%%% Execute prepared statement (asynchronous version)
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% ID - prepared query ID
%%% Params - query parameters
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% Result - result of query or error or timeout.
-spec execute_query_async(Connection :: connection() | stream(), ID :: binary(), Params :: query_params(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
execute_query_async(Stream = #stream{}, ID, Params, ReplyTo) ->
	tdm_stream:execute_query_async(Stream, ID, Params, ReplyTo);
execute_query_async(Connection, ID, Params, ReplyTo) ->
	tdm_connection:execute_query_async(Connection, ID, Params, ReplyTo).

%%% @doc
%%% Execute batch query with default timeout
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query(connection() | stream(), batch_query()) -> timeout | ok | error().
batch_query(Stream = #stream{}, Batch) ->
  tdm_stream:batch_query(Stream, Batch, ?DEFAULT_TIMEOUT);
batch_query(Connection, Batch) ->
  tdm_connection:batch_query(Connection, Batch, ?DEFAULT_TIMEOUT).

%%% @doc
%%% Execute batch query (asynchronous version)
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query_async(Connection :: connection() | stream(), Batch :: batch_query(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
batch_query_async(Stream = #stream{}, Batch, ReplyTo) ->
	tdm_stream:batch_query_async(Stream, Batch, ReplyTo);
batch_query_async(Connection, Batch, ReplyTo) ->
	tdm_connection:batch_query_async(Connection, Batch, ReplyTo).

%%% @doc
%%% Execute batch query
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query(connection() | stream(), batch_query(), timeout()) -> timeout | ok.
batch_query(Stream = #stream{}, Batch, Timeout) ->
  tdm_stream:batch_query(Stream, Batch, Timeout);
batch_query(Connection, Batch, Timeout) ->
  tdm_connection:batch_query(Connection, Batch, Timeout).


%%% @doc
%%% Execute batch query
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% Timeout - the number of milliseconds before operation times out.
%%% UseCache - use cached preparestatements
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query(connection() | stream(), batch_query(), timeout(), boolean()) -> timeout | ok | error().
batch_query(Stream = #stream{}, Batch, Timeout, UseCache) ->
  tdm_stream:batch_query(Stream, Batch, Timeout, UseCache);
batch_query(Connection, Batch, Timeout, UseCache) ->
  tdm_connection:batch_query(Connection, Batch, Timeout, UseCache).

%%% @doc
%%% Execute batch query (asynchronous version)
%%%
%%% Connection - connection to DB, as returned from get_connection() or stream
%%% Batch - batch query record. Contains list of queries in the batch. It can be prepared statements or simple DML cql queries (INSERT/UPDATE/DELETE, no SELECTs)
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% UseCache - use cached preparestatements
%%% Result - result of query or error or timeout.
%%% @end
-spec batch_query_async(Connection :: connection() | stream(), Batch :: batch_query(), ReplyTo :: async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
batch_query_async(Stream = #stream{}, Batch, ReplyTo, UseCache) ->
	tdm_stream:batch_query_async(Stream, Batch, ReplyTo, UseCache);
batch_query_async(Connection, Batch, ReplyTo, UseCache) ->
	tdm_connection:batch_query_async(Connection, Batch, ReplyTo, UseCache).

%%% @doc
%%% Subscribe to Cassandra cluster events.
%%% Events will be processed via 'cassandra_events' event manager
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% EventType - types of events to subscribe (schema_change, topology_change or status_change)
%%% Timeout - the number of milliseconds before operation times out.
%%% Result - ok | timeout | error
%%% @end
-spec subscribe_events(connection(), list(string() | atom()), timeout()) -> ok | timeout | error().
subscribe_events(Connection, EventTypes, Timeout) ->
  tdm_connection:subscribe_events(Connection, EventTypes, Timeout).

%%% @doc
%%% Subscribe to Cassandra cluster events (use default timeout)
%%% Events will be processed via 'cassandra_events' event manager
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% EventType - types of events to subscribe (schema_change, topology_change or status_change)
%%% Result - ok | timeout | error
%%% @end
-spec subscribe_events(connection(), list(string() | atom())) -> ok | timeout | error().
subscribe_events(Connection, EventTypes) ->
  tdm_connection:subscribe_events(Connection, EventTypes, ?DEFAULT_TIMEOUT).

%%% Subscribe to Cassandra cluster events (asynchronous version)
%%% Events will be processed via 'cassandra_events' event manager
%%%
%%% Connection - connection to DB, as returned from get_connection()
%%% EventType - types of events to subscribe (schema_change, topology_change or status_change)
%%% ReplyTo :: undefined | atom | pid() | fun/1 | {M, F, A} - asynchronous reply target (function or pid/name or undefined (for no reply))
%%% Result - ok | timeout | error
-spec subscribe_events_async(Connection :: connection(), EventTypes :: list(string() | atom()), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
subscribe_events_async(Connection, EventTypes, ReplyTo) ->
	tdm_connection:subscribe_events_async(Connection, EventTypes, ReplyTo).
