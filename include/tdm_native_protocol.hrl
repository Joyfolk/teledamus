-define(MAX_BODY_LENGTH, 268435455). %% from spec: currently a frame is limited to 256MB in length
-define(MAX_SHORT_LENGTH, 65535).
-define(MAX_LONG_LENGTH, 2147483647).

-record(tdm_flags, {compressing = false:: boolean(), tracing = false:: boolean()}).
-record(tdm_header, {type :: request | response, version = 2::0..127, flags = #tdm_flags{} :: #tdm_flags{}, stream = 1 :: -127..128, opcode :: byte()}).
-record(tdm_frame, {header :: #tdm_header{}, length :: integer(), body :: binary()}).
-record(tdm_error, {error_code :: integer(), type :: atom(), message :: string(), additional_info :: term()}).




%%
%% opcode values
%% An integer byte that distinguish the actual message:
%%
-define(OPC_ERROR, 0).
-define(OPC_STARTUP, 1).
-define(OPC_READY, 2).
-define(OPC_AUTHENTICATE, 3).
-define(OPC_OPTIONS, 5).
-define(OPC_SUPPORTED, 6).
-define(OPC_QUERY, 7).
-define(OPC_RESULT, 8).
-define(OPC_PREPARE, 9).
-define(OPC_EXECUTE, 10).
-define(OPC_REGISTER, 11).
-define(OPC_EVENT, 12).
-define(OPC_BATCH, 13).
-define(OPC_AUTH_CHALLENGE, 14).
-define(OPC_AUTH_RESPONSE, 15).
-define(OPC_AUTH_SUCCESS, 16).


%%
%% A consistency level specification. This is a byte
%% representing a consistency level with the following correspondance:
%%
-define(CONSISTENCY_ANY, 0).
-define(CONSISTENCY_ONE, 1).
-define(CONSISTENCY_TWO, 2).
-define(CONSISTENCY_THREE, 3).
-define(CONSISTENCY_QUORUM, 4).
-define(CONSISTENCY_ALL, 5).
-define(CONSISTENCY_LOCAL_QUORUM, 6).
-define(CONSISTENCY_EACH_QUORUM, 7).
-define(CONSISTENCY_SERIAL, 8).
-define(CONSISTENCY_LOCAL_SERIAL, 9).
-define(CONSISTENCY_LOCAL_ONE, 10).

-type consistency_level() :: any | one | quorum | all | local_quorum | each_quorum | serial | local_serial | local_one.

%% CQL VERSION
-define(CQL_VERSION, <<"3.0.0">>).

%% ERROR CODES
-define(ERR_SERVER_ERROR,    16#0000).
-define(ERR_PROTOCOL_ERROR,  16#000A).
-define(ERR_BAD_CREDENTIALS, 16#0100).
-define(ERR_UNAVAILABLE_EXCEPTION, 16#1000).
-define(ERR_OVERLOADED,      16#1001).
-define(ERR_IS_BOOTSTRAPING, 16#1002).
-define(ERR_TRUNCATE_ERROR,  16#1003).
-define(ERR_WRITE_TIMEOUT,   16#1100).
-define(ERR_READ_TIMEOUT,    16#1200).
-define(ERR_SYNTAX_ERROR,    16#2000).
-define(ERR_UNATHORIZED,     16#2100).
-define(ERR_INVALID,         16#2200).
-define(ERR_CONFIG_ERROR,    16#2300).
-define(ERR_ALREADY_EXISTS,  16#2400).
-define(ERR_UNPREPARED,      16#2500).


%% RESULT KIND
-define(RES_VOID,     16#0001).
-define(RES_ROWS,     16#0002).
-define(RES_KEYSPACE, 16#0003).
-define(RES_PREPARED, 16#0004).
-define(RES_SCHEMA,   16#0005).


%% OPTION TYPES
-define(OPT_CUSTOM,    16#0000).
-define(OPT_ASCII,     16#0001).
-define(OPT_BIGINT,    16#0002).
-define(OPT_BLOB,      16#0003).
-define(OPT_BOOLEAN,   16#0004).
-define(OPT_COUNTER,   16#0005).
-define(OPT_DECIMAL,   16#0006).
-define(OPT_DOUBLE,    16#0007).
-define(OPT_FLOAT,     16#0008).
-define(OPT_INT,       16#0009).
-define(OPT_TEXT,      16#000A).
-define(OPT_TIMESTAMP, 16#000B).
-define(OPT_UUID,      16#000C).
-define(OPT_VARCHAR,   16#000D).
-define(OPT_VARINT,    16#000E).
-define(OPT_TIMEUUID,  16#000F).
-define(OPT_INET,      16#0010).
-define(OPT_LIST,      16#0020).
-define(OPT_MAP,       16#0021).
-define(OPT_SET,       16#0022).


%% queries
-record(tdm_query_params, {consistency_level = quorum :: consistency_level(), skip_metadata = false:: boolean(), page_size :: integer(),
                           bind_values = []:: list(any()), paging_state :: binary(), serial_consistency = undefined :: consistency_level()}).

-type query_params() :: #tdm_query_params{}.
-export_type([query_params/0]).

-define(BATCH_LOGGED, 0).
-define(BATCH_UNLOGGED, 1).
-define(BATCH_COUNTER, 2).

-type batch_type() :: logged | unlogged | counter.
-type prepared_batch_query() :: {binary(), list()}.
-type simple_batch_query() :: {string(), list()}.
-type single_batch_query() :: prepared_batch_query() | simple_batch_query().

-record(tdm_batch_query, {batch_type = logged :: batch_type(), queries :: list(single_batch_query()), consistency_level = quorum:: consistency_level()}).
-type batch_query() :: #tdm_batch_query{}.


-record(tdm_connection, {pid :: pid(), host :: list(), port :: pos_integer(), default_stream :: stream()}).
-record(tdm_stream, {connection :: connection(), stream_pid :: pid(), stream_id :: 1..127}).

-type error() :: #tdm_error{}.
-type stream() :: #tdm_stream{}.
-type connection() :: #tdm_connection{}.
-type compression() :: none | lz4 | snappy.
-type options() :: [{string(), string()}].
-type keyspace() :: {keyspace, string()}.
-type schema_change() :: {created, string(), string()} | {updated, string(), string()} | {dropped, string(), string()}.
-type paging_state() :: binary().
-type colspec() :: [{string(), string(), string(), atom()}].
-type metadata() :: {colspec(), paging_state()}.
-type rows() :: [list()].
-type socket() :: gen_tcp:socket() | ssl:sslsocket().
-type transport() :: tcp | ssl.
-type result_rows() :: {metadata(), paging_state(), rows()}.
-export_type([connection/0, error/0, options/0, keyspace/0, schema_change/0, paging_state/0, metadata/0, rows/0, batch_query/0, result_rows/0, stream/0, compression/0, socket/0, transport/0]).

-type async_target() :: undefined | atom() | pid() | fun(() -> any()) | {atom(), atom(), list()}.
-export_type([async_target/0]).