-module(teledamus).

-include_lib("teledamus.hrl").

%% API
-export([]).

-type connection() :: pid().
-type error() :: {error, Reason :: term()}.

-type consistency_level() :: any | one | quorum | all | local_quorum | each_quorum | serial | local_serial | local_one.
-type serial_consistency_level() :: serial | local_serial.


-type string() :: list() | binary().
-type query_text() :: string().
-type cql_variable_name() :: string().
-type cql_value() :: any().
-type cql_plain_type() :: ascii | bigint | blob | boolean | counter | decimal | double | float | int | text | timestamp | uuid | varchar | varint | timeuuid | inet.
-type cql_list_type() :: {list, ValueType :: cql_plain_type()}.
-type cql_set_type() :: {set, ValueType :: cql_plain_type()}.
-type cql_map_type() :: {map, KeyType :: cql_plain_type(), ValueType :: cql_plain_type()}.
-type cql_custom_type() :: {custom, JavaClassName :: string()}.
-type cql_tuple_type() :: tuple(). %% tuple of any size, containing cql_plain_typle() specs
-type cql_udt_type() :: {udt, [{FieldName :: atom(), cql_type()}]}.
-type cql_type() :: cql_plain_type() | cql_list_type() | cql_set_type() | cql_map_type() | cql_custom_type() | cql_tuple_type() | cql_udt_type().

%% -type simple_query() :: {query_text(), typed_bind_variables()}.
%% -type typed_bind_variables() :: [typed_cql_value()] | maps:map(cql_variable_name(), typed_cql_value()).
%% -type typed_cql_value() :: {cql_type(), cql_value()}.
%%
%% -type prepared_query() :: {prepared_query_id(), untyped_bind_variables()}.
-type prepared_query_id() :: binary().
%% -type untyped_bind_variables() :: [cql_value()] | maps:map(cql_variable_name(), cql_value()).
%%
%% -type batch_query() :: {batch_type(), [simple_query() | prepared_query()]}.
-type batch_type() :: logged_batch | unlogged_batch | counter_batch.

-type query() :: string() | prepared_query_id().
-type batch_query() :: {batch_type(), [query()]}.

-type statement() :: #tdm_statement{}.

-type protocol_version() :: 1 | 2 | 3.
-type credentials() :: {UserName :: string(), Password :: string()}.

-export_type([string/0, connection/0, error/0, query_text/0, cql_variable_name/0, cql_value/0, cql_value/0, cql_plain_type/0, cql_list_type/0, cql_map_type/0,
    cql_set_type/0, cql_custom_type/0, cql_udt_type/0, cql_type/0, simple_query/0, typed_bind_variables/0, typed_cql_value/0, prepared_query/0, prepared_query_id/0,
    untyped_bind_variables/0, batch_query/0, batch_type/0, consistency_level/0, query/0, statement/0, query_params/0, serial_consistency_level/0,
    protocol_version/0, credentials/0]).




-spec use(Keyspace :: string()) -> ok | error().
-spec query(Statement :: statement(), BindVars :: bind_variables()) -> ok | rows () | boolean() | error().
-spec query(Statement :: statement(), BindVars :: bind_variables(), PageSize :: pos_integer(), PageState :: binary()) -> rows () | error().
-spec query(Statement :: statement(), BindVars :: bind_variables(), PageFun :: fun((rows()) -> ok | error())) -> ok | error().
