%% cql types
-type cql_value_type() :: atom() | term(). %% should be recursive type definition, but it's not supported by erlang
-type cql_data() :: binary().
-type cql_value() :: any().
-type cql_tagged_value() :: {cql_value_type(), cql_value()}.
-type cql_string() :: string() | list(non_neg_integer()).
-export_type([cql_value_type/0, cql_data/0, cql_value/0, cql_tagged_value/0, cql_string/0]).

%% custom data format
-record(tdm_decimal, {scale :: integer(), value :: integer()}).
-type big_decimal() :: #tdm_decimal{}.
-export_type([big_decimal/0]).

-type ipv4() :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type ipv6() :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-record(tdm_inet, {ip :: ipv4() | ipv6()}).
-type inet() :: #tdm_inet{}.
-export_type([ipv4/0, ipv6/0, inet/0]).
