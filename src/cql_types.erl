-module(cql_types).

-include_lib("cql_types.hrl").

%% API
-export([decode_t/2, decode_v/2, decode_tv/2, encode_t/1, encode/2, erase_type/1, decode/2]).


-spec decode_t(cql_value_type(), cql_data()) -> {cql_tagged_value(), cql_data()}.
decode_t(Type, D) ->
	{V, R} = decode(Type, D),
	{{Type, V}, R}.

decode_v(Type, D) ->
  element(1, decode(Type, D)).

decode_tv(Type, D) ->
  element(1, decode_t(Type, D)).

-spec encode_t(cql_tagged_value()) -> cql_data().
encode_t({Type, Value}) ->
	encode(Type, Value).


-spec erase_type(cql_tagged_value()) -> cql_value().
erase_type({_Type, Value}) ->
	Value.


-spec decode(cql_value_type(), cql_data()) -> {cql_value(), cql_data()}.
decode(Type, Value) ->
  decode(Type, Value, int).

get_length(Data, IntSize) ->
  S = case IntSize of
    short -> 16;
    int -> 32
  end,
  <<Length:S/signed, Rest/binary>> = Data,
  {Length, Rest}.

-spec decode(cql_value_type(), cql_data(), int | short) -> {cql_value(), cql_data()}.
decode(Type, Value, IntSize) ->
%%   decode_int
  {Length, Data} = get_length(Value, IntSize),
  if
    Length > 0 ->
      <<D:Length/binary, Rest/binary>> = Data,
      V = case Type of
            {custom, _Class} -> D;
            ascii -> decode_string(D, ascii);
            bigint -> decode_int(D);
            blob -> D;
            boolean -> decode_boolean(D);
            counter -> decode_int(D);
            decimal -> decode_decimal(D);
            double -> decode_float(D);
            float -> decode_float(D);
            int -> decode_int(D);
            text -> decode_string(D, unicode);
            timestamp -> decode_int(D);
            uuid -> D;      %% todo: make uuid generator & utils http://johannburkard.de/software/uuid/
            varchar -> decode_string(D, unicode);
            varint -> decode_int(D);
            timeuuid -> D;  %% todo: make time uuid generator & utils  http://johannburkard.de/software/uuid/
            inet -> decode_inet(D);

            {list, ValueType} ->
              <<L:16/big-unsigned-integer, X0/binary>> = D,
              decode_collection(X0, L, ValueType);

            {map, KeyType, ValueType} ->
              <<L:16/big-unsigned-integer, X0/binary>> = D,
              decode_map(X0, L, KeyType, ValueType);

            {set, ValueType} ->
              <<L:16/big-unsigned-integer, X0/binary>> = D,
              decode_collection(X0, L, ValueType);

            _ ->
              throw({unsupported_type, Type})
          end,
      {V, Rest};
    true ->
      {undefined, Data}
  end.



-spec encode(cql_value_type(), cql_value()) -> cql_data().
encode(Type, Value) ->
  encode(Type, Value, int).

-spec encode(cql_value_type(), cql_value(), int | short) -> cql_data().
encode(Type, Value, IntSize) ->
  if
    Value =/= undefined ->
      D = case Type of
            {custom, _Class} -> Value;
            ascii -> encode_string(Value, ascii);
            bigint -> encode_int(Value, long);
            blob -> Value;
            boolean -> encode_boolean(Value);
            counter -> encode_int(Value, long);
            decimal -> encode_decimal(Value);
            double -> encode_float(Value, double);
            float -> encode_float(Value, float);
            int -> encode_uint(Value, int);
            text -> encode_string(Value, unicode);
            timestamp -> encode_int(Value, long);
            uuid -> Value;
            varchar -> encode_string(Value, unicode);
            varint -> encode_int(Value, bigint);
            timeuuid -> Value;
            inet -> encode_inet(Value);

            {list, ValueType} ->
              L = encode_uint(length(Value), short),
              C = lists:map(fun(X) -> encode(ValueType, X, short) end, Value),
              list_to_binary([L| C]);

            {map, KeyType, ValueType} ->
              L = encode_uint(length(Value), short),
              C = lists:map(fun({K, V}) -> [encode(KeyType, K, short), encode(ValueType, V, short)] end, Value),
              list_to_binary([L| C]);

            {set, ValueType} ->
              L = encode_uint(length(Value), short),
              C = lists:map(fun(X) -> encode(ValueType, X, short) end, Value),
              list_to_binary([L| C]);

            _ ->
              throw({unsupported_type, Type})
          end,
      Length = encode_int(byte_size(D), IntSize),
      <<Length/binary,D/binary>>;

    true ->
      encode_int(-1, IntSize)
  end.

-spec decode_collection(binary(), non_neg_integer(), cql_value_type()) -> list(cql_value()).
decode_collection(X, N, T)  ->
	{L, _Rest} = decode_collection(X, N, T, []),
	lists:reverse(L).

-spec decode_collection(binary(), non_neg_integer(), cql_value_type(), list(cql_value())) -> list(cql_value()).
decode_collection(X, N, T, Acc) ->
	if
		N =< 0 ->
			{Acc, X};
		true ->
      {V, X1} = decode(T, X, short),
%%       <<L:32, VX:L/binary-unit:8, X1>> = X,
%% 			{V, _R} = decode(T, VX),
%%       {V, X1},
			decode_collection(X1, N - 1, T, [V | Acc])
	end.

-spec decode_map(binary(), non_neg_integer(), cql_value_type(), cql_value_type()) -> list({cql_value(), cql_value()}).
decode_map(X, N, K, V)  ->
  {L, _Rest} = decode_map(X, N, K, V, []),
  lists:reverse(L).

-spec decode_map(binary(), non_neg_integer(), cql_value_type(), cql_value_type(), list({cql_value(), cql_value()})) -> list({cql_value(), cql_value()}).
decode_map(X, N, K, V, Acc) ->
  if
    N =< 0 ->
      {Acc, X};
    true ->
      {KV, X1} = decode(K, X, short),
      {VV, X2} = decode(V, X1, short),
      decode_map(X2, N - 1, K, V, [{KV, VV} | Acc])
  end.

-spec decode_string(binary(), unicode | ascii) -> list(integer()).
decode_string(V, unicode) ->  unicode:characters_to_list(V, utf8);
decode_string(V, ascii) ->  binary_to_list(V).

-spec encode_string(list(integer()), unicode | ascii) -> binary().
encode_string(V, ascii) -> list_to_binary(V);
encode_string(V, unicode) -> unicode:characters_to_binary(V, utf8).

-spec decode_int(binary()) -> integer().
decode_int(<<V:8/big-signed-integer>>) -> V;
decode_int(<<V:16/big-signed-integer>>) -> V;
decode_int(<<V:32/big-signed-integer>>) -> V;
decode_int(<<V:64/big-signed-integer>>) -> V;
decode_int(X) when is_binary(X) ->
  L = byte_size(X) * 8,
  <<V:L/big-signed-integer>> = X,
  V.

-spec encode_int(integer(), byte | short | int | long | big) -> binary().
encode_int(V, byte) -> <<V:8/big-signed-integer>>;
encode_int(V, short) -> <<V:16/big-signed-integer>>;
encode_int(V, int) -> <<V:32/big-signed-integer>>;
encode_int(V, long) -> <<V:64/big-signed-integer>>;
encode_int(V, bigint) ->
  L = int_size(V) * 8,
  <<V:L/big-signed-integer>>.

-spec decode_uint(binary()) -> binary().
decode_uint(<<V:8/big-unsigned-integer>>) -> V;
decode_uint(<<V:16/big-unsigned-integer>>) -> V;
decode_uint(<<V:32/big-unsigned-integer>>) -> V;
decode_uint(<<V:64/big-unsigned-integer>>) -> V;
decode_uint(X) when is_binary(X) ->
  L = byte_size(X) * 8,
  <<V:L/big-unsigned-integer>> = X,
  V.

-spec encode_uint(non_neg_integer(), byte | short | int | long | big) -> binary().
encode_uint(V, byte) -> <<V:8/big-unsigned-integer>>;
encode_uint(V, short) -> <<V:16/big-unsigned-integer>>;
encode_uint(V, int) -> <<V:32/big-unsigned-integer>>;
encode_uint(V, long) -> <<V:64/big-unsigned-integer>>;
encode_uint(V, bigint) -> binary:encode_unsigned(V).


-spec decode_float(binary()) -> float().
decode_float(<<V:32/big-float>>) -> V;
decode_float(<<V:64/big-float>>) -> V.

-spec encode_float(binary(), float | double) -> float().
encode_float(V, float) -> <<V:32/big-float>>;
encode_float(V, double) -> <<V:64/big-float>>.

-spec decode_boolean(binary()) -> boolean().
decode_boolean(<<V:1/binary>>) -> V =/= <<0>>.

-spec encode_boolean(boolean()) -> binary().
encode_boolean(V) ->  if V =/= true -> <<0>>; true -> <<1>> end.

-spec int_size(integer()) -> pos_integer().
int_size(Value) ->
  if
    Value < 0 -> byte_size(binary:encode_unsigned(-Value * 2)); %% little hack - (value * 2) for negative numbers
    true -> byte_size(binary:encode_unsigned(Value))
  end.

-spec decode_decimal(binary()) -> big_decimal().
decode_decimal(Data) ->
  <<Scale:32/big-signed-integer, Value/binary>> = Data,
  L = byte_size(Value) * 8,
  <<V:L/big-signed-integer>> = Value,
  #decimal{scale = Scale, value = V}.

-spec encode_decimal(big_decimal()) -> {binary()}.
encode_decimal(#decimal{scale = Scale, value = Value}) ->
	L = int_size(Value) * 8,
  <<Scale:32/big-signed-integer, Value:L/big-signed-integer>>.

-spec decode_inet(binary()) -> inet().
decode_inet(<<A:8/big-unsigned-integer, B:8/big-unsigned-integer, C:8/big-unsigned-integer, D:8/big-unsigned-integer>>) ->
  #inet{ip = {A, B, C, D}};
decode_inet(<<A:16/big-unsigned-integer, B:16/big-unsigned-integer, C:16/big-unsigned-integer, D:16/big-unsigned-integer,
             E:16/big-unsigned-integer, F:16/big-unsigned-integer, G:16/big-unsigned-integer, H:16/big-unsigned-integer>>) ->
  #inet{ip = {A, B, C, D, E, F, G, H}}.

-spec encode_inet(inet()) -> binary().
encode_inet(#inet{ip = {A, B, C, D}}) ->
  <<A:8/big-unsigned-integer, B:8/big-unsigned-integer, C:8/big-unsigned-integer, D:8/big-unsigned-integer>>;
encode_inet(#inet{ip = {A, B, C, D, E, F, G, H}}) ->  %% , port = P
  <<A:16/big-unsigned-integer, B:16/big-unsigned-integer, C:16/big-unsigned-integer, D:16/big-unsigned-integer,
    E:16/big-unsigned-integer, F:16/big-unsigned-integer, G:16/big-unsigned-integer, H:16/big-unsigned-integer>>.

