-module(native_parser).

-include_lib("native_protocol.hrl").

%% API
-export([parse_frame_header/1, encode_frame_header/1, parse_frame/2, encode_frame/2, parse_flags/1, encode_flags/1]).
-export([encode_int/1, parse_int/1,encode_short/1, parse_short/1, parse_string/1, encode_string/1, parse_long_string/1, encode_long_string/1]).
-export([encode_uuid/1, parse_uuid/1, encode_string_list/1, parse_string_list/1, parse_byte/1, encode_byte/1, parse_bytes/1, encode_bytes/1, parse_short_bytes/1, encode_short_bytes/1]).
-export([parse_consistency_level/1, encode_consistency_level/1]).
-export([parse_string_map/1, encode_string_map/1, parse_string_multimap/1, encode_string_multimap/1]).
-export([parse_option/1, encode_option/1, parse_option_list/1, encode_option_list/1]).
-export([parse_error/1, parse_result/1, parse_metadata/1]).
-export([encode_query_flags/1,encode_query_params/1,encode_batch_query/1,parse_event/1,encode_query/2]).
-export([encode_event_types/1]).

parse_frame_header(<<Type:1, Version:7/big-unsigned-integer, Flags:1/binary, Stream:8/big-signed-integer, OpCode:8/big-unsigned-integer>>) ->
	#header{type = parse_type(Type), version = Version, flags = parse_flags(Flags), stream = Stream, opcode = OpCode}.


encode_frame_header(#header{type = Type, version = Version, flags = Flags, stream = Stream, opcode = OpCode}) ->
	F = encode_flags(Flags),
	T = encode_type(Type),
	<<T:1, Version:7/big-unsigned-integer, F:1/binary, Stream:8/big-signed-integer, OpCode:8/big-unsigned-integer>>.

parse_type(0) -> request;
parse_type(1) -> response.

encode_type(request) -> 0;
encode_type(response) -> 1.

parse_boolean(0) -> false;
parse_boolean(1) -> true.

encode_boolean(false) -> 0;
encode_boolean(true) -> 1.

parse_flags(<<_N:6, Tracing:1, Compressing:1>>) ->
	#flags{tracing = parse_boolean(Tracing), compressing = parse_boolean(Compressing)}.

encode_flags(#flags{compressing = Compressing, tracing = Tracing}) ->
	T = encode_boolean(Tracing),
	C = encode_boolean(Compressing),
	<<0:6,T:1,C:1>>.

is_compressed(#frame{header = #header{flags = #flags{compressing = Compressed}}}) -> Compressed.

decompress_if_needed(Frame, Compression) ->
	case is_compressed(Frame) of
		true ->
			case Compression of
				{_Name, {M, _CF, DF}, _} ->
					Body = M:DF(Frame#frame.body),
					Length = iolist_size(Body),
					Header = Frame#frame.header,
					Flags = Header#header.flags,
					Frame#frame{body = Body, length = Length, header = Header#header{flags = Flags#flags{compressing = false}}};
				_ ->
					throw({unsupported_compression_type, Compression})
			end;
		_ ->
			Frame
	end.

compress_if_needed(Frame, Compression) ->
	case Compression of
		none ->
			Frame;
		{_Name, {M, CF, _DF}, Threshold} ->
			case iolist_size(Frame#frame.body) > Threshold of
				true ->
					Header = Frame#frame.header,
					Flags = Header#header.flags,
					Frame#frame{body = M:CF(Frame#frame.body), header = (Frame#frame.header)#header{flags = Flags#flags{compressing = true}}};
				_ ->
					Frame
			end;
		_ ->
      throw({unsupported_compression_type, Compression})
	end.

parse_frame(<<Header:4/binary, Length:32/big-unsigned-integer, Body:Length/binary, Rest/binary>>, Compression) ->
	F = #frame{header = parse_frame_header(Header), length = Length, body = Body},
	{decompress_if_needed(F, Compression), Rest};
parse_frame(<<Rest/binary>>, _Compressing) ->
	{undefined, Rest}.

encode_frame(Frame, Compression) ->
	F = compress_if_needed(Frame, Compression),
	#frame{header = Header, body = Body} = F,
	Length = iolist_size(Body),
	if
		Length > ?MAX_BODY_LENGTH -> throw(body_size_too_long);
		true -> ok
	end,
	H = encode_frame_header(Header),
	<<H/binary, Length:32/big-signed-integer, Body/binary>>.



parse_int(<<V:32/big-signed-integer, Rest/binary>>) ->
	{V, Rest}.

encode_int(V) ->
	<<V:32/big-signed-integer>>.

parse_byte(<<V:1/binary, Rest/binary>>) ->
	{V, Rest}.

encode_byte(V) ->
	<<V:8/big-signed-integer>>.


parse_short(<<V:16/big-unsigned-integer, Rest/binary>>) ->
	{V, Rest}.

encode_short(V) ->
	<<V:16/big-unsigned-integer>>.

parse_string(<<L:16/big-unsigned-integer, V:L/binary, Rest/binary>>) ->
	{unicode:characters_to_list(V, utf8), Rest}.


encode_string(V) ->
	B = unicode:characters_to_binary(V, utf8),
	L = byte_size(B),
	if
		L > ?MAX_SHORT_LENGTH -> throw(string_size_too_long);
		true -> ok
	end,
	<<L:16/big-unsigned-integer, B/binary>>.

parse_long_string(<<L:32/big-signed-integer, V:L/binary, Rest/binary>>) ->
	{unicode:characters_to_list(V, utf8), Rest}.

encode_long_string(V) ->
	B = unicode:characters_to_binary(V, utf8),
	L = byte_size(B),
	if
		L > ?MAX_LONG_LENGTH -> throw(string_size_too_long);
		true -> ok
	end,
	<<L:32/big-signed-integer, B/binary>>.

parse_uuid(<<V:16/binary, Rest/binary>>) ->
	{V, Rest}.

encode_uuid(V) when is_binary(V) ->
	L = byte_size(V),
	if
		L =/= 16 -> throw({invalid_uuid_byte_size, L});
		true -> ok
	end,
	V.

parse_ntimes(X, N, F) when is_binary(X), is_function(F, 1) ->
	{L, Rest} = parse_ntimes(X, N, F, []),
	{lists:reverse(L), Rest}.

parse_ntimes(X, N, F, Acc) ->
	if
		N =< 0 ->
			{Acc, X};
		true ->
			{V, X1} = F(X),
			parse_ntimes(X1, N - 1, F, [V | Acc])
	end.

encode_short_list(L, F) ->
	S = encode_short(length(L)),
	list_to_binary([S | lists:map(F, L)]).

encode_list(L, F) ->
	S = encode_int(length(L)),
	list_to_binary([S | lists:map(F, L)]).

parse_string_list(X) when is_binary(X) ->
	{N, X1} = parse_short(X),
	parse_ntimes(X1, N, fun parse_string/1).

encode_string_list(L) when is_list(L) ->
	encode_short_list(L, fun encode_string/1).

parse_bytes(X) when is_binary(X) ->
	{N, X1} = parse_int(X),
	if
		N > 0 -> parse_bytes(X1, N);
		true -> {<<>>, X1}
	end.

encode_bytes(X) when is_binary(X) ->
	list_to_binary([encode_int(byte_size(X)), X]).

parse_bytes(X1, N) ->
	<<V:N/binary, Rest/binary>> = X1,
	{V, Rest}.

parse_short_bytes(X)  ->
	{N, X1} = parse_short(X),
	parse_bytes(X1, N).

encode_short_bytes(X)  ->
	list_to_binary([encode_short(byte_size(X)), X]).


parse_option(X) ->
  {Type, X0} = parse_short(X),
  case Type of
    ?OPT_CUSTOM ->
			{Class, X1} = parse_string(X0),
			{{custom, Class}, X1};

    ?OPT_ASCII -> {ascii, X0};
    ?OPT_BIGINT -> {bigint, X0};
    ?OPT_BLOB -> {blob, X0};
    ?OPT_BOOLEAN -> {boolean, X0};
    ?OPT_COUNTER -> {counter, X0};
    ?OPT_DECIMAL -> {decimal, X0};
    ?OPT_DOUBLE -> {double, X0};
    ?OPT_FLOAT -> {float, X0};
    ?OPT_INT -> {int, X0};
    ?OPT_TEXT -> {text, X0};
    ?OPT_TIMESTAMP -> {timestamp, X0};
    ?OPT_UUID -> {uuid, X0};
    ?OPT_VARCHAR -> {varchar, X0};
    ?OPT_VARINT -> {varint, X0};
    ?OPT_TIMEUUID -> {timeuuid, X0};
    ?OPT_INET -> {inet, X0};

    ?OPT_LIST ->
      {T, X1} = parse_option(X0),
			{{list, T}, X1};

    ?OPT_MAP ->
      {KT, X1} = parse_option(X0),
      {VT, X2} = parse_option(X1),
			{{map, KT, VT}, X2};

    ?OPT_SET ->
      {T, X1} = parse_option(X0),
			{{set, T}, X1};

    _ ->
      throw({unsupported_option_type, Type})
  end.

encode_option(Type) ->
	case Type of
		custom -> encode_short(?OPT_CUSTOM);
		ascii -> encode_short(?OPT_ASCII);
		bigint -> encode_short(?OPT_BIGINT);
		blob -> encode_short(?OPT_BLOB);
		boolean -> encode_short(?OPT_BOOLEAN);
		counter -> encode_short(?OPT_COUNTER);
		decimal -> encode_short(?OPT_DECIMAL);
		double -> encode_short(?OPT_DOUBLE);
		float -> encode_short(?OPT_FLOAT);
		int -> encode_short(?OPT_INT);
		text -> encode_short(?OPT_TEXT);
		timestamp -> encode_short(?OPT_TIMESTAMP);
		uuid -> encode_short(?OPT_UUID);
		varchar -> encode_short(?OPT_VARCHAR);
		varint -> encode_short(?OPT_VARINT);
		timeuuid -> encode_short(?OPT_TIMEUUID);
		inet -> encode_short(?OPT_INET);
		{list, T} -> list_to_binary([encode_short(?OPT_LIST), encode_option(T)]);
		{map, K, V} ->  list_to_binary([encode_short(?OPT_MAP), encode_option(K), encode_option(V)]);
		{set, T} -> list_to_binary([encode_short(?OPT_SET), encode_option(T)]);
		_ -> throw({unsupported_value_type, Type})
	end.


parse_option_list(X)  when is_binary(X)->
	{N, X1} = parse_short(X),
	parse_ntimes(X1, N, fun parse_option/1).

encode_option_list(X) when is_list(X) ->
	encode_list(X, fun encode_option/1).


parse_consistency_level(X) ->
	{R, Rest} = parse_short(X),
	{int_to_consistency(R), Rest}.

encode_consistency_level(X) ->
	encode_short(consistency_to_int(X)).

%%  any | one | quorum | all | local_quorum | each_quorum | serial | local_serial | local_one
consistency_to_int(X) ->
	case X of
		any -> ?CONSISTENCY_ANY;
		one -> ?CONSISTENCY_ONE;
		two -> ?CONSISTENCY_TWO;
		three -> ?CONSISTENCY_THREE;
		quorum -> ?CONSISTENCY_QUORUM;
		all -> ?CONSISTENCY_ALL;
		local_quorum -> ?CONSISTENCY_LOCAL_QUORUM;
		each_quorum -> ?CONSISTENCY_EACH_QUORUM;
		serial -> ?CONSISTENCY_SERIAL;
		local_serial -> ?CONSISTENCY_LOCAL_SERIAL;
		local_one -> ?CONSISTENCY_LOCAL_ONE;
		_ -> undefined
	end.



int_to_consistency(X) ->
	case X of
		?CONSISTENCY_ANY -> any;
		?CONSISTENCY_ONE -> one;
		?CONSISTENCY_TWO -> two ;
		?CONSISTENCY_THREE -> three;
		?CONSISTENCY_QUORUM -> quorum;
		?CONSISTENCY_ALL -> all;
		?CONSISTENCY_LOCAL_QUORUM -> local_quorum;
		?CONSISTENCY_EACH_QUORUM -> each_quorum;
		?CONSISTENCY_SERIAL -> serial;
		?CONSISTENCY_LOCAL_SERIAL -> local_serial;
		?CONSISTENCY_LOCAL_ONE -> local_one;
		_ -> undefined
	end.

parse_string_map(X) ->
	{N, X1} = parse_short(X),
	parse_ntimes(X1, N, fun parse_string_pair/1).

encode_string_map(X) when is_list(X) ->
	encode_short_list(X, fun encode_string_pair/1).

parse_string_multimap(X) ->
	{N, X1} = parse_short(X),
	parse_ntimes(X1, N, fun parse_string_pair_with_list/1).

encode_string_multimap(X) when is_list(X) ->
	encode_short_list(X, fun encode_string_pair_with_list/1).


parse_string_pair(X) ->
	{K, X1} = parse_string(X),
	{V, Rest} = parse_string(X1),
	{{K, V}, Rest}.

encode_string_pair({K, V}) ->
	BK = encode_string(K),
	BV = encode_string(V),
	<<BK/binary, BV/binary>>.

parse_string_pair_with_list(X) ->
	{K, X1} = parse_string(X),
	{V, Rest} = parse_string_list(X1),
	{{K, V}, Rest}.

encode_string_pair_with_list({K, V}) ->
	BK = encode_string(K),
	BV = encode_string_list(V),
	<<BK/binary, BV/binary>>.


parse_error(#frame{body = Body}) ->
	{Code, X0} = parse_int(Body),
	{Message, X1} = parse_string(X0),
	Error = #error{error_code = Code, message = Message},
	case Code of
		?ERR_SERVER_ERROR ->
			Error#error{type = server_error};
		?ERR_PROTOCOL_ERROR ->
			Error#error{type = protocol_error};
		?ERR_BAD_CREDENTIALS ->
			Error#error{type = bad_credentials};
		?ERR_UNAVAILABLE_EXCEPTION ->
			{CL, XC0} = parse_consistency_level(X1),
			{Required, XC1} = parse_int(XC0),
			{Alive, _XC2} = parse_int(XC1),
			Error#error{type = unavailable_exception, additional_info = [{consistency_level, CL}, {nodes_required, Required}, {nodes_alive, Alive}]};
		?ERR_OVERLOADED ->
			Error#error{type = coordinator_node_is_overloaded};
		?ERR_IS_BOOTSTRAPING ->
			Error#error{type = coordinator_node_is_bootstrapping};
		?ERR_TRUNCATE_ERROR ->
			Error#error{type = truncate_error};
		?ERR_WRITE_TIMEOUT ->
			{CL, XT0} = parse_consistency_level(X1),
			{Received, XT1} = parse_int(XT0),
			{BlockFor, XT2} = parse_int(XT1),
			{WriteType, _XT3} = parse_string(XT2),
			Error#error{type = write_timeout, additional_info = [{consistency_level, CL}, {nodes_received, Received}, {nodes_required, BlockFor}, {write_type, WriteType}]};
		?ERR_READ_TIMEOUT ->
			{CL, XT0} = parse_consistency_level(X1),
			{Received, XT1} = parse_int(XT0),
			{BlockFor, XT2} = parse_int(XT1),
			{DataPresent, _XT3} = parse_byte(XT2),
			DP = if DataPresent =:= 0 -> false; true -> true end,
			Error#error{type = read_timeout, additional_info = [{consistency_level, CL}, {nodes_received, Received}, {nodes_required, BlockFor}, {data_present, DP}]};
		?ERR_SYNTAX_ERROR ->
			Error#error{type = syntax_error};
		?ERR_UNATHORIZED ->
			Error#error{type = unathorized};
		?ERR_INVALID ->
			Error#error{type = invalid_query};
		?ERR_CONFIG_ERROR ->
			Error#error{type = config_error};
		?ERR_ALREADY_EXISTS ->
			{Keyspace, XE0} = parse_string(X1),
			{Table, _XE1} = parse_string(XE0),
			Error#error{type = already_exists, additional_info = [{keyspace, Keyspace}, {table, Table}]};
		?ERR_UNPREPARED ->
			{ID, _X2} = parse_short_bytes(X1),
			Error#error{type = unprepared_query, additional_info = [{query_id, ID}]};
		_ ->
			Error
	end.

parse_result(#frame{body = Body}) ->
	{Kind, X0} = parse_int(Body),
	case Kind of
		?RES_VOID ->
			ok;
		?RES_ROWS ->
			{V, _Rest} = parse_rows(X0),
      V;
		?RES_KEYSPACE ->
			{KS, _X1} = parse_string(X0),
      {keyspace, KS};
		?RES_PREPARED ->
			{ID, X1} = parse_short_bytes(X0),
			{Metadata, X2} = parse_metadata(X1),
			{ResultMetadata, _X3} = if
        X2 =/= <<>> -> parse_metadata(X2);
        true -> {undefined, undefined}
      end,
			{ID, Metadata, ResultMetadata};
		?RES_SCHEMA ->
			{Change, X1} = parse_string(X0),
			{Keyspace, X2} = parse_string(X1),
			{Table, _X3} = parse_string(X2),
			case Change of
				"CREATED" -> {created, Keyspace, Table};
				"UPDATED" -> {updated, Keyspace, Table};
				"DROPPED" -> {dropped, Keyspace, Table};
				_ -> {Change, Keyspace, Table}
			end;
		_ ->
			{error, {unknown_result_kind, Kind}}
	end.


parse_metadata(Data) ->
	{Flags, X0} = parse_int(Data),
	{ColumnCount, X0_1} = parse_int(X0),
	<<_N:29,NoMeta:1,HasMorePage:1,GlobalTableSpec:1>> = <<Flags:32/big-signed-integer>>,
	{PagingState, X1} = if
												HasMorePage =:= 1 ->
													parse_bytes(X0_1);
												true ->
													{undefined, X0_1}
											end,
	if
		NoMeta =:= 1 ->
			{{[[] || _X <- lists:seq(1, ColumnCount)], PagingState}, X1};
		true ->
			{Keyspace, Table, X2} = if
																GlobalTableSpec =:= 1 ->
																	{T, X1_0} = parse_string(X1),
																	{C, X1_1} = parse_string(X1_0),
																	{T, C, X1_1};
																true ->
																	{undefined, undefined, X1}
															end,
			{CS, Rest} = parse_colspecs(X2, ColumnCount, [], Keyspace, Table),
			{{lists:reverse(CS), PagingState}, Rest}
	end.

parse_rows(Data) ->
	{{ColSpecs, PagingState}, X0} = parse_metadata(Data),
	{RowCount, X1} = parse_int(X0),
	{Rows, Rest} = parse_rows(X1, RowCount, [], ColSpecs),
	{{ColSpecs, PagingState, lists:reverse(Rows)}, Rest}.

parse_rows(Data, Count, Acc, ColSpecs) ->
	if
		Count > 0 ->
			{Row, X0} = parse_row(Data, [], ColSpecs),
			parse_rows(X0, Count - 1, [lists:reverse(Row) | Acc], ColSpecs);
		true ->
			{Acc, Data}
	end.

parse_row(Data, Acc, ColSpecs) ->
	case ColSpecs of
		[] ->
			{Acc, Data};
		[H | T] ->
      {V, X0} = case H of
        [] -> %% no metadata, use blob
          cql_types:decode(blob, Data);
        {_K, _T, _N, Type} ->
          cql_types:decode(Type, Data);
        U ->
          error_logger:error_msg("Uparseable colspec ~p~n", [U])
      end,
			parse_row(X0, [V | Acc], T)
	end.

parse_colspecs(Data, Count, Acc, Keyspace, Table) ->
	if
		Count > 0 ->
			{K, T, X0} = if
										 Keyspace =/= undefined ->
											 {Keyspace, Table, Data};
										 true ->
											 {KsName, X0_0} = parse_string(Data),
											 {TbName, X0_1} = parse_string(X0_0),
											 {KsName, TbName, X0_1}
									 end,
			{Name, X1} = parse_string(X0),
			{Type, X2} = parse_option(X1),
			parse_colspecs(X2, Count - 1, [{K, T, Name, Type} | Acc], Keyspace, Table);

		true ->
			{Acc, Data}
	end.

encode_event_types(EventTypes) ->
	encode_string_list(lists:map(fun(X) ->
	  case X of
			topology_change -> "TOPOLOGY_CHANGE";
			status_change -> "STATUS_CHANGE";
			schema_change -> "SCHEMA_CHANGE";
			X -> if
					   is_atom(X) -> string:to_upper(atom_to_list(X));
		         is_list(X) -> string:to_upper(X);
					   true -> throw(unknown_event_type)
				   end
		end
	end, EventTypes)).

parse_event(#frame{body = Body}) ->
	{EventType, X0} = parse_string(Body),
	case EventType of
		"TOPOLOGY_CHANGE" ->
			{ChangeType, X1} = parse_string(X0),
			{Inet, _Rest} = parse_string(X1),
			case ChangeType of
				"NEW_NODE" ->
					{topology_change, new_node, Inet};
				"REMOVED_NODE" ->
					{topology_change, removed_node, Inet};
				_ ->
					{topology_change, unknown, Inet}
			end;
		"STATUS_CHANGE" ->
			{Status, X1} = parse_string(X0),
			{Inet, _Rest} = parse_string(X1),
			case Status of
				"UP" ->
					{status_change, node_up, Inet};
				"DOWN" ->
					{status_change, node_down, Inet};
				_ ->
					{status_change, unknown, Inet}
			end;
		"SCHEMA_CHANGE" ->
			{ChangeType, X1} = parse_string(X0),
			{Keyspace, X2} = parse_string(X1),
			{Table, _Rest} = parse_string(X2),
			case ChangeType of
				"CREATED" ->
					{schema_change, created, Keyspace, Table};
				"UPDATED" ->
					{schema_change, updated, Keyspace, Table};
				"DROPPED" ->
					{schema_change, dropped, Keyspace, Table};
				_ ->
					{schema_change, unknown, Keyspace, Table}
			end;
		_ ->
			{unknown_event_type, EventType, Body}
	end.

encode_query_flags(#query_params{bind_values = Bind, skip_metadata = SkipMetadata, page_size = PageSize, paging_state = PagingState, serial_consistency = SerialConsistency}) ->
	F0 = case Bind of
				 undefined -> 0;
				 [] -> 0;
				 _ -> 16#01
			 end,
	F1 = if SkipMetadata =:= false -> 0; true -> 16#02 end,
	F2 = if PageSize =:= undefined -> 0; true -> 16#04 end,
	F3 = if PagingState =:= undefined -> 0; true -> 16#08 end,
	F4 = if SerialConsistency =:= undefined -> 0; true -> 16#10 end,
	<<(F0 bor F1 bor F2 bor F3 bor F4):8/big-unsigned-integer>>.


encode_query_params(Params = #query_params{consistency_level = Consistency, bind_values = Bind, page_size = ResultPageSize, paging_state = PagingState, serial_consistency = SerialConsistency}) ->
	CL = encode_consistency_level(Consistency),
	Flags = encode_query_flags(Params),
	Vars = encode_values(Bind),
	RPS = if ResultPageSize =:= undefined -> <<>>; true -> encode_int(ResultPageSize) end,
	SCL = if SerialConsistency =:= undefined -> <<>>; true -> encode_consistency_level(SerialConsistency) end,
	PS = if PagingState =:= undefined -> <<>>; true -> encode_bytes(PagingState) end,
	<<CL/binary,Flags/binary,Vars/binary,RPS/binary,PS/binary,SCL/binary>>.


encode_batch_type(BatchType) ->
	R = case BatchType of
				logged -> ?BATCH_LOGGED;
				unlogged -> ?BATCH_UNLOGGED;
				counter -> ?BATCH_COUNTER;
				_ -> ?BATCH_LOGGED
			end,
	encode_byte(R).

encode_batch_query(#batch_query{batch_type = BatchType, queries = Queries, consistency_level = Consistency}) ->
	BT = encode_batch_type(BatchType),
	QC = encode_short(length(Queries)),
	QL = list_to_binary(lists:map(fun encode_single_query/1, Queries)),
	CL = encode_consistency_level(Consistency),
	<<BT/binary,QC/binary,QL/binary,CL/binary>>.

encode_single_query({ID, Binds}) when is_binary(ID), is_list(Binds) ->  %% prepared statement
	B = encode_values(Binds),
	Q = encode_short_bytes(ID),
	<<1:8/big-signed-integer,Q/binary,B/binary>>;
encode_single_query({Query, Binds}) when is_list(Query), is_list(Binds) -> %% query statement
	Q = encode_long_string(Query),
	B = encode_values(Binds),
	<<0:8/big-signed-integer,Q/binary,B/binary>>.

encode_values(BindValues) ->
	N = encode_short(length(BindValues)),

	V = list_to_binary(lists:map(fun(X) ->
    cql_types:encode_t(X)
  end,  BindValues)),
	<<N/binary,V/binary>>.


-spec encode_query(binary() | string(), #query_params{}) -> binary().
encode_query(Query, Params) when is_list(Query) ->
	Q = encode_long_string(Query),
	P = encode_query_params(Params),
	<<Q/binary,P/binary>>;
encode_query(Query, Params) when is_binary(Query) ->
	Q = encode_short_bytes(Query),
	P = encode_query_params(Params),
	<<Q/binary,P/binary>>.