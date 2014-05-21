-module(stream).

%% -behaviour(gen_server).

%% API
-export([start/3]).

%% -export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, from_cache/2, to_cache/3, query/5, prepare_query/4, batch_query/4, handle_frame/2, init/3]).
-export([options_async/2, query_async/4, query_async/5, prepare_query_async/3, prepare_query_async/4, execute_query_async/4, batch_query_async/3, batch_query_async/4, subscribe_events_async/3]).

-include_lib("native_protocol.hrl").


-record(state, {connection :: connection(), id :: 1..127, caller :: {pid(), any()}, compression = none :: compression()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start(connection(), pos_integer(), compression()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start(Connection, StreamId, Compression) ->
	proc_lib:start(?MODULE, init, [Connection, StreamId, Compression]).

-spec options(Stream :: stream(), Timeout :: timeout()) ->  timeout | error() | options().
options(Stream, Timeout) ->
  call(Stream, options, Timeout).

-spec options_async(Stream :: stream(), ReplyTo :: async_target()) ->  ok | {error, Reason :: term()}.
options_async(Stream, ReplyTo) ->
	cast(Stream, options, ReplyTo).

-spec query(Stream :: stream(), Query :: string(), Params :: query_params(), Timeout :: timeout()) -> timeout | ok | error() | result_rows() | schema_change().
query(Stream, Query, Params, Timeout) ->
  call(Stream, {query, Query, Params}, Timeout).

-spec query_async(Stream :: stream(), Query :: string(), Params :: query_params(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
query_async(Stream, Query, Params, ReplyTo) ->
	cast(Stream, {query, Query, Params}, ReplyTo).

-spec query(Stream :: stream(), Query :: string(), Params :: query_params(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | ok | error() | result_rows() | schema_change().
query(Stream = #stream{connection = Con}, Query, Params, Timeout, UseCache) ->
  case UseCache of
    true ->
      case stmt_cache:cache(Query, Con, Timeout) of
        {ok, Id} -> call(Stream, {execute, Id, Params}, Timeout);
        Err = #error{} -> Err
      end;
    false ->
      call(Stream, {query, Query, Params}, Timeout)
  end.

-spec query_async(Stream :: stream(), Query :: string(), Params :: query_params(), ReplyTo :: async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
query_async(Stream = #stream{connection = Con}, Query, Params, ReplyTo, UseCache) ->
	case UseCache of
		true ->
			stmt_cache:cache_async(Query, Con, fun(Res) ->
	       case Res of
				  {ok, Id} -> cast(Stream, {execute, Id, Params}, ReplyTo);
				  Err = #error{} -> Err
        end
			end);
		false ->
			cast(Stream, {query, Query, Params}, ReplyTo)
	end.


-spec prepare_query(Stream :: stream(), Query :: string(), Timeout :: timeout()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Stream, Query, Timeout) ->
  prepare_query(Stream, Query, Timeout, false).

-spec prepare_query_async(Stream :: stream(), Query :: string(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
prepare_query_async(Stream, Query, Timeout) ->
	prepare_query_async(Stream, Query, Timeout, false).

-spec prepare_query(Stream :: stream(), Query :: string(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | error() | {binary(), metadata(), metadata()}.
prepare_query(Stream, Query, Timeout, UseCache) ->
  R = call(Stream, {prepare, Query}, Timeout),
  case {UseCache, R} of
    {true, {Id, _, _}} ->
      to_cache(Stream, Query, Id),
      R;
    {false, _} ->
      R
  end.

-spec prepare_query_async(Stream :: stream(), Query :: string(), ReplyTo :: async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
prepare_query_async(Stream, Query, ReplyTo, UseCache) ->
	cast(Stream, {prepare, Query}, fun(R) ->
		case {UseCache, R} of
			{true, {Id, _, _}} ->
				to_cache(Stream, Query, Id),
				reply_if_needed(ReplyTo, R);
			{false, _} ->
				reply_if_needed(ReplyTo, R)
		end
	end).

-spec execute_query(Stream :: stream(), ID :: binary(), Params :: query_params(), Timeout :: timeout()) -> timeout | ok | error() | result_rows() | schema_change().
execute_query(Stream, ID, Params, Timeout) ->
  call(Stream, {execute, ID, Params}, Timeout).

-spec execute_query_async(Stream :: stream(), ID :: binary(), Params :: query_params(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
execute_query_async(Stream, ID, Params, ReplyTo) ->
	cast(Stream, {execute, ID, Params}, ReplyTo).


-spec batch_query(Stream :: stream(), Batch :: batch_query(), Timeout :: timeout()) -> timeout | ok | error().
batch_query(Stream, Batch, Timeout) ->
  call(Stream, {batch, Batch}, Timeout).

-spec batch_query_async(Stream :: stream(), Batch :: batch_query(), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
batch_query_async(Stream, Batch, ReplyTo) ->
	cast(Stream, {batch, Batch}, ReplyTo).

-spec batch_query(Stream :: stream(), Batch :: batch_query(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | ok | error().
batch_query(Stream = #stream{connection = Con}, Batch = #batch_query{queries = Queries}, Timeout, UseCache) ->
  case UseCache of
    true ->
      NBatch = Batch#batch_query{queries = lists:map(
        fun({Id, Args}) when is_binary(Id) ->
             {Id, Args};
          ({Query, Args}) when is_list(Query) ->
            case stmt_cache:cache(Query, Con, Timeout) of
              {ok, Id} ->
                {Id, Args};
              Err ->
                throw({caching_error, Err})
            end
        end, Queries)},
      call(Stream, {batch, NBatch}, Timeout);

    false ->
      call(Stream, {batch, Batch}, Timeout)
  end.

-spec batch_query_async(Stream :: stream(), Batch :: batch_query(), ReplyTo :: async_target(), UseCache :: boolean()) -> timeout | ok.
batch_query_async(Stream, Batch, ReplyTo, false) ->
	batch_query_async(Stream, Batch, ReplyTo);
batch_query_async(Stream = #stream{connection = Con}, Batch = #batch_query{queries = Queries}, ReplyTo, true) ->
	ToCache = lists:filter(fun({Q, _Args}) -> is_list(Q) end, Queries),
	stmt_cache:cache_async_multple(ToCache, Con, fun(Dict) ->
		Qs = lists:map(fun({Q, Arg}) ->
			if
				is_binary(Q) -> {Q, Arg};
				true -> {dict:fetch(Q, Dict), Arg}
			end
		end, Queries),
		cast(Stream, {batch, Batch#batch_query{queries = Qs}}, ReplyTo)
	end).

-spec subscribe_events(Stream :: stream(), EventTypes :: list(string() | atom()), Timeout :: timeout()) -> ok | timeout | error().
subscribe_events(Stream, EventTypes, Timeout) ->
  call(Stream, {register, EventTypes}, Timeout).

-spec subscribe_events_async(Stream :: stream(), EventTypes :: list(string() | atom()), ReplyTo :: async_target()) -> ok | {error, Reason :: term()}.
subscribe_events_async(Stream, EventTypes, ReplyTo) ->
	cast(Stream, {register, EventTypes}, ReplyTo).

from_cache(#stream{connection = #connection{host = Host, port = Port}}, Query) ->
  stmt_cache:from_cache({Host, Port, Query}).

to_cache(#stream{connection = #connection{host = Host, port = Port}}, Query, Id) ->
  stmt_cache:to_cache({Host, Port, Query}, Id).




%% -spec(init(Args :: term()) -> {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} | {stop, Reason :: term()} | ignore).
init(Connection, StreamId, Compression) ->
	proc_lib:init_ack({ok, self()}),
	process_flag(trap_exit, true),
  loop(#state{connection = Connection, id = StreamId, compression = Compression}).

loop(State) ->
	receive
		Msg ->
			{noreply, NewState} = handle_msg(Msg, State),
			erlang:yield(),
			loop(NewState)
	end.


handle_frame(Pid, Frame) ->
  Pid ! {handle_frame, Frame}.

reply_if_needed(Caller, Reply) ->
  case Caller of
    undefined ->
      ok;
    {Pid, _} when is_pid(Pid) -> %% call reply
      erlang:send(Pid, {reply, Caller, Reply}, [noconnect]);
		Pid when is_pid(Pid) -> %% cast reply
			erlang:send(Pid, Reply, [noconnect]);
		Fun when is_function(Fun, 1) ->  %% cast reply
			Fun(Reply);
		{M, F, A} ->  %% cast reply
			erlang:apply(M, F, A ++ [Reply])
  end.


handle_msg(Request, State = #state{caller = Caller, connection = #connection{pid = Connection}, compression = Compression, id = StreamId}) ->
	case Request of
		{call, From, Msg} ->
			case Msg of
				options ->
					Frame = #frame{header = #header{type = request, opcode = ?OPC_OPTIONS, stream = StreamId}, length = 0, body = <<>>},
					connection:send_frame(Connection, native_parser:encode_frame(Frame, Compression)),
					{noreply, State#state{caller = From}};

				{query, Query, Params} ->
					Body = native_parser:encode_query(Query, Params),
					Frame = #frame{header = #header{type = request, opcode = ?OPC_QUERY, stream = StreamId}, length = byte_size(Body), body = Body},
					connection:send_frame(Connection, native_parser:encode_frame(Frame, Compression)),
					{noreply, State#state{caller = From}};

				{prepare, Query} ->
					Body = native_parser:encode_long_string(Query),
					Frame = #frame{header = #header{type = request, opcode = ?OPC_PREPARE, stream = StreamId}, length = byte_size(Body), body = Body},
					connection:send_frame(Connection, native_parser:encode_frame(Frame, Compression)),
					{noreply, State#state{caller = From}};

				{execute, ID, Params} ->
					Body = native_parser:encode_query(ID, Params),
					Frame = #frame{header = #header{type = request, opcode = ?OPC_EXECUTE, stream = StreamId}, length = byte_size(Body), body = Body},
					connection:send_frame(Connection, native_parser:encode_frame(Frame, Compression)),
					{noreply, State#state{caller = From}};

				{batch, BatchQuery} ->
					Body = native_parser:encode_batch_query(BatchQuery),
					Frame = #frame{header = #header{type = request, opcode = ?OPC_BATCH, stream = StreamId}, length = byte_size(Body), body = Body},
					connection:send_frame(Connection, native_parser:encode_frame(Frame, Compression)),
					{noreply, State#state{caller = From}};

				{register, EventTypes} ->
					start_gen_event_if_required(),
					Body = native_parser:encode_event_types(EventTypes),
					Frame = #frame{header = #header{type = request, opcode = ?OPC_REGISTER, stream = StreamId}, length = byte_size(Body), body = Body},
					connection:send_frame(Connection, native_parser:encode_frame(Frame, Compression)),
					{noreply, State#state{caller = From}};

				_ ->
					error_logger:error_msg("Unknown request ~p~n", [Request]),
					reply_if_needed(From, unknown_request),
					{noreply, State#state{caller = undefined}}
			end;

		{handle_frame, Frame} ->
			OpCode = (Frame#frame.header)#header.opcode,
			case OpCode of
				?OPC_ERROR ->
					Error = native_parser:parse_error(Frame),
					error_logger:error_msg("CQL error ~p~n", [Error]),
					reply_if_needed(Caller, {error, Error}),
					{noreply, State#state{caller = undefined}};
				?OPC_READY ->
					reply_if_needed(Caller, ok),
					{noreply, State#state{caller = undefined}};
				?OPC_AUTHENTICATE ->
					throw({not_supported_option, authentificate}),
					{noreply, State};
				?OPC_SUPPORTED ->
					{Options, _} = native_parser:parse_string_multimap(Frame#frame.body),
					reply_if_needed(Caller, Options),
					{noreply, State#state{caller = undefined}};
				?OPC_RESULT ->
					Result = native_parser:parse_result(Frame),
					reply_if_needed(Caller, Result),
					{noreply, State#state{caller = undefined}};
				?OPC_EVENT ->
					Result = native_parser:parse_event(Frame),
					gen_event:notify(cassandra_events, Result),
					{noreply, State};
				_ ->
					error_logger:warning_msg("Unsupported OpCode: ~p~n", [OpCode]),
					{noreply, State#state{caller = undefined}}
			end
	end.


start_gen_event_if_required() ->
	case whereis(cassandra_events) of
		undefined ->
			gen_event:start_link(cassandra_events);
		_ ->
			ok
	end.


call(#stream{stream_pid = Pid}, Msg, Timeout) ->
  try erlang:monitor(process, Pid) of
    Mref ->
      Tag = {self(), Mref},
      erlang:send(Pid, {call, Tag, Msg}, [noconnect]),
      receive
        {reply, Tag, X} ->
          erlang:demonitor(Mref, [flush]),
          X
      after
        Timeout -> {error, timeout}
      end
  catch
    error: Reason ->
      {error, Reason}
  end.

cast(#stream{stream_pid = Pid}, Msg, AsyncTarget) ->
	try
		ok = erlang:send(Pid, {call, AsyncTarget, Msg}, [noconnect])
	catch
		error: Reason ->
			{error, Reason}
	end.