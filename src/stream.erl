-module(stream).

%% -behaviour(gen_server).

%% API
-export([start/3]).

%% -export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, from_cache/2, to_cache/3, query/5, prepare_query/4, batch_query/4, handle_frame/2, init/3]).


-include_lib("native_protocol.hrl").


-record(state, {connection :: connection(), id :: 1..127, caller :: pid(), compression = none :: compression()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start(connection(), pos_integer(), compression()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start(Connection, StreamId, Compression) ->
	proc_lib:start(?MODULE, init, [Connection, StreamId, Compression]).

options(Stream, Timeout) ->
  call(Stream, options, Timeout).

query(Stream, Query, Params, Timeout) ->
  call(Stream, {query, Query, Params}, Timeout).

query(Stream = #stream{connection = Con}, Query, Params, Timeout, UseCache) ->
  case UseCache of
    true ->
      case stmt_cache:cache(Query, Con, Timeout) of
        {ok, Id} -> call(Stream, {execute, Id, Params}, Timeout);
        Err -> Err
      end;
    false ->
      call(Stream, {query, Query, Params}, Timeout)
  end.


prepare_query(Stream, Query, Timeout) ->
  prepare_query(Stream, Query, Timeout, false).

prepare_query(Stream, Query, Timeout, UseCache) ->
  R = call(Stream, {prepare, Query}, Timeout),
  case {UseCache, R} of
    {true, {Id, _, _}} ->
      to_cache(Stream, Query, Id),
      R;
    {false, _} ->
      R
  end.

execute_query(Stream, ID, Params, Timeout) ->
  call(Stream, {execute, ID, Params}, Timeout).

batch_query(Stream, Batch, Timeout) ->
  call(Stream, {batch, Batch}, Timeout).

batch_query(Stream, Batch = #batch_query{queries = Queries}, Timeout, UseCache) ->
  case UseCache of
    true ->
      NBatch = Batch#batch_query{queries = lists:map(
        fun({Id, Args}) when is_binary(Id) ->
          {Id, Args};
          ({Query, Args}) when is_list(Query) ->
            {Id, _, _} = prepare_query(Stream, Query, Timeout, true),
            {Id, Args}
        end, Queries)},
      call(Stream, {batch, NBatch}, Timeout);

    false ->
      call(Stream, {batch, Batch}, Timeout)
  end.

subscribe_events(Stream, EventTypes, Timeout) ->
  call(Stream, {register, EventTypes}, Timeout).

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


%% -spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} | {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
%%             {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} | {stop, Reason :: term(), NewState :: #state{}}).
%% handle_call(Request, From, State) ->
%% 	handle_msg({call, From, Request}, State).  %% todo: remove


handle_frame(Pid, Frame) ->
  Pid ! {handle_frame, Frame}.

reply_if_needed(Caller, Reply) ->
  case Caller of
    undefined ->
      ok;
    _ ->
      Caller ! {reply, Reply}
  end.


%% -spec(handle_cast(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), NewState :: #state{}}).
%% handle_cast(Request, State) ->
%%   handle_msg(Request, State).
%%
%% -spec(handle_info(Info :: timeout() | term(), State :: #state{}) -> {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), NewState :: #state{}}).
%% handle_info(Request, State) ->
%%   handle_msg(Request, State).


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
					reply_if_needed(Caller, Error),
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
  Pid ! {call, self(), Msg},
  receive
    {reply, X} -> X
  after
    Timeout -> {error, timeout}
  end.