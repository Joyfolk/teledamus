-module(stream).

-behaviour(gen_server).

%% API
-export([start/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, from_cache/2, to_cache/3, query/5, prepare_query/4, batch_query/4, handle_frame/2]).


-include_lib("native_protocol.hrl").


-record(state, {connection :: connection(), id :: 1..127, caller :: pid(), compression = none :: compression()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start(connection(), pos_integer(), compression()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start(Connection, StreamId, Compression) ->
  gen_server:start(?MODULE, [Connection, StreamId, Compression], []).

options(#stream{stream_pid = Pid}, Timeout) ->
  gen_server:call(Pid, options, Timeout).

query(#stream{stream_pid = Pid}, Query, Params, Timeout) ->
  gen_server:call(Pid, {query, Query, Params}, Timeout).

query(#stream{stream_pid = Pid, connection = Con}, Query, Params, Timeout, UseCache) ->
  case UseCache of
    true ->
      case stmt_cache:cache(Query, Con, Timeout) of
        {ok, Id} -> gen_server:call(Pid, {execute, Id, Params}, Timeout);
        Err -> Err
      end;
    false ->
      gen_server:call(Pid, {query, Query, Params}, Timeout)
  end.


prepare_query(#stream{stream_pid = Pid}, Query, Timeout) ->
  prepare_query(#stream{stream_pid = Pid}, Query, Timeout, false).

prepare_query(Stream = #stream{stream_pid = Pid}, Query, Timeout, UseCache) ->
  R = gen_server:call(Pid, {prepare, Query}, Timeout),
  case {UseCache, R} of
    {true, {Id, _, _}} ->
      to_cache(Stream, Query, Id),
      R;
    {false, _} ->
      R
  end.

execute_query(#stream{stream_pid = Pid}, ID, Params, Timeout) ->
  gen_server:call(Pid, {execute, ID, Params}, Timeout).

batch_query(#stream{stream_pid = Pid}, Batch, Timeout) ->
  gen_server:call(Pid, {batch, Batch}, Timeout).

batch_query(Stream = #stream{stream_pid = Pid}, Batch = #batch_query{queries = Queries}, Timeout, UseCache) ->
  case UseCache of
    true ->
      NBatch = Batch#batch_query{queries = lists:map(
        fun({Id, Args}) when is_binary(Id) ->
          {Id, Args};
          ({Query, Args}) when is_list(Query) ->
            {Id, _, _} = prepare_query(Stream, Query, Timeout, true),
            {Id, Args}
        end, Queries)},
      gen_server:call(Pid, {batch, NBatch}, Timeout);

    false ->
      gen_server:call(Pid, {batch, Batch}, Timeout)
  end.

subscribe_events(#stream{stream_pid = Pid}, EventTypes, Timeout) ->
  gen_server:call(Pid, {register, EventTypes}, Timeout).

from_cache(#stream{connection = #connection{host = Host, port = Port}}, Query) ->
  stmt_cache:from_cache({Host, Port, Query}).

to_cache(#stream{connection = #connection{host = Host, port = Port}}, Query, Id) ->
  stmt_cache:to_cache({Host, Port, Query}, Id).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) -> {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} | {stop, Reason :: term()} | ignore).
init([Connection, StreamId, Compression]) ->
	process_flag(trap_exit, true),
  {ok, #state{connection = Connection, id = StreamId, compression = Compression}}.


-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} | {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
            {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} | {stop, Reason :: term(), NewState :: #state{}}).
handle_call(Request, From, State = #state{id = StreamId, connection = #connection{pid = Connection}, compression = Compression}) ->
  case Request of
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
      {reply, unknown_request, State}
  end.


handle_frame(Pid, Frame) ->
  Pid ! {handle_frame, Frame}.

reply_if_needed(Caller, Reply) ->
  case Caller of
    undefined ->
      ok;
    _ ->
      gen_server:reply(Caller, Reply)
  end.


-spec(handle_cast(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(Request, State = #state{caller = Caller}) ->
  case Request of
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

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) -> {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), NewState :: #state{}}).
handle_info(Request, State = #state{caller = Caller}) ->
  case Request of
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

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: #state{}) -> term()).
terminate(_Reason, #state{caller = Id, connection = #connection{pid = Pid}}) ->
	case is_process_alive(Pid) of
		true ->	connection:release_stream(#stream{connection = #connection{pid = Pid}, stream_id = Id, stream_pid = self()}, 1000), ok;
		_ -> ok
	end.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{}, Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


start_gen_event_if_required() ->
	case whereis(cassandra_events) of
		undefined ->
			gen_event:start_link(cassandra_events);
		_ ->
			ok
	end.