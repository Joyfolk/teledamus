-module(stream).

-behaviour(gen_server).

%% API
-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, from_cache/3, to_cache/4, query/5, prepare_query/4, batch_query/4, handle_frame/2]).

-define(SERVER, ?MODULE).

-include_lib("native_protocol.hrl").

-record(state, {connection :: connection(), id :: 1..127, caller :: pid()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link(connection(), pos_integer()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Connection, StreamId) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Connection, StreamId], []).

options(#stream{stream_pid = Pid}, Timeout) ->
  gen_server:call(Pid, {options, Timeout}, Timeout).

query(#stream{stream_pid = Pid}, Query, Params, Timeout) ->
  gen_server:call(Pid, {query, Query, Params, Timeout}, Timeout).

query(#stream{stream_pid = Pid}, Query, Params, Timeout, UseCache) ->
  case UseCache of
    true ->
      case from_cache(Pid, Query, Timeout) of
        {ok, Id} ->
          execute_query(Pid, Id, Params, Timeout);
        _ ->
          case prepare_query(Pid, Query, Timeout) of
            {Id, _, _} ->
              to_cache(Pid, Query, Id, Timeout),
              execute_query(Pid, Id, Params, Timeout);
            Err ->
              Err
          end
      end;

    false ->
      query(Pid, Query, Params, Timeout)
  end.


prepare_query(#stream{stream_pid = Pid}, Query, Timeout) ->
  prepare_query(#stream{stream_pid = Pid}, Query, Timeout, false).

prepare_query(Stream = #stream{stream_pid = Pid}, Query, Timeout, UseCache) ->
  R = gen_server:call(Pid, {prepare, Query, Timeout}, Timeout),
  case {UseCache, R} of
    {true, {Id, _, _}} ->
      to_cache(Stream, Query, Id, Timeout),
      R;
    {false, _} ->
      R
  end.

execute_query(#stream{stream_pid = Pid}, ID, Params, Timeout) ->
  gen_server:call(Pid, {execute, ID, Params, Timeout}, Timeout).

batch_query(#stream{stream_pid = Pid}, Batch, Timeout) ->
  gen_server:call(Pid, {batch, Batch, Timeout}, Timeout).

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
      gen_server:call(Pid, {batch, NBatch, Timeout}, Timeout);

    false ->
      gen_server:call(Pid, {batch, Batch, Timeout}, Timeout)
  end.

subscribe_events(#stream{stream_pid = Pid}, EventTypes, Timeout) ->
  gen_server:call(Pid, {register, EventTypes, Timeout}, Timeout).

from_cache(#stream{stream_pid = Pid}, Query, Timeout) ->
  gen_server:call(Pid, {from_cache, Query, Timeout}, Timeout).

to_cache(#stream{stream_pid = Pid}, Query, Id, Timeout) ->
  gen_server:call(Pid, {to_cache, Query, Id, Timeout}, Timeout).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) -> {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} | {stop, Reason :: term()} | ignore).
init([Connection, StreamId]) ->
  {ok, #state{connection = Connection, id = StreamId}}.


-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}} | {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
            {noreply, NewState :: #state{}} | {noreply, NewState :: #state{}, timeout() | hibernate} | {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} | {stop, Reason :: term(), NewState :: #state{}}).
handle_call(Request, From, State = #state{id = StreamId, connection = {connection, Connection}}) ->
  case Request of
    {options, _Timeout} ->
      gen_server:cast(Connection, {options, StreamId}),
      {noreply, State#state{caller = From}};

    {query, Query, Params, _Timeout} ->
      gen_server:cast(Connection, {query, Query, Params, StreamId}),
      {noreply, State#state{caller = From}};

    {prepare, Query, _Timeout} ->
      gen_server:cast(Connection, {prepare, Query, StreamId}),
      {noreply, State#state{caller = From}};

    {execute, ID, Params, _Timeout} ->
      gen_server:cast(Connection, {execute, ID, Params, StreamId}),
      {noreply, State#state{caller = From}};

    {batch, BatchQuery, _Timeout} ->
      gen_server:cast(Connection, {batch,  BatchQuery, StreamId}),
      {noreply, State#state{caller = From}};

    {register, EventTypes, _Timeout} ->
      gen_server:cast(Connection, {register, EventTypes, StreamId}),
      {noreply, State#state{caller = From}};

    {from_cache, Query, Timeout} ->
      R = gen_server:call(Connection, {from_cache, Query, StreamId}, Timeout),
      {reply, R, State};

    {to_cache, Query, Id, Timeout} ->
      R = gen_server:call(Connection, {to_cache, Query, Id, StreamId}, Timeout),
      {reply, R, State};

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
terminate(Reason, #state{caller = Id, connection = Pid}) ->
  error_logger:error_msg("Stopping stream ~p because of ~p~n", [Id, Reason]),
  connection:release_stream(#stream{connection = Pid, stream_id = Id, stream_pid = self()}, 1000),
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{}, Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

