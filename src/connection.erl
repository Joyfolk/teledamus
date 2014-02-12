-module(connection).

-behaviour(gen_server).

-include_lib("native_protocol.hrl").

-type socket() :: gen_tcp:socket() | ssl:sslsocket().

%% API
-export([start_link/4]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, get_socket/1, from_cache/3, to_cache/4, query/5, prepare_query/4, batch_query/4,
         new_stream/2, release_stream/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_STREAM_ID, 0).

-record(state, {transport = gen_tcp :: tcp | ssl, socket :: socket(), buffer = <<>>:: binary(), caller :: pid(), compression = none :: none | lz4 | snappy,
                stms_cache :: dict(), streams :: dict()}).  %% stmts_cache::dict(binary(), list()), streams:: dict(pos_integer(), stream())


%%%===================================================================
%%% API
%%%===================================================================

new_stream({connection, Pid}, Timeout) ->
  gen_server:call(Pid, new_stream, Timeout).

release_stream(S = #stream{connection = {connection, Pid}}, Timeout) ->
  gen_server:call(Pid, {release_stream, S}, Timeout).

options({connection, Pid}, Timeout) ->
  gen_server:call(Pid, {options, ?DEFAULT_STREAM_ID}, Timeout).

query({connection, Pid}, Query, Params, Timeout) ->
  gen_server:call(Pid, {query, Query, Params, ?DEFAULT_STREAM_ID}, Timeout).

query(Con, Query, Params, Timeout, UseCache) ->
  case UseCache of
    true ->
      case from_cache(Con, Query, Timeout) of
        {ok, Id} ->
          execute_query(Con, Id, Params, Timeout);
        _ ->
          case prepare_query(Con, Query, Timeout) of
            {Id, _, _} ->
              to_cache(Con, Query, Id, Timeout),
              execute_query(Con, Id, Params, Timeout);
            Err ->
              Err
          end
      end;

    false ->
      query(Con, Query, Params, Timeout)
  end.


prepare_query(Con, Query, Timeout) ->
  prepare_query(Con, Query, Timeout, false).

prepare_query(Con = {connection, Pid}, Query, Timeout, UseCache) ->
  R = gen_server:call(Pid, {prepare, Query, ?DEFAULT_STREAM_ID}, Timeout),
  case {UseCache, R} of
    {true, {Id, _, _}} ->
      to_cache(Con, Query, Id, Timeout),
      R;
    {false, _} ->
      R
  end.

execute_query({connection, Pid}, ID, Params, Timeout) ->
  gen_server:call(Pid, {execute, ID, Params, ?DEFAULT_STREAM_ID}, Timeout).

batch_query({connection, Pid}, Batch, Timeout) ->
  gen_server:call(Pid, {batch, Batch, ?DEFAULT_STREAM_ID}, Timeout).

batch_query(Con = {connection, Pid}, Batch = #batch_query{queries = Queries}, Timeout, UseCache) ->
  case UseCache of
    true ->
      NBatch = Batch#batch_query{queries = lists:map(
        fun({Id, Args}) when is_binary(Id) ->
          {Id, Args};
          ({Query, Args}) when is_list(Query) ->
            {Id, _, _} = prepare_query(Con, Query, Timeout, true),
            {Id, Args}
        end, Queries)},
      gen_server:call(Pid, {batch, NBatch, ?DEFAULT_STREAM_ID}, Timeout);
    false ->
      gen_server:call(Pid, {batch, Batch, ?DEFAULT_STREAM_ID}, Timeout)
  end.

subscribe_events({connection, Pid}, EventTypes, Timeout) ->
  gen_server:call(Pid, {register, EventTypes, ?DEFAULT_STREAM_ID}, Timeout).

get_socket({connection, Pid}) ->
  gen_server:call(Pid, get_socket).

from_cache({connection, Pid}, Query, Timeout) ->
  gen_server:call(Pid, {from_cache, Query}, Timeout).

to_cache({connection, Pid}, Query, Id, Timeout) ->
  gen_server:call(Pid, {to_cache, Query, Id}, Timeout).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Socket, Credentials, Transport, Compression) ->
  gen_server:start_link(?MODULE, [Socket, Credentials, Transport, Compression], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Socket, Credentials, Transport, Compression]) ->
  case startup(Socket, Credentials, Transport, Compression) of
    ok ->
      {ok, #state{socket = Socket, transport = Transport, compression = Compression, stms_cache = dict:new(), streams = dict:new()}};
    {error, Reason} ->
      {stop, Reason};
    R ->
      {stop, {unknown_error, R}}
  end.

update_state(StreamId, From, State) ->
  if
    is_integer(StreamId) andalso StreamId > 0 ->
      {noreply, State};
    true ->
      {noreply, State#state{caller = From}}
  end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) -> {reply, Reply, State} | {reply, Reply, State, Timeout} | {noreply, State} | {noreply, State, Timeout} | {stop, Reason, Reply, State} | {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, From, State = #state{socket = Socket, transport = Transport, compression = Compression, streams = Streams}) ->
  case Request of
    new_stream ->
      case find_next_stream_id(Streams) of
        no_streams_available ->
          {reply, {error, no_streams_available}, State};
        StreamId ->
          case stream:start_link({connection, self()}, StreamId) of
            {ok, StreamPid} ->
              Stream = #stream{connection = {connection, self()}, stream_pid = StreamPid, stream_id = StreamId},
              {reply, Stream, State#state{streams = dict:append(StreamId, Stream, Streams)}};
            {error, X} ->
              {reply, {error, X}, State}
          end
      end;

    {release_stream, #stream{stream_id = Id}} ->
      %% todo: close something?
      {reply, ok, State#state{streams = dict:erase(Id, Streams)}};

    {from_cache, Query} ->
      {reply, dict:find(Query, State#state.stms_cache), State};

    {to_cache, Query, Id} ->
      {reply, ok, State#state{stms_cache = dict:store(Query, Id, State#state.stms_cache)}};

    get_socket ->
      {reply, State#state.socket, Socket};

    _ ->
      handle_request(Request, From, State)
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(Request, State) ->
  handle_request(Request, undefined, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({tcp, Socket, Data}, #state{socket = Socket, transport = Transport, buffer = Buffer, compression = Compression} = State) ->
  case native_parser:parse_frame(<<Buffer/binary, Data/binary>>, Compression) of
    {undefined, NewBuffer} ->
      set_active(Socket, Transport),
      {noreply, State#state{buffer = NewBuffer}};
    {Frame, NewBuffer} ->
      handle_frame(Frame, State#state{buffer = NewBuffer})
  end;

handle_info({ssl, Socket, Data}, #state{socket = Socket, buffer = Buffer, compression = Compression} = State) ->
  case native_parser:parse_frame(<<Buffer/binary, Data/binary>>, Compression) of
    {undefined, NewBuffer} ->
      {noreply, State#state{buffer = NewBuffer}};
    {Frame, NewBuffer} ->
      handle_frame(Frame, State#state{buffer = NewBuffer})
  end;

handle_info({tcp_closed, _Socket}, State) ->
  error_logger:error_msg("TCP connection closed~n"),
  {stop, tcp_closed, State};

handle_info({ssl_closed, _Socket}, State) ->
  error_logger:error_msg("SSL connection closed~n"),
  {stop, ssl_closed, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
  error_logger:error_msg("TCP error [~p]~n", [Reason]),
  {stop, {tcp_error, Reason}, State};

handle_info({ssl_error, _Socket, Reason}, State) ->
  error_logger:error_msg("SSL error [~p]~n", [Reason]),
  {stop, {tcp_error, Reason}, State};

handle_info(Info, State) ->
  error_logger:warning_msg("Unhandled info: ~p~n", [Info]),
  {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{socket = Socket, transport = Transport}) ->
  Transport:close(Socket).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.





%%%===================================================================
%%% Internal functions
%%%===================================================================


send(Socket, Transport, Compression, Frame) ->
  F = native_parser:encode_frame(Frame, Compression),
  Transport:send(Socket, F).


set_active(Socket, Transport) ->
  case Transport of
    gen_tcp -> inet:setopts(Socket, [{active, once}]);
    Transport -> Transport:setopts(Socket, [{active, once}])
  end.


handle_frame(Frame = #frame{header = #header{opcode = OpCode}}, State = #state{caller = Caller, transport = Transport, streams = Streams}) ->
  set_active(State#state.socket, Transport),
  StreamId = (Frame#frame.header)#header.stream,
  UseStream = if StreamId > 0 andalso StreamId < 128 -> dict:is_key(StreamId, Streams); true -> false end,
  if
    UseStream ->
      [Stream] = dict:fetch(StreamId, Streams),
      stream:handle_frame(Stream#stream.stream_pid, Frame),
      {noreply, State};

    true ->
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


reply_if_needed(Caller, Reply) ->
  case Caller of
    undefined ->
      ok;
    _ ->
      gen_server:reply(Caller, Reply)
  end.


startup_opts(Compression) ->
  CQL = [{<<"CQL_VERSION">>,  ?CQL_VERSION}],
  case Compression of
    none ->
      CQL;
    {Name, _, _} ->
      N = list_to_binary(Name),
      [{<<"COMPRESSION", N/binary>>} | CQL];
    _ ->
      throw({unsupported_compression_type, Compression})
  end.


startup(Socket, Credentials, Transport, Compression) ->
  case send(Socket, Transport, Compression, #frame{header = #header{opcode = ?OPC_STARTUP, type = request}, body = native_parser:encode_string_map(startup_opts(Compression))}) of
    ok ->
      {Frame, _R} = read_frame(Socket, Transport, Compression),
      case (Frame#frame.header)#header.opcode of
        ?OPC_ERROR ->
          Error = native_parser:parse_error(Frame),
          error_logger:error_msg("CQL error ~p~n", [Error]),
          {error, Error};
        ?OPC_AUTHENTICATE ->
          {Authenticator, _Rest} = native_parser:parse_string(Frame#frame.body),
          authenticate(Socket, Authenticator, Credentials, Transport, Compression);
        ?OPC_READY ->
          ok;
        _ ->
          {error, unknown_response_code}
      end;
    Error ->
      {error, Error}
  end.

read_frame(Socket, Transport, Compression) ->
  {ok, Data} = Transport:recv(Socket, 8),
  <<_Header:4/binary, Length:32/big-unsigned-integer>> = Data,
  {ok, Body} = if
                 Length =/= 0 ->
                   Transport:recv(Socket, Length);
                 true ->
                   {ok, <<>>}
               end,
  F = <<Data/binary,Body/binary>>,
  native_parser:parse_frame(F, Compression).


encode_plain_credentials(User, Password) when is_list(User) ->
  encode_plain_credentials(list_to_binary(User), Password);
encode_plain_credentials(User, Password) when is_list(Password) ->
  encode_plain_credentials(User, list_to_binary(Password));
encode_plain_credentials(User, Password) when is_binary(User), is_binary(Password)->
  native_parser:encode_bytes(<<0,User/binary,0,Password/binary>>).


authenticate(Socket, Authenticator, Credentials, Transport, Compression) ->
  %% todo: pluggable authentification
  {User, Password} = Credentials,
  case send(Socket, Transport, Compression, #frame{header = #header{opcode = ?OPC_AUTH_RESPONSE, type = request}, body = encode_plain_credentials(User, Password)}) of
    ok ->
      {Frame, _R} = read_frame(Socket, Transport, Compression),
      case (Frame#frame.header)#header.opcode of
        ?OPC_ERROR ->
          Error = native_parser:parse_error(Frame),
          error_logger:error_msg("Authentication error [~p]: ~p~n", [Authenticator, Error]),
          {error, Error};
        ?OPC_AUTH_CHALLENGE ->
          error_logger:error_msg("Unsupported authentication message~n"),
          {error, authentification_error};
        ?OPC_AUTH_SUCCESS ->
          ok;
        _ ->
          {error, unknown_response_code}
      end;
    Error ->
      {error, Error}
  end.


start_gen_event_if_required() ->
  case whereis(cassandra_events) of
    undefined ->
      gen_event:start_link(cassandra_events);
    _ ->
      ok
  end.


find_next_stream_id(Streams) ->
  find_next_stream_id(1, Streams).

find_next_stream_id(128, _Streams) ->
  no_streams_available;
find_next_stream_id(Id, Streams) ->
  case dict:is_key(Id, Streams) of
    false ->
      Id;
    true ->
      find_next_stream_id(Id + 1, Streams)
  end.


handle_request(Request, From, State = #state{socket = Socket, transport = Transport, compression = Compression}) ->
  case Request of
    {options, StreamId} ->
      send(Socket, Transport, Compression, #frame{header = #header{type = request, opcode = ?OPC_OPTIONS}, length = 0, body = <<>>}),
      set_active(Socket, Transport),
      update_state(StreamId, From, State);

    {query, Query, Params, StreamId} ->
      Body = native_parser:encode_query(Query, Params),
      send(Socket, Transport, Compression, #frame{header = #header{type = request, opcode = ?OPC_QUERY}, length = byte_size(Body), body = Body}),
      set_active(Socket, Transport),
      update_state(StreamId, From, State);

    {prepare, Query, StreamId} ->
      Body = native_parser:encode_long_string(Query),
      send(Socket, Transport, Compression, #frame{header = #header{type = request, opcode = ?OPC_PREPARE}, length = byte_size(Body), body = Body}),
      set_active(Socket, Transport),
      update_state(StreamId, From, State);

    {execute, ID, Params, StreamId} ->
      Body = native_parser:encode_query(ID, Params),
      send(Socket, Transport, Compression, #frame{header = #header{type = request, opcode = ?OPC_EXECUTE}, length = byte_size(Body), body = Body}),
      set_active(Socket, Transport),
      update_state(StreamId, From, State);

    {batch, BatchQuery, StreamId} ->
      Body = native_parser:encode_batch_query(BatchQuery),
      send(Socket, Transport, Compression, #frame{header = #header{type = request, opcode = ?OPC_BATCH}, length = byte_size(Body), body = Body}),
      set_active(Socket, Transport),
      update_state(StreamId, From, State);

    {register, EventTypes, StreamId} ->
      start_gen_event_if_required(),
      Body = native_parser:encode_event_types(EventTypes),
      send(Socket, Transport, Compression, #frame{header = #header{type = request, opcode = ?OPC_REGISTER}, length = byte_size(Body), body = Body}),
      set_active(Socket, Transport),
      update_state(StreamId, From, State);

      _ ->
       error_logger:error_msg("Unknown request ~p~n", [Request]),
       {noereply, State#state{caller = undefined}}
end.