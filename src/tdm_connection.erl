-module(tdm_connection).

-behaviour(gen_server).

-include_lib("teledamus.hrl").


%% API
-export([start/6, start/7, prepare_ets/0]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, get_socket/1, from_cache/2, to_cache/3, query/5, prepare_query/4, batch_query/4,
    new_stream/2, release_stream/2, release_stream_async/1, send_frame/2, get_default_stream/1]).

-export([options_async/2, query_async/4, query_async/5, prepare_query_async/3, prepare_query_async/4, execute_query_async/4, batch_query_async/3, batch_query_async/4, subscribe_events_async/3, close/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_STREAM_ID, 0).
-define(DEF_STREAM_ETS, default_streams).


-record(state, {transport = gen_tcp :: teledamus:transport(), socket :: teledamus:socket(), buffer = <<>>:: binary(), caller :: pid(), compression = none :: teledamus:compression(),
    streams :: dict(), host :: list(), port :: pos_integer(), monitor_ref :: reference()}).


%%%===================================================================
%%% API
%%%===================================================================

-spec send_frame(pid(), binary()) -> ok | {error, Reason :: term()}.
send_frame(Pid, Frame) ->
    gen_server:cast(Pid, {send_frame, Frame}).

-spec new_stream(teledamus:connection(), timeout()) -> teledamus:stream() | teledamus:error().
new_stream(#tdm_connection{pid = Pid}, Timeout) ->
    gen_server:call(Pid, new_stream, Timeout).

-spec release_stream(teledamus:stream(), timeout()) -> ok | teledamus:error().
release_stream(S = #tdm_stream{connection = #tdm_connection{pid = Pid}}, Timeout) ->
    tdm_stream:stop(S),
    gen_server:call(Pid, {release_stream, S}, Timeout).

-spec release_stream_async(teledamus:stream()) -> ok | teledamus:error().
release_stream_async(S = #tdm_stream{connection = #tdm_connection{pid = Pid}}) ->
    tdm_stream:stop(S),
    gen_server:cast(Pid, {release_stream, S}).

-spec options(Connection :: teledamus:connection(), Timeout :: timeout()) ->  timeout | teledamus:error() | teledamus:options().
options(#tdm_connection{default_stream = Stream}, Timeout) ->
    tdm_stream:options(Stream, Timeout).

-spec options_async(Connection :: teledamus:connection(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
options_async(#tdm_connection{default_stream = Stream}, Timeout) ->
    tdm_stream:options_async(Stream, Timeout).

-spec query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), Timeout :: timeout()) -> timeout | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
query(#tdm_connection{default_stream = Stream}, Query, Params, Timeout) ->
    tdm_stream:query(Stream, Query, Params, Timeout).

-spec query_async(Con :: teledamus:connection(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
query_async(#tdm_connection{default_stream = Stream}, Query, Params, ReplyTo) ->
    tdm_stream:query_async(Stream, Query, Params, ReplyTo).

-spec query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
query(#tdm_connection{default_stream = Stream}, Query, Params, Timeout, UseCache) ->
    tdm_stream:query(Stream, Query, Params, Timeout, UseCache).

-spec query_async(Con :: teledamus:connection(), Query :: string(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
query_async(#tdm_connection{default_stream = Stream}, Query, Params, ReplyTo, UseCache) ->
    tdm_stream:query_async(Stream, Query, Params, ReplyTo, UseCache).

-spec prepare_query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Timeout :: timeout()) -> timeout | teledamus:error() | {binary(), teledamus:metadata(), teledamus:metadata()}.
prepare_query(#tdm_connection{default_stream = Stream}, Query, Timeout) ->
    tdm_stream:prepare_query(Stream, Query, Timeout).

-spec prepare_query_async(Con :: teledamus:connection(), Query :: teledamus:query_text(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
prepare_query_async(#tdm_connection{default_stream = Stream}, Query, ReplyTo) ->
    tdm_stream:prepare_query_async(Stream, Query, ReplyTo).

-spec prepare_query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | teledamus:error() | {binary(), teledamus:metadata(), teledamus:metadata()}.
prepare_query(#tdm_connection{default_stream = Stream}, Query, Timeout, UseCache) ->
    tdm_stream:prepare_query(Stream, Query, Timeout, UseCache).

-spec prepare_query_async(Con :: teledamus:connection(), Query :: teledamus:query_text(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
prepare_query_async(#tdm_connection{default_stream = Stream}, Query, ReplyTo, UseCache) ->
    tdm_stream:prepare_query_async(Stream, Query, ReplyTo, UseCache).

-spec execute_query(Con :: teledamus:connection(), ID :: teledamus:prepared_query_id(), Params :: teledamus:query_params(), Timeout :: timeout()) -> timeout | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
execute_query(#tdm_connection{default_stream = Stream}, ID, Params, Timeout) ->
    tdm_stream:execute_query(Stream, ID, Params, Timeout).

-spec execute_query_async(Con :: teledamus:connection(), ID :: teledamus:prepared_query_id(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
execute_query_async(#tdm_connection{default_stream = Stream}, ID, Params, ReplyTo) ->
    tdm_stream:execute_query_async(Stream, ID, Params, ReplyTo).

-spec batch_query(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), Timeout :: timeout()) -> timeout | ok | teledamus:error().
batch_query(#tdm_connection{default_stream = Stream}, Batch, Timeout) ->
    tdm_stream:batch_query(Stream, Batch, Timeout).

-spec batch_query_async(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
batch_query_async(#tdm_connection{default_stream = Stream}, Batch, ReplyTo) ->
    tdm_stream:batch_query_async(Stream, Batch, ReplyTo).

-spec batch_query(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | ok | teledamus:error().
batch_query(#tdm_connection{default_stream = Stream}, Batch, Timeout, UseCache) ->
    tdm_stream:batch_query(Stream, Batch, Timeout, UseCache).

-spec batch_query_async(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
batch_query_async(#tdm_connection{default_stream = Stream}, Batch, ReplyTo, UseCache) ->
    tdm_stream:batch_query_async(Stream, Batch, ReplyTo, UseCache).

-spec subscribe_events(Con :: teledamus:connection(), EventTypes :: list(string() | atom()), Timeout :: timeout()) -> ok | timeout | teledamus:error().
subscribe_events(#tdm_connection{default_stream = Stream}, EventTypes, Timeout) ->
    tdm_stream:subscribe_events(Stream, EventTypes, Timeout).

-spec subscribe_events_async(Con :: teledamus:connection(), EventTypes :: [string() | atom()], ReplyTo :: teledamus:async_target()) -> ok | timeout | {error, Reason :: term()}.
subscribe_events_async(#tdm_connection{default_stream = Stream}, EventTypes, ReplyTo) ->
    tdm_stream:subscribe_events_async(Stream, EventTypes, ReplyTo).

-spec get_socket(Con :: teledamus:connection()) -> teledamus:socket() | {error, Reason :: term()}.
get_socket(#tdm_connection{pid = Pid})->
    gen_server:call(Pid, get_socket).

-spec from_cache(Con :: teledamus:connection(), Query :: string()) -> {ok, binary()} | not_found.
from_cache(#tdm_connection{host = Host, port = Port}, Query) ->
    tdm_stmt_cache:from_cache({Host, Port, Query}).

-spec to_cache(Con :: teledamus:connection(), Query :: teledamus:query_text(), PreparedStmtId :: binary()) -> true.
to_cache(#tdm_connection{host = Host, port = Port}, Query, PreparedStmtId) ->
    tdm_stmt_cache:to_cache({Host, Port, Query}, PreparedStmtId).

-spec prepare_ets() -> default_streams.
prepare_ets() ->
    case ets:info(?DEF_STREAM_ETS) of
        undefined -> ets:new(?DEF_STREAM_ETS, [named_table, set, public, {write_concurrency, false}, {read_concurrency, true}]);
        _ -> ?DEF_STREAM_ETS
    end.

-spec close(Con :: teledamus:connection(), Timeout :: timeout()) -> ok.
close(#tdm_connection{pid = Pid}, Timeout) ->
    gen_server:call(Pid, {stop, Timeout}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(Socket, Credentials, Transport, Compression, Host, Port) ->
    gen_server:start(?MODULE, [Socket, Credentials, Transport, Compression, Host, Port, undefined], []).

start(Socket, Credentials, Transport, Compression, Host, Port, ChannelMonitor) ->
    gen_server:start(?MODULE, [Socket, Credentials, Transport, Compression, Host, Port, ChannelMonitor], []).

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
init([Socket, Credentials, Transport, Compression, Host, Port, ChannelMonitor]) ->
    RR = case startup(Socket, Credentials, Transport, Compression) of
        ok ->
            set_active(Socket, Transport),
            Connection = #tdm_connection{pid = self(), host = Host, port = Port},
            {ok, StreamId} = tdm_stream:start(Connection, ?DEFAULT_STREAM_ID, Compression, ChannelMonitor),
            MonitorRef = monitor(process, StreamId),
            DefStream = #tdm_stream{connection = Connection, stream_id = ?DEFAULT_STREAM_ID, stream_pid = StreamId},
            DefStream2 = DefStream#tdm_stream{connection = Connection#tdm_connection{default_stream = DefStream}},
            ets:insert(?DEF_STREAM_ETS, {self(), DefStream2}),
            {ok, #state{socket = Socket, transport = Transport, compression = Compression, streams = dict:store(?DEFAULT_STREAM_ID, DefStream2, dict:new()), monitor_ref = MonitorRef}};
        {error, Reason} ->
            {stop, Reason}
    end,
    RR.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) -> {reply, Reply, State} | {reply, Reply, State, Timeout} | {noreply, State} | {noreply, State, Timeout} | {stop, Reason, Reply, State} | {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State = #state{socket = Socket, transport = Transport, compression = Compression, streams = Streams, host = Host, port = Port, monitor_ref = MonitorRef}) ->
    case Request of
        {get_stream, Id} ->
            case dict:find(Id, Streams) of
                {ok, Stream} ->
                    {reply, Stream, State};
                _ ->
                    {reply, {error, {stream_not_found, Id}}, State}
            end;

        new_stream ->
            case find_next_stream_id(Streams) of
                no_streams_available ->
                    {reply, {error, no_streams_available}, State};
                StreamId ->
                    DefStream = dict:fetch(?DEFAULT_STREAM_ID, Streams),
                    Connection = #tdm_connection{pid = self(), host = Host, port = Port, default_stream = DefStream},
                    case tdm_stream:start(Connection, StreamId, Compression) of
                        {ok, StreamPid} ->
                            Stream = #tdm_stream{connection = Connection, stream_pid = StreamPid, stream_id = StreamId},
                            {reply, Stream, State#state{streams = dict:store(StreamId, Stream, Streams)}};
                        {error, X} ->
                            {reply, {error, X}, State}
                    end
            end;

        {release_stream, #tdm_stream{stream_id = Id}} ->
            {reply, ok, State#state{streams = dict:erase(Id, Streams)}};

        get_socket ->
            {reply, State#state.socket, Socket};

        {stop, Timeout} ->
            demonitor(MonitorRef, [flush]),
            DefStream = dict:fetch(?DEFAULT_STREAM_ID, Streams),
            tdm_stream:close(DefStream, Timeout),
            Transport:close(Socket),
            {stop, normal, ok, State};

        _ ->
            error_logger:error_msg("Unknown request ~p~n", [Request]),
            {reply, {error, unknown_request}, State#state{caller = undefined}}
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
handle_cast(Request, State = #state{transport = Transport, socket = Socket, streams = Streams}) ->
    case Request of
        {send_frame, Frame} ->
            case Transport of
                tcp ->
                    erlang:port_command(Socket, Frame);
                _ ->
                    Transport:send(Socket, Frame)
            end,
            {noreply, State};

        {release_stream, #tdm_stream{stream_id = Id}} ->
            {reply, ok, State#state{streams = dict:erase(Id, Streams)}};

        _ ->
            error_logger:error_msg("Unknown request ~p~n", [Request]),
            {noreply, State#state{caller = undefined}}
    end.


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
handle_info({tcp, Socket, Data}, #state{socket = Socket, transport = _Transport, buffer = Buffer, compression = Compression} = State) ->
    case tdm_native_parser:parse_frame(<<Buffer/binary, Data/binary>>, Compression) of
        {undefined, NewBuffer} ->
%%       set_active(Socket, Transport),
            {noreply, State#state{buffer = NewBuffer}};
        {Frame, NewBuffer} ->
            handle_frame(Frame, State#state{buffer = NewBuffer})
    end;

handle_info({ssl, Socket, Data}, #state{socket = Socket, buffer = Buffer, compression = Compression} = State) ->
    case tdm_native_parser:parse_frame(<<Buffer/binary, Data/binary>>, Compression) of
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

handle_info({inet_reply, _Socket, ok}, State) ->
    {noreply, State};

handle_info({inet_reply, _, Status}, State) ->
    error_logger:error_msg("Socket error [~p]~n", [Status]),
    {noreply, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->  %% todo
%%     error_logger:error_msg("Child killed ~p: ~p, state=~p", [Pid, Reason, State]),
    #tdm_stream{stream_pid = DefPid} = dict:fetch(?DEFAULT_STREAM_ID, State#state.streams),
    case Pid of
        DefPid -> {stop, {default_stream_death, Reason}, State};
        _ -> {noreply, State}
    end;

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
    ets:delete(?DEF_STREAM_ETS, self()),
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
    F = tdm_native_parser:encode_frame(Frame, Compression),
    Transport:send(Socket, F).


set_active(Socket, Transport) ->
    case Transport of
        gen_tcp -> inet:setopts(Socket, [{active, true}]);
        Transport -> Transport:setopts(Socket, [{active, true}])
    end.


handle_frame(Frame = #tdm_frame{header = Header}, State = #state{streams = Streams}) ->
%%   set_active(State#state.socket, Transport),
    StreamId = Header#tdm_header.stream,
    case dict:is_key(StreamId, Streams) of
        true ->
            Stream = dict:fetch(StreamId, Streams),
            tdm_stream:handle_frame(Stream#tdm_stream.stream_pid, Frame);
        false ->
            Stream = dict:fetch(?DEFAULT_STREAM_ID, Streams),
            tdm_stream:handle_frame(Stream#tdm_stream.stream_pid, Frame)
    end,
    {noreply, State}.

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
    case send(Socket, Transport, Compression, #tdm_frame{header = #tdm_header{opcode = ?OPC_STARTUP, type = request}, body = tdm_native_parser:encode_string_map(startup_opts(Compression))}) of
        ok ->
            {Frame, _R} = read_frame(Socket, Transport, Compression),
            case (Frame#tdm_frame.header)#tdm_header.opcode of
                ?OPC_ERROR ->
                    Error = tdm_native_parser:parse_error(Frame),
                    error_logger:error_msg("CQL error ~p~n", [Error]),
                    {error, Error};
                ?OPC_AUTHENTICATE ->
                    {Authenticator, _Rest} = tdm_native_parser:parse_string(Frame#tdm_frame.body),
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
    tdm_native_parser:parse_frame(F, Compression).


encode_plain_credentials(User, Password) when is_list(User) ->
    encode_plain_credentials(list_to_binary(User), Password);
encode_plain_credentials(User, Password) when is_list(Password) ->
    encode_plain_credentials(User, list_to_binary(Password));
encode_plain_credentials(User, Password) when is_binary(User), is_binary(Password)->
    tdm_native_parser:encode_bytes(<<0,User/binary,0,Password/binary>>).


authenticate(Socket, Authenticator, Credentials, Transport, Compression) ->
    %% todo: pluggable authentification
    {User, Password} = Credentials,
    case send(Socket, Transport, Compression, #tdm_frame{header = #tdm_header{opcode = ?OPC_AUTH_RESPONSE, type = request}, body = encode_plain_credentials(User, Password)}) of
        ok ->
            {Frame, _R} = read_frame(Socket, Transport, Compression),
            case (Frame#tdm_frame.header)#tdm_header.opcode of
                ?OPC_ERROR ->
                    Error = tdm_native_parser:parse_error(Frame),
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

get_default_stream(#tdm_connection{pid = Pid}) ->
    #tdm_stream{} = gen_server:call(Pid, {get_stream, ?DEFAULT_STREAM_ID}).
