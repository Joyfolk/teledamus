-module(tdm_connection).

-behaviour(gen_server).

-include_lib("teledamus.hrl").


-define(CONNECTION_TIMEOUT, 2000).
-define(RECV_TIMEOUT, 2000).

%% API
-export([start/8, prepare_ets/0]).
-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, get_socket/1, from_cache/2, to_cache/3, query/5, prepare_query/4, batch_query/4,
    new_stream/2, release_stream/2, release_stream_async/1, send_frame/2, get_default_stream/1]).

-export([options_async/2, query_async/4, query_async/5, prepare_query_async/3, prepare_query_async/4, execute_query_async/4, batch_query_async/3, batch_query_async/4, subscribe_events_async/3,
    close/2, get_protocol_version/1, get_connection/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_STREAM_ID, 0).
-define(DEF_STREAM_ETS, default_streams).


-record(state, {transport = gen_tcp :: teledamus:transport(), socket :: teledamus:socket(), buffer = <<>>:: binary(), caller :: pid(), compression = none :: teledamus:compression(),
    streams :: dict(), host :: list(), port :: pos_integer(), monitor_ref :: reference(), protocol = tdm_cql3 :: teledamus:protocol_module()}).


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

-spec options(Connection :: teledamus:connection(), Timeout :: timeout()) ->  {error, timeout} | teledamus:error() | teledamus:options().
options(#tdm_connection{default_stream = Stream}, Timeout) ->
    tdm_stream:options(Stream, Timeout).

-spec options_async(Connection :: teledamus:connection(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
options_async(#tdm_connection{default_stream = Stream}, Timeout) ->
    tdm_stream:options_async(Stream, Timeout).

-spec query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), Timeout :: timeout()) -> {error, timeout} | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
query(#tdm_connection{default_stream = Stream}, Query, Params, Timeout) ->
    tdm_stream:query(Stream, Query, Params, Timeout).

-spec query_async(Con :: teledamus:connection(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
query_async(#tdm_connection{default_stream = Stream}, Query, Params, ReplyTo) ->
    tdm_stream:query_async(Stream, Query, Params, ReplyTo).

-spec query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), Timeout :: timeout(), UseCache :: boolean()) -> {error, timeout} | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
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

-spec prepare_query(Con :: teledamus:connection(), Query :: teledamus:query_text(), Timeout :: timeout(), UseCache :: boolean()) -> {error, timeout} | teledamus:error() | {binary(), teledamus:metadata(), teledamus:metadata()}.
prepare_query(#tdm_connection{default_stream = Stream}, Query, Timeout, UseCache) ->
    tdm_stream:prepare_query(Stream, Query, Timeout, UseCache).

-spec prepare_query_async(Con :: teledamus:connection(), Query :: teledamus:query_text(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
prepare_query_async(#tdm_connection{default_stream = Stream}, Query, ReplyTo, UseCache) ->
    tdm_stream:prepare_query_async(Stream, Query, ReplyTo, UseCache).

-spec execute_query(Con :: teledamus:connection(), ID :: teledamus:prepared_query_id(), Params :: teledamus:query_params(), Timeout :: timeout()) -> {error, timeout} | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
execute_query(#tdm_connection{default_stream = Stream}, ID, Params, Timeout) ->
    tdm_stream:execute_query(Stream, ID, Params, Timeout).

-spec execute_query_async(Con :: teledamus:connection(), ID :: teledamus:prepared_query_id(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
execute_query_async(#tdm_connection{default_stream = Stream}, ID, Params, ReplyTo) ->
    tdm_stream:execute_query_async(Stream, ID, Params, ReplyTo).

-spec batch_query(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), Timeout :: timeout()) -> {error, timeout} | teledamus:result_rows() | ok | teledamus:error().
batch_query(#tdm_connection{default_stream = Stream}, Batch, Timeout) ->
    tdm_stream:batch_query(Stream, Batch, Timeout).

-spec batch_query_async(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
batch_query_async(#tdm_connection{default_stream = Stream}, Batch, ReplyTo) ->
    tdm_stream:batch_query_async(Stream, Batch, ReplyTo).

-spec batch_query(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), Timeout :: timeout(), UseCache :: boolean()) -> {error, timeout} | ok | teledamus:error().
batch_query(#tdm_connection{default_stream = Stream}, Batch, Timeout, UseCache) ->
    tdm_stream:batch_query(Stream, Batch, Timeout, UseCache).

-spec batch_query_async(Con :: teledamus:connection(), Batch :: teledamus:batch_query(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
batch_query_async(#tdm_connection{default_stream = Stream}, Batch, ReplyTo, UseCache) ->
    tdm_stream:batch_query_async(Stream, Batch, ReplyTo, UseCache).

-spec subscribe_events(Con :: teledamus:connection(), EventTypes :: list(string() | atom()), Timeout :: timeout()) -> ok | {error, timeout} | teledamus:error().
subscribe_events(#tdm_connection{default_stream = Stream}, EventTypes, Timeout) ->
    tdm_stream:subscribe_events(Stream, EventTypes, Timeout).

-spec subscribe_events_async(Con :: teledamus:connection(), EventTypes :: [string() | atom()], ReplyTo :: teledamus:async_target()) -> ok | {error, timeout} | {error, Reason :: term()}.
subscribe_events_async(#tdm_connection{default_stream = Stream}, EventTypes, ReplyTo) ->
    tdm_stream:subscribe_events_async(Stream, EventTypes, ReplyTo).

-spec get_socket(Con :: teledamus:connection()) -> teledamus:socket() | {error, Reason :: term()}.
get_socket(#tdm_connection{pid = Pid})->
    gen_server:call(Pid, get_socket).

-spec get_connection(Pid :: pid()) -> teledamus:connection() | {error, Reason :: term()}.
get_connection(Pid) ->
    gen_server:call(Pid, get_connection).

-spec get_protocol_version(Con :: teledamus:connection()) -> teledamus:protocol_version().
get_protocol_version(#tdm_connection{pid = Pid}) ->
    gen_server:call(Pid, get_protocol_version).

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
%% @spec start() ->
%%          {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start(Host, Port, Opts, Credentials, Transport, Compression, ChannelMonitor, MinProtocol) ->
    gen_server:start(?MODULE, [Host, Port, Opts, Credentials, Transport, Compression, ChannelMonitor, MinProtocol], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) ->
%%          {ok, State} | {ok, State, Timeout} | ignore | {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port, Opts, Credentials, Transport, Compression, ChannelMonitor, MinProtocol]) ->
    try
        case startup(Host, Port, Opts, Credentials, Transport, Compression, MinProtocol) of
            {ok, Socket, Protocol} ->
                set_active(Socket, Transport),
                Connection = #tdm_connection{pid = self(), host = Host, port = Port},
                {ok, StreamId} = tdm_stream:start(Connection, ?DEFAULT_STREAM_ID, Compression, ChannelMonitor, protocol_to_module(Protocol)),
                MonitorRef = monitor(process, StreamId),
                DefStream = #tdm_stream{connection = Connection, stream_id = ?DEFAULT_STREAM_ID, stream_pid = StreamId},
                DefStream2 = DefStream#tdm_stream{connection = Connection#tdm_connection{default_stream = DefStream}},
                ets:insert(?DEF_STREAM_ETS, {self(), DefStream2}),
                {ok, #state{
                    socket = Socket,
                    transport = Transport,
                    compression = Compression,
                    streams = dict:store(?DEFAULT_STREAM_ID, DefStream2, dict:new()),
                    monitor_ref = MonitorRef,
                    protocol = protocol_to_module(Protocol),
                    host = Host,
                    port = Port
                }};
            {error, Reason} ->
                error_logger:error_msg("Failed to start with ~p", [Reason]),
                {stop, Reason}
        end
    catch
        E1: EE1 ->
            error_logger:error_msg("Failed to start with ~p:~p, stacktrace=~p", [E1, EE1, erlang:get_stacktrace()]),
            {stop, {E1, EE1}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) -> {reply, Reply, State} | {reply, Reply, State, Timeout} | {noreply, State} | {noreply, State, Timeout} | {stop, Reason, Reply, State} | {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State = #state{socket = Socket, transport = Transport, compression = Compression, streams = Streams, host = Host, port = Port, monitor_ref = MonitorRef, protocol = Protocol}) ->
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
                    case tdm_stream:start(Connection, StreamId, Compression, Protocol) of
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
            {reply, State#state.socket, State};

        get_connection ->
            {reply, #tdm_connection{default_stream = dict:fetch(?DEFAULT_STREAM_ID, Streams), host = Host, port = Port, pid = self()}, State};

        get_protocol_version ->
            {reply, module_to_protocol(Protocol), State};

        {stop, Timeout} ->
            stop_int(MonitorRef, Streams, Timeout, Socket, Transport),
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
                gen_tcp ->
                    erlang:port_command(Socket, Frame);
                _ ->
                    Transport:send(Socket, Frame)
            end,
            {noreply, State};

        {release_stream, #tdm_stream{stream_id = Id}} ->
            {noreply, State#state{streams = dict:erase(Id, Streams)}};

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
handle_info({tcp, Socket, Data}, #state{socket = Socket, transport = _Transport, buffer = Buffer, compression = Compression, protocol = Protocol} = State) ->
    case Protocol:parse_frame(<<Buffer/binary, Data/binary>>, Compression) of
        {undefined, NewBuffer} ->
%%       set_active(Socket, Transport),
            {noreply, State#state{buffer = NewBuffer}};
        {Frame, NewBuffer} ->
            handle_frame(Frame, State#state{buffer = NewBuffer})
    end;

handle_info({ssl, Socket, Data}, #state{socket = Socket, buffer = Buffer, compression = Compression, protocol = Protocol} = State) ->
    case Protocol:parse_frame(<<Buffer/binary, Data/binary>>, Compression) of
        {undefined, NewBuffer} ->
            {noreply, State#state{buffer = NewBuffer}};
        {Frame, NewBuffer} ->
            handle_frame(Frame, State#state{buffer = NewBuffer})
    end;

handle_info({tcp_closed, _Socket}, State) ->
    error_logger:error_msg("~p: TCP connection closed~n", [self()]),
    {stop, tcp_closed, State};

handle_info({ssl_closed, _Socket}, State) ->
    error_logger:error_msg("~p: SSL connection closed~n", [self()]),
    {stop, ssl_closed, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    error_logger:error_msg("~p: TCP error [~p]~n", [self(), Reason]),
    {stop, {tcp_error, Reason}, State};

handle_info({ssl_error, _Socket, Reason}, State) ->
    error_logger:error_msg("~p: SSL error [~p]~n", [self(), Reason]),
    {stop, {tcp_error, Reason}, State};

handle_info({inet_reply, _Socket, ok}, State) ->
    {noreply, State};

handle_info({inet_reply, _, Status}, State) ->
    error_logger:error_msg("Socket error [~p]~n", [Status]),
    {noreply, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->  %% todo
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
terminate(Reason, #state{socket = Socket, transport = Transport, host = Host, port = Port, monitor_ref = MonitorRef, streams = Streams}) ->
    try
        stop_int(MonitorRef, Streams, 5000, Socket, Transport),
        ets:delete(?DEF_STREAM_ETS, self())
    after
        case Reason of
            normal ->
                ok;
            shutdown ->
                ok;
            {shutdown, _Reason} ->
                ok;
            _ ->
                tdm_stmt_cache:invalidate(Host, Port) %% invalidate cache in case of cassandra server shutdown
        end
    end.

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


send(Socket, Transport, Compression, Frame, Protocol) ->
    F = Protocol:encode_frame(Frame, Compression),
    Transport:send(Socket, F).


set_active(Socket, Transport) ->
    case Transport of
        gen_tcp -> inet:setopts(Socket, [{active, true}]);
        Transport -> Transport:setopts(Socket, [{active, true}])
    end.


handle_frame(Frame = #tdm_frame{header = Header}, State = #state{streams = Streams}) ->
%%   set_active(State#state.socket, Transport),
    StreamId = case Header#tdm_header.stream >= 0 of
                   true -> Header#tdm_header.stream;
                   false -> ?DEFAULT_STREAM_ID          %% negative stream id is server initiated streams
               end,
    case dict:is_key(StreamId, Streams) of
        true ->
            Stream = dict:fetch(StreamId, Streams),
            tdm_stream:handle_frame(Stream#tdm_stream.stream_pid, Frame);
        false ->
            error_logger:info_msg("Unhandled frame to stream ~p", [StreamId]),
            ignore
    end,
    {noreply, State}.

startup_opts(Compression, Protocol) ->
    CQL = [{<<"CQL_VERSION">>,  Protocol:cql_version()}],
    case Compression of
        none ->
            CQL;
        {Name, _, _} ->
            N = list_to_binary(Name),
            [{<<"COMPRESSION", N/binary>>} | CQL];
        _ ->
            throw({unsupported_compression_type, Compression})
    end.


-spec protocol_to_module(teledamus:protocol_version()) -> teledamus:protocol_module().
protocol_to_module(cql1) -> tdm_cql1;
protocol_to_module(cql2) -> tdm_cql2;
protocol_to_module(cql3) -> tdm_cql3.


-spec module_to_protocol(teledamus:protocol_module()) -> teledamus:protocol_version().
module_to_protocol(tdm_cql1) -> cql1;
module_to_protocol(tdm_cql2) -> cql2;
module_to_protocol(tdm_cql3) -> cql3.



startup(Host, Port, Opts, Credentials, Transport, Compression, MinProtocol) ->
    Protocols = lists:filter(fun(Protocol) -> Protocol =:= protocol_to_module(MinProtocol) end, [tdm_cql3, tdm_cql2, tdm_cql1]),
    startup_int(Host, Port, Opts, Credentials, Transport, Compression, Protocols, MinProtocol).

startup_int(_Host, _Port, _Opts, _Credentials, _Transport, _Compression, [], MinProtocol) ->
    {error, {protocol_version_not_supported, MinProtocol}};
startup_int(Host, Port, Opts, Credentials, Transport, Compression, [Protocol | Protocols], MinProtocol) ->
    case startup_int(Host, Port, Opts, Credentials, Transport, Compression, Protocol) of
        {error, closed} ->  %% try to lower protocol version
            startup_int(Host, Port, Opts, Credentials, Transport, Compression, Protocols, MinProtocol);
        Ok = {ok, _Socket, _Protocol} ->
            Ok;
        Error ->
            error_logger:error_msg("Connection error: ~p", [Error]),
            Error
    end.

startup_int(Host, Port, Opts, Credentials, Transport, Compression, Protocol) ->
    try Transport:connect(Host, Port, Opts, ?CONNECTION_TIMEOUT) of
        {ok, Socket} ->
            try
                CasOpts = startup_opts(Compression, Protocol),
                case send(Socket, Transport, Compression, #tdm_frame{header = Protocol:startup_header(), body = Protocol:encode_string_map(CasOpts)}, Protocol) of
                    ok ->
                        {Frame, _R} = read_frame(Socket, Transport, Compression, Protocol),
                        case (Frame#tdm_frame.header)#tdm_header.opcode of
                            ?OPC_ERROR ->
                                Error = Protocol:parse_error(Frame),
                                error_logger:error_msg("CQL error ~p~n", [Error]),
                                {error, Error};
                            ?OPC_AUTHENTICATE ->
                                {Authenticator, _Rest} = Protocol:parse_string(Frame#tdm_frame.body),
                                authenticate(Socket, Authenticator, Credentials, Transport, Compression, Protocol);
                            ?OPC_READY ->
                                {ok, Socket, module_to_protocol(Protocol)};
                            OpCode ->
                                {error, {unknown_response_code, OpCode}}
                        end;
                    Error ->
                        {error, Error}
                end
            catch
                throw:closed -> %% todo: strange control passing, replace
                    {error, closed};
                E1:EE1 ->
                    error_logger:error_msg("Connection error: ~p:~p, trace=~p", [E1, EE1, erlang:get_stacktrace()]),
                    catch Transport:close(Socket),
                    {error, {E1, EE1, erlang:get_stacktrace()}}
            end;
        Err = {error, Reason} ->
            error_logger:error_msg("~p:~p : Connection error: ~p", [Host, Port, Reason]),
            Err
    catch
        E: EE ->
            error_logger:error_msg("~p:~p : Connection error: ~p:~p, trace=~p", [Host, Port, E, EE, erlang:get_stacktrace()]),
            {error, {E, EE, erlang:get_stacktrace()}}
    end.


read_frame(Socket, Transport, Compression, Protocol) ->
    HeaderLen = Protocol:header_length(),
    {ok, Data} = Transport:recv(Socket, HeaderLen + 4, ?RECV_TIMEOUT), %% header + frame length (32b)
    <<_Header:HeaderLen/binary, Length:32/big-unsigned-integer>> = Data,
    Body = if
        Length =/= 0 ->
            case Transport:recv(Socket, Length, ?RECV_TIMEOUT) of
                {ok, X} -> X;
                {error, X} -> throw(X)
            end;
        true ->
            <<>>
    end,
    F = <<Data/binary,Body/binary>>,
    Protocol:parse_frame(F, Compression).


encode_plain_credentials(User, Password, Protocol) when is_list(User) ->
    encode_plain_credentials(list_to_binary(User), Password, Protocol);
encode_plain_credentials(User, Password, Protocol) when is_list(Password) ->
    encode_plain_credentials(User, list_to_binary(Password), Protocol);
encode_plain_credentials(User, Password, Protocol) when is_binary(User), is_binary(Password)->
    Protocol:encode_bytes(<<0,User/binary,0,Password/binary>>).


authenticate(Socket, Authenticator, Credentials, Transport, Compression, Protocol) ->
    %% todo: pluggable authentification
    {User, Password} = Credentials,
    case send(Socket, Transport, Compression, #tdm_frame{header = #tdm_header{opcode = ?OPC_AUTH_RESPONSE, type = request}, body = encode_plain_credentials(User, Password, Protocol)}, Protocol) of
        ok ->
            {Frame, _R} = read_frame(Socket, Transport, Compression, Protocol),
            case (Frame#tdm_frame.header)#tdm_header.opcode of
                ?OPC_ERROR ->
                    Error = Protocol:parse_error(Frame),
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


stop_int(MonitorRef, Streams, Timeout, Socket, Transport) ->
    error_logger:info_msg("Stopping connection~n"),
    demonitor(MonitorRef, [flush]),
    DefStream = dict:fetch(?DEFAULT_STREAM_ID, Streams),
    catch tdm_stream:close(DefStream, Timeout),
    catch Transport:close(Socket).