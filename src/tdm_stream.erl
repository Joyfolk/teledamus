-module(tdm_stream).

%% -behaviour(gen_server).

%% API
-export([start/3, start/4, stop/2, stop/1]).

-export([options/2, query/4, prepare_query/3, execute_query/4, batch_query/3, subscribe_events/3, from_cache/2, to_cache/3, query/5, prepare_query/4, batch_query/4, handle_frame/2, init/4]).
-export([options_async/2, query_async/4, query_async/5, prepare_query_async/3, prepare_query_async/4, execute_query_async/4, batch_query_async/3, batch_query_async/4, subscribe_events_async/3]).

-include_lib("teledamus.hrl").


-record(state, {connection :: teledamus:connection(), id :: teledamus:stream_id(), caller :: term(), compression = none :: teledamus:compression(), channel_monitor :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start(teledamus:connection(), teledamus:stream_id(), teledamus:compression()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start(Connection, StreamId, Compression) ->
    proc_lib:start(?MODULE, init, [Connection, StreamId, Compression, undefined]).

-spec(start(teledamus:connection(), teledamus:stream_id(), teledamus:compression(), atom()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start(Connection, StreamId, Compression, ChannelMonitor) ->
    proc_lib:start(?MODULE, init, [Connection, StreamId, Compression, ChannelMonitor]).

-spec stop(Stream :: teledamus:stream()) ->  timeout | ok.
stop(Stream) ->
    stop(Stream, infinity).

-spec stop(Stream :: teledamus:stream(), Timeout :: timeout()) ->  timeout | ok.
stop(Stream, Timeout) ->
    call(Stream, stop, Timeout).

-spec options(Stream :: teledamus:stream(), Timeout :: timeout()) ->  timeout | teledamus:error() | teledamus:options().
options(Stream, Timeout) ->
    call(Stream, options, Timeout).

-spec options_async(Stream :: teledamus:stream(), ReplyTo :: teledamus:async_target()) ->  ok | {error, Reason :: term()}.
options_async(Stream, ReplyTo) ->
    cast(Stream, options, ReplyTo).

-spec query(Stream :: teledamus:stream(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), Timeout :: timeout()) -> timeout | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
query(Stream, Query, Params, Timeout) ->
    call(Stream, {query, Query, Params}, Timeout).

-spec query_async(Stream :: teledamus:stream(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target()) -> ok | {error, Reason :: term()}.
query_async(Stream, Query, Params, ReplyTo) ->
    cast(Stream, {query, Query, Params}, ReplyTo).

-spec query(Stream :: teledamus:stream(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
query(Stream = #tdm_stream{connection = Con}, Query, Params, Timeout, UseCache) ->
    case UseCache of
        true ->
            case tdm_stmt_cache:cache(Query, Con, Timeout) of
                {ok, Id} -> call(Stream, {execute, Id, Params}, Timeout);
                Err = #tdm_error{} -> Err
            end;
        false ->
            call(Stream, {query, Query, Params}, Timeout)
    end.

-spec query_async(Stream :: teledamus:stream(), Query :: teledamus:query_text(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
query_async(Stream = #tdm_stream{connection = Con}, Query, Params, ReplyTo, UseCache) ->
    case UseCache of
        true ->
            tdm_stmt_cache:cache_async(Query, Con, fun(Res) ->
                case Res of
                    {ok, Id} -> cast(Stream, {execute, Id, Params}, ReplyTo);
                    Err = #tdm_error{} -> Err
                end
            end);
        false ->
            cast(Stream, {query, Query, Params}, ReplyTo)
    end.


-spec prepare_query(Stream :: teledamus:stream(), Query :: teledamus:query_text(), Timeout :: timeout()) -> timeout | teledamus:error() | {teledamus:prepared_query_id(), teledamus:metadata(), teledamus:metadata()}.
prepare_query(Stream, Query, Timeout) ->
    prepare_query(Stream, Query, Timeout, false).

-spec prepare_query_async(Stream :: teledamus:stream(), Query :: teledamus:string(), ReplyTo :: teledamus:async_target()) -> ok | {error, Reason :: term()}.
prepare_query_async(Stream, Query, Timeout) ->
    prepare_query_async(Stream, Query, Timeout, false).

-spec prepare_query(Stream :: teledamus:stream(), Query :: teledamus:query_text(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | teledamus:error() | {teledamus:prepared_query_id(), teledamus:metadata(), teledamus:metadata()}.
prepare_query(Stream, Query, Timeout, UseCache) ->
    R = call(Stream, {prepare, Query}, Timeout),
    case {UseCache, R} of
        {true, {Id, _, _}} ->
            to_cache(Stream, Query, Id),
            R;
        {false, _} ->
            R
    end.

-spec prepare_query_async(Stream :: teledamus:stream(), Query :: teledamus:query_text(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> ok | {error, Reason :: term()}.
prepare_query_async(Stream, Query, ReplyTo, UseCache) ->
    cast(Stream, {prepare, Query}, fun(R) ->
        case {UseCache, R} of
            {true, {Id, _, _}} ->
                to_cache(Stream, Query, Id),
                reply_if_needed(ReplyTo, R, undefined); %% todo: monitoring?
            {false, _} ->
                reply_if_needed(ReplyTo, R, undefined)  %% todo: monitoring?
        end
    end).

-spec execute_query(Stream :: teledamus:stream(), ID :: teledamus:prepared_query_id(), Params :: teledamus:query_params(), Timeout :: timeout()) -> timeout | ok | teledamus:error() | teledamus:result_rows() | teledamus:schema_change().
execute_query(Stream, ID, Params, Timeout) ->
    call(Stream, {execute, ID, Params}, Timeout).

-spec execute_query_async(Stream :: teledamus:stream(), ID :: teledamus:prepared_query_id(), Params :: teledamus:query_params(), ReplyTo :: teledamus:async_target()) -> ok | {error, Reason :: term()}.
execute_query_async(Stream, ID, Params, ReplyTo) ->
    cast(Stream, {execute, ID, Params}, ReplyTo).


-spec batch_query(Stream :: teledamus:stream(), Batch :: teledamus:batch_query(), Timeout :: timeout()) -> timeout | ok | teledamus:error().
batch_query(Stream, Batch, Timeout) ->
    call(Stream, {batch, Batch}, Timeout).

-spec batch_query_async(Stream :: teledamus:stream(), Batch :: teledamus:batch_query(), ReplyTo :: teledamus:async_target()) -> ok | {error, Reason :: term()}.
batch_query_async(Stream, Batch, ReplyTo) ->
    cast(Stream, {batch, Batch}, ReplyTo).

-spec batch_query(Stream :: teledamus:stream(), Batch :: teledamus:batch_query(), Timeout :: timeout(), UseCache :: boolean()) -> timeout | ok | teledamus:error().
batch_query(Stream = #tdm_stream{connection = Con}, Batch = #tdm_batch_query{queries = Queries}, Timeout, UseCache) ->
    case UseCache of
        true ->
            NBatch = Batch#tdm_batch_query{queries = lists:map(
                fun({Id, Args}) when is_binary(Id) ->
                    {Id, Args};
                   ({Query, Args}) when is_list(Query) ->
                       case tdm_stmt_cache:cache(Query, Con, Timeout) of
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

-spec batch_query_async(Stream :: teledamus:stream(), Batch :: teledamus:batch_query(), ReplyTo :: teledamus:async_target(), UseCache :: boolean()) -> timeout | ok.
batch_query_async(Stream, Batch, ReplyTo, false) ->
    batch_query_async(Stream, Batch, ReplyTo);
batch_query_async(Stream = #tdm_stream{connection = Con}, Batch = #tdm_batch_query{queries = Queries}, ReplyTo, true) ->
    ToCache = lists:filter(fun({Q, _Args}) -> is_list(Q) end, Queries),
    tdm_stmt_cache:cache_async_multple(ToCache, Con, fun(Dict) ->
        Qs = lists:map(fun({Q, Arg}) ->
            if
                is_binary(Q) -> {Q, Arg};
                true -> {dict:fetch(Q, Dict), Arg}
            end
        end, Queries),
        cast(Stream, {batch, Batch#tdm_batch_query{queries = Qs}}, ReplyTo)
    end).

-spec subscribe_events(Stream :: teledamus:stream(), EventTypes :: list(string() | atom()), Timeout :: timeout()) -> ok | timeout | teledamus:error().
subscribe_events(Stream, EventTypes, Timeout) ->
    call(Stream, {register, EventTypes}, Timeout).

-spec subscribe_events_async(Stream ::teledamus:stream(), EventTypes :: list(string() | atom()), ReplyTo :: teledamus:async_target()) -> ok | {error, Reason :: term()}.
subscribe_events_async(Stream, EventTypes, ReplyTo) ->
    cast(Stream, {register, EventTypes}, ReplyTo).

-spec from_cache(teledamus:stream(), teledamus:query_text()) -> {ok, teledamus:prepared_query_id()} | not_found.
from_cache(#tdm_stream{connection = #tdm_connection{host = Host, port = Port}}, Query) ->
    tdm_stmt_cache:from_cache({Host, Port, Query}).

-spec to_cache(teledamus:stream(), teledamus:query_text(), teledamus:prepared_query_id()) -> true.
to_cache(#tdm_stream{connection = #tdm_connection{host = Host, port = Port}}, Query, Id) ->
    tdm_stmt_cache:to_cache({Host, Port, Query}, Id).



init(Connection, StreamId, Compression, ChannelMonitor) ->
    proc_lib:init_ack({ok, self()}),
    erlang:monitor(process, Connection#tdm_connection.pid),
    loop(#state{connection = Connection, id = StreamId, compression = Compression, channel_monitor = ChannelMonitor}).

loop(State) ->
    receive
        {'DOWN', _MonitorRef, _Type, _Object, Info} ->
            error_logger:error_msg("Connection down ~p: ~p", [State#state.connection, Info]),
            exit(connection_down);
        Msg ->
            try
                case handle_msg(Msg, State) of
                    {noreply, NewState} ->
                        erlang:yield(),
                        loop(NewState);
                    {stop, _NewState} ->
                        exit(normal)
                end
            catch
                E: EE ->
                    error_logger:error_msg("~p:~p, stacktrace=~p", [E, EE, erlang:get_stacktrace()]),
                    exit({E, EE})
            end
    end.


handle_frame(Pid, Frame) ->
    Pid ! {handle_frame, Frame}.

reply_if_needed(Caller, Reply, ChannelMonitor) ->
    case ChannelMonitor of
        undefined -> ok;
        _ -> ChannelMonitor:on_reply(Caller, Reply)
    end,
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


handle_msg(Request, State = #state{caller = Caller, connection = #tdm_connection{pid = Connection}, compression = Compression, id = StreamId, channel_monitor = ChannelMonitor}) ->
    case Request of
        {call, From, Msg} ->
            case ChannelMonitor of
                undefined -> ok;
                _ -> ChannelMonitor:on_call(From, Msg)
            end,
            case Msg of
                stop ->
                    {stop, State};

                options ->
                    Frame = #tdm_frame{header = #tdm_header{type = request, opcode = ?OPC_OPTIONS, stream = StreamId}, length = 0, body = <<>>},
                    tdm_connection:send_frame(Connection, tdm_native_parser:encode_frame(Frame, Compression)),
                    {noreply, State#state{caller = From}};

                {query, Query, Params} ->
                    Body = tdm_native_parser:encode_query(Query, Params),
                    Frame = #tdm_frame{header = #tdm_header{type = request, opcode = ?OPC_QUERY, stream = StreamId}, length = byte_size(Body), body = Body},
                    tdm_connection:send_frame(Connection, tdm_native_parser:encode_frame(Frame, Compression)),
                    {noreply, State#state{caller = From}};

                {prepare, Query} ->
                    Body = tdm_native_parser:encode_long_string(Query),
                    Frame = #tdm_frame{header = #tdm_header{type = request, opcode = ?OPC_PREPARE, stream = StreamId}, length = byte_size(Body), body = Body},
                    tdm_connection:send_frame(Connection, tdm_native_parser:encode_frame(Frame, Compression)),
                    {noreply, State#state{caller = From}};

                {execute, ID, Params} ->
                    Body = tdm_native_parser:encode_query(ID, Params),
                    Frame = #tdm_frame{header = #tdm_header{type = request, opcode = ?OPC_EXECUTE, stream = StreamId}, length = byte_size(Body), body = Body},
                    tdm_connection:send_frame(Connection, tdm_native_parser:encode_frame(Frame, Compression)),
                    {noreply, State#state{caller = From}};

                {batch, BatchQuery} ->
                    Body = tdm_native_parser:encode_batch_query(BatchQuery),
                    Frame = #tdm_frame{header = #tdm_header{type = request, opcode = ?OPC_BATCH, stream = StreamId}, length = byte_size(Body), body = Body},
                    tdm_connection:send_frame(Connection, tdm_native_parser:encode_frame(Frame, Compression)),
                    {noreply, State#state{caller = From}};

                {register, EventTypes} ->
                    start_gen_event_if_required(),
                    Body = tdm_native_parser:encode_event_types(EventTypes),
                    Frame = #tdm_frame{header = #tdm_header{type = request, opcode = ?OPC_REGISTER, stream = StreamId}, length = byte_size(Body), body = Body},
                    tdm_connection:send_frame(Connection, tdm_native_parser:encode_frame(Frame, Compression)),
                    {noreply, State#state{caller = From}};

                _ ->
                    error_logger:error_msg("Unknown request ~p~n", [Request]),
                    reply_if_needed(From, unknown_request, ChannelMonitor),
                    {noreply, State#state{caller = undefined}}
            end;

        {handle_frame, Frame} ->
            OpCode = (Frame#tdm_frame.header)#tdm_header.opcode,
            case OpCode of
                ?OPC_ERROR ->
                    Error = tdm_native_parser:parse_error(Frame),
                    error_logger:error_msg("CQL error ~p~n", [Error]),
                    reply_if_needed(Caller, {error, Error}, ChannelMonitor),
                    {noreply, State#state{caller = undefined}};
                ?OPC_READY ->
                    reply_if_needed(Caller, ok, ChannelMonitor),
                    {noreply, State#state{caller = undefined}};
                ?OPC_AUTHENTICATE ->
                    throw({not_supported_option, authentificate}),
                    {noreply, State};
                ?OPC_SUPPORTED ->
                    {Options, _} = tdm_native_parser:parse_string_multimap(Frame#tdm_frame.body),
                    reply_if_needed(Caller, Options, ChannelMonitor),
                    {noreply, State#state{caller = undefined}};
                ?OPC_RESULT ->
                    Result = tdm_native_parser:parse_result(Frame),
                    reply_if_needed(Caller, Result, ChannelMonitor),
                    {noreply, State#state{caller = undefined}};
                ?OPC_EVENT ->
                    Result = tdm_native_parser:parse_event(Frame),
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
            {ok, _Pid} = gen_event:start_link({local, cassandra_events}),
            ok;
        _ ->
            ok
    end.


call(#tdm_stream{stream_pid = Pid}, Msg, Timeout) ->
    try erlang:monitor(process, Pid) of
        Mref ->
            Tag = {self(), Mref},
            erlang:send(Pid, {call, Tag, Msg}, [noconnect]),
            receive
                {reply, Tag, X} ->
                    erlang:demonitor(Mref, [flush]),
                    X;
                {'DOWN', _MonitorRef, _Type, _Object, normal} ->
                    ok;
                {'DOWN', _MonitorRef, _Type, _Object, Info} ->
                    {error, Info}
            after
                Timeout ->
                    {error, timeout}
            end
    catch
        error: Reason ->
            {error, Reason}
    end.

cast(#tdm_stream{stream_pid = Pid}, Msg, AsyncTarget) ->
    try
        ok = erlang:send(Pid, {call, AsyncTarget, Msg}, [noconnect])
    catch
        error: Reason ->
            {error, Reason}
    end.