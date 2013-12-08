-module(teledamus_srv).

-behaviour(gen_server).

-include_lib("rr.hrl").

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, get_connection/0, release_connection/1]).

-define(SERVER, ?MODULE).

-type node() :: [{nonempty_string(), pos_integer()}].
-type transport() :: gen_tcp | ssl.
-type compression() :: none | lz4 | snappy.
-record(state, {nodes :: rr_state(node()), opts :: list(), credentials :: {string(), string()}, transport = gen_tcp :: transport(), compression = none :: compression()}).

%%%===================================================================
%%% API
%%%===================================================================

get_connection() ->
	gen_server:call(?SERVER, get_connection).

release_connection(Connection) ->
	gen_server:call(?SERVER, {release_connection, Connection}).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(list()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
	Credentials = {proplists:get_value(username, Args), proplists:get_value(password, Args)},
	Nodes = proplists:get_value(cassandra_nodes, Args),
	Transport = case proplists:get_value(transport, Args, tcp) of
		tcp -> gen_tcp;
		ssl -> ssl
	end,
	Opts = prepare_transport(Transport, Args),
	Init = fun() ->
		#rr_state{resources = Nodes, rr = Nodes}
	end,
	Compression = proplists:get_value(compression, Args, none),
	gen_server:start_link({local, ?SERVER}, ?MODULE, [#rr_state{init = Init}, Opts, Credentials, Transport, Compression], []).

prepare_transport(gen_tcp, Args) ->
	[{active, false}, {packet, raw}, binary, {nodelay, true}] ++ proplists:get_value(tcp_opts, Args, []);
prepare_transport(ssl, Args) ->
	ok = application:ensure_started(asn1, permanent),
	ok = application:ensure_started(crypto, permanent),
  ok = application:ensure_started(public_key, permanent),
  ok = application:ensure_started(ssl, permanent),
	[{active, false}, {packet, raw}, binary, {nodelay, true}] ++ proplists:get_value(tcp_opts, Args, []) ++ proplists:get_value(ssl_opts, Args, []).

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
-spec(init(Args :: term()) ->
	{ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term()} | ignore).
init([RR, Opts, Credentials, Transport, Compression]) ->
	{ok, #state{nodes = rr:reinit(RR), opts = Opts, credentials = Credentials, transport = Transport, compression = Compression}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
									State :: #state{}) ->
									 {reply, Reply :: term(), NewState :: #state{}} |
									 {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
									 {noreply, NewState :: #state{}} |
									 {noreply, NewState :: #state{}, timeout() | hibernate} |
									 {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
									 {stop, Reason :: term(), NewState :: #state{}}).
handle_call(Request, _From, State) ->
	#state{nodes = Nodes, opts = Opts, credentials = Credentials, transport = Transport, compression = Compression} = State,
	case Request of
		get_connection -> % todo: connection pooling
			case rr:next(Nodes) of
				{error_no_resources, _S} ->
					throw(error_no_resources);
				{{Host, Port}, NS} ->
					case Transport:connect(Host, Port, Opts) of
						{ok, Socket} ->
							{ok, Connection} = connection:start_link(Socket, Credentials, Transport, Compression),
							ok = Transport:controlling_process(Socket, Connection),
							{reply, {connection, Connection}, State#state{nodes = NS}};
						{error, Reason} ->
							{reply, {error, Reason}, State#state{nodes = NS}}
					end
			end;
	  {release_connection, Connection} ->
			 Socket = connection:get_socket(Connection),
       Transport:close(Socket),
       {reply, ok, State}
	end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), NewState :: #state{}}).
handle_cast(Request, State) ->
	#state{nodes = Nodes} = State,
	%% todo: add public api and/or automatic cluster changes discovery
	case Request of
		{add_node, Host, Port} ->
			{noreply, State#state{nodes = rr:add(Nodes, {Host, Port})}};
		{remove_node, Host, Port} ->
			{noreply, State#state{nodes = rr:remove(Nodes, {Host, Port})}};
		reload_nodes ->
			{noreply, State#state{nodes = rr:reinit(Nodes)}};
    _ ->
      {noreply, State}
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
								State :: #state{}) -> term()).
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
									Extra :: term()) ->
									 {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

