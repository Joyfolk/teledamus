-module(teledamus_app).

-behaviour(application).


-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Start teledamus application
%% @end
%%--------------------------------------------------------------------
-spec(start(StartType :: normal | {takeover, node()} | {failover, node()}, StartArgs :: term()) -> {ok, pid()} | {ok, pid(), State :: term()} | {error, Reason :: term()}).
start(_StartType, _Args) ->
	Args = application:get_all_env(teledamus),%%
	teledamus_sup:start_link(Args).


%%--------------------------------------------------------------------
%% @doc
%% Stop teledamus application
%% @end
%%--------------------------------------------------------------------
-spec(stop(State :: term()) -> term()).
stop(_State) ->
	ok.

