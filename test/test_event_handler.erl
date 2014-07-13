-module(test_event_handler).

-behaviour(gen_event).

-export([start_link/0, add_handler/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {events :: list()}).

start_link() ->
    gen_event:start_link({local, ?SERVER}).

-spec(add_handler() -> ok | {'EXIT', Reason :: term()} | term()).
add_handler() ->
    gen_event:add_handler(?SERVER, ?MODULE, []).

init([]) ->
    {ok, #state{events = []}}.

handle_event(Event, State) ->
    Events = [Event | State#state.events],
    {ok, State#state{events = Events}}.

handle_call(Request, State) ->
    case Request of
        get_events ->
            {ok, State#state.events, State};
        _ ->
            {ok, ok, State}
    end.


handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

