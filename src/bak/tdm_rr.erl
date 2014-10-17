-module(tdm_rr).

-include_lib("teledamus_rr.hrl").

%% API
-export([next/1, add/2, remove/2, from_app/2, reinit/1]).

-spec next(rr_state(RType)) -> {RType, rr_state(RType)} | {error_no_resources, rr_state(RType)}.
next(State) when is_record(State, tdm_rr_state) ->
    #tdm_rr_state{resources = R, rr = RR, init = F} = State,
    case RR of
        [H | T] ->
            {H, State#tdm_rr_state{rr = T}};
        [] ->
            case R of
                [] ->
                    NS = F(),
                    #tdm_rr_state{resources = R1} = NS,
                    case R1 of
                        [] ->
                            {error_no_resources, State};
                        _ ->
                            {hd(R1), #tdm_rr_state{resources = R1, rr = tl(R1), init = F}}
                    end;
                [H | T] ->
                    {H, State#tdm_rr_state{rr = T}}
            end
    end.

-spec add(rr_state(RType), rr_list(RType) | RType) -> rr_state(RType).
add(State, ResList) when is_record(State, tdm_rr_state), is_list(ResList) ->
    State#tdm_rr_state{resources = State#tdm_rr_state.resources ++ ResList};
add(State, Res) when is_record(State, tdm_rr_state) ->
    add(State, [Res]).

-spec remove(rr_state(RType), rr_list(RType) | RType) -> rr_state(RType).
remove(State, ResList) when is_record(State, tdm_rr_state), is_list(ResList) ->
    State#tdm_rr_state{resources = State#tdm_rr_state.resources -- ResList, rr = State#tdm_rr_state.rr -- ResList};
remove(State, Res) when is_record(State, tdm_rr_state) ->
    remove(State, [Res]).


-spec from_app(atom(), atom()) -> rr_state(_).
from_app(App, ParName) ->
    F = fun() ->
        RL = application:get_env(App, ParName, []),
        #tdm_rr_state{resources = RL, rr = RL}
    end,
    (F())#tdm_rr_state{init = F}.

-spec reinit(rr_state(RType)) -> rr_state(RType).
reinit(State) when is_record(State, tdm_rr_state) ->
    (State#tdm_rr_state.init)().
