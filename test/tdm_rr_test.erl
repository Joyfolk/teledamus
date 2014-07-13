-module(tdm_rr_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("teledamus_rr.hrl").

set_env(Env) ->
    application:set_env(test, round_robin, Env).

unset_env() ->
    application:unset_env(test, round_robin).

empty_test() ->
    R = tdm_rr:from_app(test, round_robin),
    ?assertEqual([], R#tdm_rr_state.resources),
    ?assertEqual([], R#tdm_rr_state.rr).


from_app_test() ->
    set_env([r1, r2, r3]),
    R = tdm_rr:from_app(test, round_robin),
    ?assertEqual([r1, r2, r3], R#tdm_rr_state.resources),
    ?assertEqual([r1, r2, r3], R#tdm_rr_state.rr),
    unset_env().

add_test() ->
    set_env([r1, r2, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R = tdm_rr:add(RR, r3),
    ?assertEqual([r1, r2, r4, r3], R#tdm_rr_state.resources),
    ?assertEqual([r1, r2, r4], R#tdm_rr_state.rr),
    unset_env().

add_list_test() ->
    set_env([r1, r2, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R = tdm_rr:add(RR, [r3, r5]),
    ?assertEqual([r1, r2, r4, r3, r5], R#tdm_rr_state.resources),
    ?assertEqual([r1, r2, r4], R#tdm_rr_state.rr),
    unset_env().

remove_test() ->
    set_env([r1, r2, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R = tdm_rr:remove(RR, r2),
    ?assertEqual([r1, r4], R#tdm_rr_state.resources),
    ?assertEqual([r1, r4], R#tdm_rr_state.rr),
    unset_env().

remove_list_test() ->
    set_env([r1, r2, r3, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R = tdm_rr:remove(RR, [r3, r4, r5]),
    ?assertEqual([r1, r2], R#tdm_rr_state.resources),
    ?assertEqual([r1, r2], R#tdm_rr_state.rr),
    unset_env().

reinit_test() ->
    set_env([r1, r2, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R1 = tdm_rr:remove(RR, [r1, r2, r4]),
    ?assertEqual([], R1#tdm_rr_state.resources),
    ?assertEqual([], R1#tdm_rr_state.rr),
    R = tdm_rr:reinit(R1),
    ?assertEqual([r1, r2, r4], R#tdm_rr_state.resources),
    ?assertEqual([r1, r2, r4], R#tdm_rr_state.rr),
    unset_env().

next_seq_test() ->
    set_env([r1, r2, r4]),
    R = tdm_rr:from_app(test, round_robin),
    {H1, R1} = tdm_rr:next(R),
    ?assertEqual(r1, H1),
    {H2, R2} = tdm_rr:next(R1),
    ?assertEqual(r2, H2),
    {H3, R3} = tdm_rr:next(R2),
    ?assertEqual(r4, H3),
    {H4, R4} = tdm_rr:next(R3),
    ?assertEqual(r1, H4),
    {H5, R5} = tdm_rr:next(R4),
    ?assertEqual(r2, H5),
    {H6, R6} = tdm_rr:next(R5),
    ?assertEqual(r4, H6),
    {H7, _R7} = tdm_rr:next(R6),
    ?assertEqual(r1, H7),
    unset_env().

next_reinit_test() ->
    set_env([r1, r2, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R = tdm_rr:remove(RR, [r1, r2, r4]),
    {H1, R1} = tdm_rr:next(R),
    ?assertEqual(r1, H1),
    {H2, R2} = tdm_rr:next(R1),
    ?assertEqual(r2, H2),
    {H3, R3} = tdm_rr:next(R2),
    ?assertEqual(r4, H3),
    {H4, _R4} = tdm_rr:next(R3),
    ?assertEqual(r1, H4),
    unset_env().

next_err_test() ->
    set_env([r1, r2, r4]),
    RR = tdm_rr:from_app(test, round_robin),
    R = tdm_rr:remove(RR, [r1, r2, r4]),
    {H1, R1} = tdm_rr:next(R),
    unset_env(),
    ?assertEqual(r1, H1),
    {H2, R2} = tdm_rr:next(R1),
    ?assertEqual(r2, H2),
    {H3, R3} = tdm_rr:next(R2),
    ?assertEqual(r4, H3),
    R3_ = tdm_rr:remove(R3, [r1, r2, r4]),
    {H4, R4} = tdm_rr:next(R3_),
    ?assertEqual(error_no_resources, H4),
    ?assertEqual(R3_, R4).

