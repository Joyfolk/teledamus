
-type rr_list(ResType) :: [ResType].
-record(tdm_rr_state, {resources, rr, init}).
-type rr_state(ResType) :: #tdm_rr_state{resources :: rr_list(ResType), rr :: rr_list(ResType), init :: rr_initfun(ResType)}.
-type rr_initfun(ResType) :: fun(() -> rr_state(ResType)).

-export_type([rr_state/1, rr_initfun/1, rr_list/1]).