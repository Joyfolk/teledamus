

-record(tdm_query_params, {
    consistency_level = quorum :: teledamus:consistency_level(),
    serial_consistency = undefined :: teledamus:consistency_level() | undefined,
    timestamp = undefined :: non_neg_integer() | undefined,
%%     skip_metadata = false :: boolean(),
    trace_query = false :: boolean(),
%%     page_size = undefined :: pos_integer() | undefined,
%%     paging_state = undefined :: binary() | undefined
}).


-record(tdm_statement, {
    query :: teledamus:query() | teledamus:batch_query(),
    consistency_level = quorum :: teledamus:consistency_level(),
    timestamp = undefined :: non_neg_integer() | undefined,
    serial_consistency_level = undefined :: teledamus:serial_consistency_level() | undefined,
    trace_query = false :: boolean()

%%  todo:   retry_policy = crash_immediately
}).


-record(tdm_node_info, {
    host = "localhost" :: inet:ip_address() | inet:hostname(),
    port = 9042 :: inet:port_number(),
    opts = [] :: [gen_tcp:connect_option() | ssl:connect_option()],
    compression :: atom(),
    protocol_version :: teledamus:protocol_version() | undefined,
    credentials :: teledamus:credentials()
}).