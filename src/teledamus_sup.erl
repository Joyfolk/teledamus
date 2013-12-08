-module(teledamus_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(Args) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

init(Args) ->
	{ok, { {one_for_one, 5, 10}, [
		  {teledamus_srv, {teledamus_srv, start_link, [Args]}, permanent, 5000, worker, [teledamus_srv]},
      {gen_event, {gen_event, start_link, [{local, cassandra_events}]}, permanent, 5000, worker, [gen_event]}
	  ]}
	}.

