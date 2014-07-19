-module(tdm_stmt_cache).
%% @doc
%% Prepared statement caching facilities
%% @end

-include_lib("teledamus.hrl").

%% API
-export([init/0, to_cache/2, from_cache/1, cache/3, cache_async/3, cache_async_multple/3]).

-define(STMT_CACHE, teledamus_stmt_cache).

%% @doc
%% Initializes prepared statement cache
%% @end
-spec init() -> teledamus_stmt_cache.
init() ->
    case ets:info(?STMT_CACHE) of
        undefined -> ets:new(?STMT_CACHE, [named_table, set, public, {write_concurrency, false}, {read_concurrency, true}]);
        _ -> ?STMT_CACHE
    end.

%% @doc
%% Put prepared statement id to cache for given query
%%
%% Key - {host, port, query text} tuple
%% PreparedStmtId - compiled prepared statement id
%% @end
-spec to_cache(Key :: {Host :: string(), Port :: pos_integer(), Query :: teledamus:query_text()}, PreparedStmtId :: teledamus:prepared_query_id()) -> true.
to_cache(Key, PreparedStmtId) ->
    ets:insert(?STMT_CACHE, {Key, PreparedStmtId}).

%% @doc
%% Get prepared statement id to cache for given query
%%
%% Key - {host, port, query text} tuple
%% Result - compiled prepared statement id or not_found
%% @end
-spec from_cache(Key :: {Host :: string(), Port :: pos_integer(), Query :: teledamus:query_text()}) -> {ok, teledamus:prepared_query_id()} | not_found.
from_cache(Key) ->
    case ets:lookup(?STMT_CACHE, Key) of
        [{_, Id}] ->
            {ok, Id};
        [] ->
            not_found
    end.

%% @doc
%% Get prepared statement id to cache for given query, compile it if not cached yet
%%
%% Query - cassandra query string
%% Con - cassandra connection
%% Timeout - compile operation timeout
%% Result - compiled {ok, prepared statement id} or error
%% @end
-spec cache(Query :: teledamus:query_text(), Con :: teledamus:connection(), Timeout :: timeout()) -> {ok, teledamus:prepared_query_id()} | teledamus:error().
cache(Query, Con, Timeout) ->
    #tdm_connection{host = Host, port = Port} = Con,
    case ets:lookup(?STMT_CACHE, {Host, Port, Query}) of
        [{_, Id}] ->
            {ok, Id};
        [] ->
            case tdm_connection:prepare_query(Con, Query, Timeout) of
                {PreparedStmtId, _, _} ->
                    ets:insert(?STMT_CACHE, {{Host, Port, Query}, PreparedStmtId}),
                    {ok, PreparedStmtId};
                Err = #tdm_error{} ->
                    Err
            end
    end.

-spec cache_async(Query :: teledamus:query_text(), Con ::  teledamus:connection(), Fun :: fun((Res :: term()) -> any())) -> ok | {error, Reason :: term()}.
cache_async(Query, Con, ReplyTo) ->
    #tdm_connection{host = Host, port = Port} = Con,
    case ets:lookup(?STMT_CACHE, {Host, Port, Query}) of
        [{_, Id}] ->
            ReplyTo({ok, Id});
        [] ->
            tdm_connection:prepare_query_async(Con, Query, fun(Res) ->
                case Res of
                    {PreparedStmtId, _, _} ->
                        ets:insert(?STMT_CACHE, {{Host, Port, Query}, PreparedStmtId}),
                        ReplyTo({ok, PreparedStmtId});
                    Err = #tdm_error{} ->
                        ReplyTo(Err)
                end
            end)
    end,
    ok.

-spec cache_async_multple(Queries :: [teledamus:query_text()], Con :: teledamus:connection(), Fun :: fun((Res :: term()) -> any())) -> ok | {error, Reason :: term()}.
cache_async_multple(ToCache, Con, ReplyTo) ->
    cache_async_multple(ToCache, Con, ReplyTo, dict:new()).

cache_async_multple([], _Con, ReplyTo, Dict) ->
    ReplyTo(Dict);
cache_async_multple([Q | T], Con, ReplyTo, Dict) ->
    #tdm_connection{host = Host, port = Port} = Con,
    case dict:find(Q, Dict) of
        {ok, _Id} ->
            cache_async_multple(T, Con, ReplyTo, Dict);
        _ ->
            cache_async(Q, Con, fun(Res) ->
                case Res of
                    {PreparedStmtId, _, _} ->
                        ets:insert(?STMT_CACHE, {{Host, Port, Q}, PreparedStmtId}),
                        cache_async_multple(T, Con, ReplyTo, dict:store(Q, PreparedStmtId, Dict));
                    Err = #tdm_error{} ->
                        ReplyTo(Err)
                end
            end)
    end.
