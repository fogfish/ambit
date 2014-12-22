%% @description
%%   distributed actors
-module(ambit).

-export([start/0]).
-export([
   spawn/2,
   free/1,
   whereis/1,
   whois/1
]).

%%
%% RnD application start
-ifdef(CONFIG_DEBUG).
start() ->
   applib:boot(?MODULE, []),
   lager:set_loglevel(lager_console_backend, debug).
-else.
start() ->
   applib:boot(?MODULE, []).
-endif.


%%
%% spawn service on the cluster
-spec(spawn/2 :: (any(), any()) -> {ok, [pid()]} | {error, any()}).

spawn(Name, Service) ->
   Ref = pq:lease(ambit_req),
   try
      pipe:call(pq:pid(Ref), {init, Name, Service})
   after
      pq:release(Ref)
   end.

%%
%% free service on the cluster
-spec(free/1 :: (any()) -> ok | {error, any()}).

free(Name) ->
   Ref = pq:lease(ambit_req),
   try
      pipe:call(pq:pid(Ref), {free, Name})
   after
      pq:release(Ref)
   end.

%%
%% lookup pids associated with given service
-spec(whereis/1 :: (any()) -> [pid()]).

whereis(Name) ->
   Ref = pq:lease(ambit_req),
   try
      pipe:call(pq:pid(Ref), {whereis, Name})
   after
      pq:release(Ref)
   end.

%%
%% lookup vnodes manages given service
-spec(whois/1 :: (any()) -> [ek:vnode()]).

whois(Name) ->
   ek:successors(ambit, Name).




