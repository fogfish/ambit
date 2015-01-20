%% @description
%%   distributed actors
-module(ambit).

-export([start/0]).
-export([
   spawn/2,
   free/1,
   whereis/1
   % whois/1
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
   ambit_coordinator:call(Name, {spawn, Name, Service}).

%%
%% free service on the cluster
-spec(free/1 :: (any()) -> ok | {error, any()}).

free(Name) ->
   ambit_coordinator:call(Name, {free, Name}).

%%
%% lookup pids associated with given service
-spec(whereis/1 :: (any()) -> [pid()]).

whereis(Name) ->
   ambit_coordinator:call(Name, {whereis, Name}).

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   







