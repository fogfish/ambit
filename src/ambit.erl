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
   % try
   %    pipe:call(pq:pid(Ref), 
   %       {any, Name, fun(Vnode) -> ambit_peer:cast(Vnode, {spawn, Vnode, self(), Name, Service}) end})
   % after
   %    pq:release(Ref)
   % end.

%%
%% free service on the cluster
-spec(free/1 :: (any()) -> ok | {error, any()}).

free(Name) ->
   Ref = pq:lease(ambit_req),
   try
      pipe:call(pq:pid(Ref), 
         {all, Name, fun(Vnode) -> ambit_peer:cast(Vnode, {free, Vnode, self(), Name}) end})
   after
      pq:release(Ref)
   end.

%%
%% lookup pids associated with given service
-spec(whereis/1 :: (any()) -> [pid()]).

whereis(Name) ->
   Ref = pq:lease(ambit_req),
   try
      {ok, List} = pipe:call(pq:pid(Ref), 
         {any, Name, fun(Vnode) -> ambit_peer:cast(Vnode, {whereis, self(), Name}) end}),
      [X || X <- List, is_pid(X)]
   after
      pq:release(Ref)
   end.

% %%
% %% lookup coordinator node for given key
% -spec(whois/1 :: (any()) -> ek:vnode()).

% whois(Key) ->
%    List = ek:successors(ambit, Key),
%    head(
%       lists:dropwhile(
%          fun({X, _, _, _}) -> X =/= primary end, 
%          List
%       ),
%       List
%    ).

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

% %%
% %% may be head
% head([Head | _], _) ->
%    Head;
% head(_, [Head | _]) ->
%    Head.







