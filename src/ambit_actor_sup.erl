%% @description
%%   supervisor for actor process
-module(ambit_actor_sup).
-behaviour(supervisor).

-export([
   start_link/4
  ,init/1
   %% api
  ,start_child/2
]).

%%
-define(CHILD(I),                  {service,                I, permanent, 5000, worker, dynamic}).
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000,   Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000,   Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000,   Type, dynamic}).

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------

start_link(Ns, Name, Vnode, Service) ->
   supervisor:start_link(?MODULE, [Ns, Name, Vnode, Service]).
   
init([Ns, Name, Vnode, Service]) ->   
   {ok,
      {
         {one_for_all, 0, 1},
         [
            ?CHILD(worker, actor, ambit_actor, [self(), Ns, Name, Vnode, Service])
         ]
      }
   }.

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------

%%
%%
start_child(Sup, Service) ->
   supervisor:start_child(Sup, ?CHILD(Service)).

