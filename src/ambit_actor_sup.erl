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

start_link(Addr, Name, State, Service) ->
   supervisor:start_link(?MODULE, [State, Addr, Name, Service]).
   
init([State, Addr, Name, Service]) ->   
   {ok,
      {
         {one_for_all, 0, 1},
         [
            ?CHILD(worker, actor, ambit_actor, [self(), State, Addr, Name, Service])
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

