%% @description
%%   virtual node supervisor
-module(ambit_vnode_sup).
-behaviour(supervisor).

-include("ambit.hrl").

-export([
   start_link/3, init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------

start_link(vnode, Addr, Vnode) ->
   supervisor:start_link(?MODULE, [Addr, Vnode]).
   
init([Addr, Vnode]) ->
   {ok,
      {
         {one_for_one, 0, 1},
         [
				%% @todo: we need to reserve token due to concurrency
            %%        reserve token at sup and re-bind it to vnode pns:swap
            ?CHILD(worker, ambit_vnode, [self(), Vnode])

            %% require before anything
           ,?CHILD(supervisor, pts, [Addr, ?HEAP_ACTOR])
           
           ,?CHILD(worker, ambit_vnode_spawn, [Vnode])
            %% @todo: make gossip configurable (enable/disable + timeouts)
            %% @todo: disable gossip (repair for hand-off)
           ,?CHILD(worker, aae,  [[
               {session,  ?CONFIG_AAE_TIMEOUT}
              ,{timeout,  ?CONFIG_TIMEOUT_REQ}
              ,{capacity, ?CONFIG_AAE_CAPACITY}
              ,{strategy, aae}
              ,{adapter,  {ambit_vnode_aae, [Vnode]}}
            ]])
         ]
      }
   }.
