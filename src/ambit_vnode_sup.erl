%% @description
%%   virtual node supervisor
-module(ambit_vnode_sup).
-behaviour(supervisor).

-include("ambit.hrl").

-export([
   start_link/2, init/1
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

start_link(vnode, Addr) ->
   supervisor:start_link(?MODULE, [Addr]).
   
init([Addr]) ->
   %% discover vnode definition for given address
   Vnode = hd(ek:successors(ambit, Addr)),
   {ok,
      {
         {one_for_one, 0, 1},
         [
            %% require before anything
            ?CHILD(supervisor, pts, [Addr, ?HEAP_ACTOR])
           
           ,?CHILD(worker, ambit_vnode, [self(), Vnode])
           ,?CHILD(worker, ambit_vnode_spawn, [Vnode])
            %% @todo: make gossip configurable (enable/disable + timeouts)
           ,?CHILD(worker, gossip,  [[
               {session, ?CONFIG_TIMEOUT_GOSSIP}
              ,{timeout, ?CONFIG_TIMEOUT_REQ}
              ,{adapter, {ambit_vnode_gossip, [Vnode]}}
            ]])
         ]
      }
   }.
