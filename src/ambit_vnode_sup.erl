%%
%%   Copyright 2014 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   virtual node supervisor
-module(ambit_vnode_sup).
-behaviour(supervisor).

-include("ambit.hrl").

-export([
   start_link/3, init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, infinity, Type, []}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, infinity, Type, []}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, infinity, Type, []}).

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
         {one_for_all, 0, 1},
         [
            %%
            ?CHILD(worker, ambit_vnode, [self(), Vnode])

            %% required before anything else
           ,?CHILD(supervisor, pts, [Addr, ?HEAP_ACTOR])
           
            %% spawn service
           ,?CHILD(worker, ambit_vnode_spawn, [Vnode])

            %% @todo: make gossip configurable (enable/disable + timeouts)
            %% @todo: disable gossip (repair for hand-off)
           ,?CHILD(worker, aae,  [[
               {session,  opts:val(aae, ?CONFIG_AAE_TIMEOUT, ambit)}
              ,{timeout,  ?CONFIG_TIMEOUT_REQ}
              ,{strategy, aae}
              ,{adapter,  {ambit_vnode_aae, [Vnode]}}
            ]])
         ]
      }
   }.
