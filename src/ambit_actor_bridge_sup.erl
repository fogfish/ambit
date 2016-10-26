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
%%   Supervisor for actor process(es). It manages both actual actor and controlling processes.
%%   The supervisor is required to spawn "arbitrary" client process and integrate it to common
%%   name space. The global actor factory do not work due to actor specific "service" definition  
-module(ambit_actor_bridge_sup).
-behaviour(supervisor).

-export([
   start_link/3
  ,init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, transient, infinity,   Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, transient, infinity,   Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, transient, infinity,   Type, dynamic}).

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------

start_link(Addr, Key, Vnode) ->
   supervisor:start_link(?MODULE, [Addr, Key, Vnode]).
   
init([Addr, Key, Vnode]) ->   
   {ok,
      {
         {one_for_one, 1000000, 1},
         [
            ?CHILD(worker, bridge, ambit_actor_bridge, [self(), Addr, Key, Vnode])
         ]
      }
   }.

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------
