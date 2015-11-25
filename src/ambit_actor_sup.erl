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
-module(ambit_actor_sup).
-behaviour(supervisor).

-export([
   start_link/3
  ,init/1
   %% api
  ,init_service/2
  ,free_service/1
]).

%%
-define(CHILD(I),                  {service,                I, permanent, infinity, worker, dynamic}).
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, infinity,   Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, infinity,   Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, infinity,   Type, dynamic}).

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
         {one_for_all, 0, 1},
         [
            ?CHILD(worker, actor, ambit_actor, [self(), Addr, Key, Vnode])
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
init_service(Sup, Service) ->
   supervisor:start_child(Sup, ?CHILD(Service)).


free_service(Sup) ->
   supervisor:terminate_child(Sup, service),
   supervisor:delete_child(Sup, service).
   




