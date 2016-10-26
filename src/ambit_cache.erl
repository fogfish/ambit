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
%%   example cache service used for RnD and demo purposes
-module(ambit_cache).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/1
  ,start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode) ->
   start_link(Vnode, lwwreg).

start_link(Vnode, Type) ->
   pipe:start_link(?MODULE, [Vnode, Type], []).

init([_Vnode, Type]) ->
   ?DEBUG("ambit [cache]: init ~p", [_Vnode]),
   {ok, handle, #{val => crdts:new(Type)}}.

free(_Reason, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({put, _, ValA}, Tx, #{val := ValB} = State) ->
   ValC = crdts:join(ValA, ValB),
   pipe:ack(Tx, {ok, ValC}),
   {next_state, handle, State#{val => ValC}};

handle({get, _}, Tx, #{val := Val} = State) ->
   pipe:ack(Tx, {ok, Val}),
   {next_state, handle, State}.
