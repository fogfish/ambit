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
%%   echo service (used for RnD)
-module(ambit_echo).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).
%% ambit callback
-export([
   process/1,
   handoff/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode) ->
   pipe:start_link(?MODULE, [Vnode], []).

init([Vnode]) ->
   ?DEBUG("ambit [echo]: init ~p", [Vnode]),
   erlang:process_flag(trap_exit, true),
   {ok, handle, Vnode}.

free(_, Vnode) ->
   ?DEBUG("ambit [echo]: free ~p", [Vnode]),
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% ambit
%%%
%%%----------------------------------------------------------------------------   

process(Root) ->
   {ok, Root}.

handoff(Root, Vnode) ->
   pipe:call(Root, {handoff, Vnode}).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({handoff, Vnode}, Tx, {primary, _, _, _}=State) ->
   ?ERROR("ambit [echo]: data transfer to ~p", [Vnode]),
   pipe:ack(Tx, ok),
   {next_state, handle, State};

handle({handoff, Vnode}, Tx, {handoff, _, _, _}=State) ->
   ?ERROR("ambit [echo]: hint gossip with ~p", [Vnode]),
   pipe:ack(Tx, ok),
   {next_state, handle, State};

handle(Msg, Tx, State) ->
   pipe:ack(Tx, Msg),
   {next_state, handle, State}.


