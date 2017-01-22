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
%% @doc
%%   ambit actor interface
-module(ambit_actor).
-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

-export([
   spawn/2,
   service/2,
   call/3,
   sync/3,
   handoff/3
]).

%%
%% spawn actor (bridge to idle state)
-spec spawn(ek:vnode(), _) -> pid().

spawn(Vnode, Key) ->
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(Addr, Key) of
      undefined ->
         ?DEBUG("ambit [actor]: ~p spawn ~p", [Vnode, Key]),
         {ok, _} = pts:ensure(Addr, Key, [Vnode]),
         pns:whereis(Addr, Key);
      Pid ->
         Pid
   end.      


%%
%% return service / entity specification
-spec service(ek:vnode(), _) -> #entity{} | undefined. 

service(Vnode, Key) ->
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(Addr, Key) of
      undefined ->
         undefined;
      Pid       ->
         pipe:ioctl(Pid, service)
   end.


%%
%% request actor service
-spec call(ek:vnode(), _, _) -> {ok, _} | {error, _}.

call(Vnode, Key, Req) ->
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(Addr, Key) of
      undefined ->
         {error, noroute};
      Pid ->
         pipe:call(Pid, Req)
   end.

%%
%% reconcile actor with peer
-spec sync(ek:vnode(), _, ek:vnode()) -> ok.

sync(Vnode, Key, Peer) ->
   ambit_actor:call(Vnode, Key, {sync, Peer}).

%%
%% handoff actor to peer
-spec handoff(ek:vnode(), _, ek:vnode()) -> ok.

handoff(Vnode, Key, Peer) ->
   ambit_actor:call(Vnode, Key, {handoff, Peer}).
