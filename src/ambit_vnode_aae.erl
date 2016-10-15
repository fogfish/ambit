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
%%   active anti-entropy
-module(ambit_vnode_aae).
-behaviour(aae).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

-export([
   new/1
  ,free/2
  ,peers/1
  ,session/2
  ,handshake/3
  ,snapshot/1
  ,diff/3
]).


%%
%% initialize anti-entropy leader state
%% return identity of itself and new state data 
-spec new(list()) -> {ek:vnode(), ek:vnode()}.

new([Vnode]) ->
   Addr = ek:vnode(addr, Vnode),
   pipe:ioctl(pns:whereis(vnode, Addr), {aae, self()}),
   {Vnode, Vnode}. 

%%
%% terminate anti-entropy state either session or leader
-spec free(any(), ek:vnode()) -> ok.

free(_, _) ->
   ok.


%%
%% return list of candidate peers (potential successors)
-spec peers(ek:vnode()) -> {[ek:vnode()], ek:vnode()}.

peers(State) ->
   Ring = ek:vnode(ring, State),
   Addr = ek:vnode(addr, State),
   List = [X || X <- ek:successors(Ring, Addr),
               erlang:node( ek:vnode(peer, X) ) =/= erlang:node()
   ],
   {List, State}.      

%%
%% initialize new anti-entropy session
-spec session(ek:vnode(), ek:vnode()) -> ek:vnode().

session(_Peer, State) ->
   State.

%%
%% connect session to selected remote peer using pipe protocol
-spec handshake(ek:vnode(), any(), ek:vnode()) -> ek:vnode().

handshake(Peer, Req, State) ->
   ambit_peer:send(Peer, {aae, Req}),
   State.

%%
%% make snapshot, returns key/val stream 
-spec snapshot(ek:vnode()) -> {datum:stream(), ek:vnode()}.

snapshot(State) ->
   Addr   = ek:vnode(addr, State),
   Stream = stream:build([Name || {Name, _Pid} <- pns:lookup(Addr, '_')]),
   {Stream, State}.

%%
%% remote peer diff, called for each key, order is arbitrary 
%%
-spec diff(ek:vnode(), binary(), ek:vnode()) -> ok.

diff(Peer, Key, State) ->
   Addr    = ek:vnode(addr, State),
   %% (?) Why do we force node type to primary, it has to be either primary 
   %%     or handoff depends on the type
   % Handoff = erlang:setelement(1, Peer, primary),
   ambit_actor_bridge:sync(pns:whereis(Addr, Key), Peer),
   ok.

   % case ambit_actor:service(Addr, Name) of
   %    %% service died during aae session
   %    undefined ->
   %       ok;      

   %    %% sync removed service
   %    #entity{val = undefined} = Entity ->
   %       ?DEBUG("ambit [aae]: (-) ~p", [Name]),
   %       Tx = ambit_peer:cast(Handoff, {'$ambitz', free, Entity}),
   %       receive
   %          {Tx, _} ->
   %             ok
   %       after ?CONFIG_TIMEOUT_REQ ->
   %             ok
   %       end;

   %    %% sync existed service
   %    Entity ->
   %       ?DEBUG("ambit [aae]: (+) ~p", [Name]),
   %       Tx = ambit_peer:cast(Handoff, {'$ambitz', spawn, Entity}),
   %       receive
   %          {Tx, _} ->
   %             ok
   %       after ?CONFIG_TIMEOUT_REQ ->
   %             ok
   %       end
   % end.

