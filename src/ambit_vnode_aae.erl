%% @description
%%   active anti-entropy
-module(ambit_vnode_aae).
-behaviour(aae).

-include("ambit.hrl").

-export([
   new/1
  ,peers/1
  ,session/2
  ,handshake/3
  ,chunks/1
  ,snapshot/1
  ,snapshot/2
  ,diff/4
]).


%%
%% initialize anti-entropy leader state
%% return identity of itself and new state data 
-spec(new/1 :: (list()) -> {ek:vnode(), ek:vnode()}).

new([{_, Addr, Key, Peer}=Vnode]) ->
   ok = pns:register(vnode, {ae, Addr}, self()), 
   {{ae, Addr, Key, Peer}, Vnode}.

%%
%% return list of candidate peers (potential successors)
-spec(peers/1 :: (ek:vnode()) -> {[ek:vnode()], ek:vnode()}).

peers({_, Self, _, _}=State) ->
   List = [{ae, Addr, Node, Peer} || 
         {primary, Addr, Node, Peer} <- ek:successors(ambit, Self),
         erlang:node(Peer) =/= erlang:node()],
   {List, State}.      

%%
%% initialize new anti-entropy session
-spec(session/2 :: (ek:vnode(), ek:vnode()) -> ek:vnode()).

session(_Peer, State) ->
   State.

%%
%% connect session to selected remote peer using pipe protocol
-spec(handshake/3 :: (ek:vnode(), any(), ek:vnode()) -> ek:vnode()).

handshake(Peer, Req, State) ->
   ambit_peer:send(Peer, Req),
   State.

%%
%% return chunk(s) to apply anti-entropy
-spec(chunks/1 :: (ek:vnode()) -> {list(), ek:vnode()}).

chunks(State) ->
   {[], State}.  

%%
%% make snapshot, returns key/val stream 
-spec(snapshot/1 :: (ek:vnode()) -> {datum:stream(), ek:vnode()}).
-spec(snapshot/2 :: (list(), ek:vnode()) -> {datum:stream(), ek:vnode()}).

snapshot({_, Addr, _, _}=State) ->
   Stream = stream:build(pns:lookup(Addr, '_')),
   {Stream, State}.

snapshot(_, State) ->
   snapshot(State).

%%
%% remote peer diff, called for each key, order is arbitrary 
%%
-spec(diff/4 :: (ek:vnode(), binary(), pid(), ek:vnode()) -> ok).

diff(Peer, Name, _Pid, {_, Addr, _, _}) ->
   Service = ambit_actor:service(Addr, Name),
   Vnode   = erlang:setelement(1, Peer, primary),
   Tx = ambit_peer:cast(Vnode, {spawn, Name, Service}),
   receive
      {Tx, _} ->
         ok
   after ?CONFIG_TIMEOUT_REQ ->
         ok
   end.

