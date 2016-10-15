%% @doc
%%   vnode interface
-module(ambit_vnode).
-include("ambit.hrl").

-export([
   spawn/1,
   send/2,

   foreach/1,
   handoff/2
]).

%%
%% spawn v-node
-spec spawn(ek:vnode()) -> pid().

spawn(Vnode) ->
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(vnode, Addr) of
      undefined ->
         ?DEBUG("ambit [vnode]: spawn ~p", [Vnode]),
         {ok, _} = pts:ensure(vnode, Addr, [Vnode]),
         pns:whereis(vnode, Addr);
      Pid ->
         Pid
   end.


%%
%% send direct message to v-node
-spec send(ek:vnode(), _) -> _.

send(Vnode, Msg) ->
   pts:send(vnode, ek:vnode(addr, Vnode), Msg).


%%
%% apply function to each v-node descriptor
-spec foreach( fun((ek:vnode()) -> ok) ) -> ok.

foreach(Fun) ->
   pts:foreach(
      fun(_, Pid) -> 
         Fun(pipe:ioctl(Pid, vnode))
      end, 
      vnode
   ).

%%
%% Each v-node holds key interval from ring. V-node is either
%%  * primary (aka replica) permanently stores actual object  
%%  * handoff (aka hints collection) temporary keeps history of changes to object  
%% The change of cluster topology impacts on Vnode roles. We can define a following 
%% relations between cluster peers depending on v-nodes they operate:
%%  * undefined - given peer do not have relation v-node
%%  * replica   - given peer holds replica(s) of v-node
%%  * handoff   - given peer exclusively responsible for v-node 
%%
-spec handoff(node(), ek:vnode()) -> undefined | {replica, ek:vnode()} | {handoff, ek:vnode()}.

handoff(Peer, Vnode) ->
   Type = ek:vnode(type, Vnode),
   Ring = ek:vnode(ring, Vnode),
   Addr = ek:vnode(addr, Vnode),
   Node = ek:vnode(node, Vnode),
   Layout = ek:successors(Ring, Addr),
   handoff(Type, Node =:= Peer, lists:keyfind(Peer, 4, Layout), lists:keyfind(Node, 4, Layout)).

% peer is not part of v-node successor list
handoff(      _, false, false,     _) -> undefined;
% peer is responsible for v-node, handoff it
handoff(primary, false, Vnode, false) -> {handoff, Vnode};
% peer manages v-node replica 
handoff(primary, false, Vnode,     _) -> {replica, Vnode};
% peer manages v-node replica but v-node holds hints
handoff(handoff, true,  Vnode,     _) -> {handoff, Vnode};
% other
handoff(handoff,    _,      _,     _) -> undefined.


