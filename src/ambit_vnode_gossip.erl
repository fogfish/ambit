-module(ambit_vnode_gossip).
-behaviour(gossip).

-include("ambit.hrl").

-export([
   init/1
  ,peer/1
  ,handshake/3
  ,snapshot/1
  ,diff/4
]).

%%
%%
init([{_, Addr, Key, Peer}=Vnode]) ->
   ok = pns:register(vnode, {gossip, Addr}, self()), 
   {{gossip, Addr, Key, Peer}, Vnode}.


%%
%% return list of candidate peers 
%%
-spec(peer/1 :: (any()) -> [pid()]).

peer({_, Self, _, _}) ->
   %% return list of potential successors
   [{gossip, Addr, Node, Peer} || 
         {primary, Addr, Node, Peer} <- ek:successors(ambit, Self),
         erlang:node(Peer) =/= erlang:node()].      

%%
%% connect gossip session to selected remote peer using pipe protocol
%% 
-spec(handshake/3 :: (atom(), any(), any()) -> ok).

handshake(Req, Peer, _Vnode) ->
   ambit_peer:send(Peer, Req).

%%
%% make snapshot, returns key/val stream 
%%
-spec(snapshot/1 :: (any()) -> datum:stream()).

snapshot({_, Addr, _, _}) ->
   stream:build(pns:lookup(Addr, '_')).

%%
%% remote peer diff, called for each key, order is arbitrary 
%%
-spec(diff/4 :: (any(), any(), any(), any()) -> datum:stream()).

diff(Peer, Name, _Pid, {_, Addr, _, _}) ->
   Service = ambit_actor:service(Addr, Name),
   Vnode   = erlang:setelement(1, Peer, primary),
   Tx = ambit_peer:cast(Vnode, {spawn, Name, Service}),
   receive
      {Tx, {ok, _}} ->
         ok
   after ?CONFIG_TIMEOUT_REQ ->
         ok
   end.

