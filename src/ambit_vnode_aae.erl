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
-spec(new/1 :: (list()) -> {ek:vnode(), ek:vnode()}).

new([{_, Addr, Key, Peer}=Vnode]) ->
   ok = pns:register(vnode_sys, {aae, Addr}, self()), 
   {{ae, Addr, Key, Peer}, Vnode}. 

%%
%% terminate anti-entropy state either session or leader
-spec(free/2 :: (any(), ek:vnode()) -> ok).

free(_, _) ->
   ok.


%%
%% return list of candidate peers (potential successors)
-spec(peers/1 :: (ek:vnode()) -> {[ek:vnode()], ek:vnode()}).

peers({_, Self, _, _}=State) ->
   List = [{aae, Addr, Node, Peer} ||
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
%% make snapshot, returns key/val stream 
-spec(snapshot/1 :: (ek:vnode()) -> {datum:stream(), ek:vnode()}).

snapshot({_, Addr, _, _}=State) ->
   Stream = stream:build([Name || {Name, _Pid} <- pns:lookup(Addr, '_')]),
   {Stream, State}.

%%
%% remote peer diff, called for each key, order is arbitrary 
%%
-spec(diff/3 :: (ek:vnode(), binary(), ek:vnode()) -> ok).

diff(Peer, Name, {_, Addr, _, _}) ->
   Handoff = erlang:setelement(1, Peer, primary),
   case ambit_actor:service(Addr, Name) of
      %% service died during aae session
      undefined ->
         ok;      

      %% sync removed service
      #entity{val = undefined} = Entity ->
         ?DEBUG("ambit [aae]: (-) ~p", [Name]),
         Tx = ambit_peer:cast(Handoff, {remove, Entity}),
         receive
            {Tx, _} ->
               ok
         after ?CONFIG_TIMEOUT_REQ ->
               ok
         end;

      %% sync existed service
      Entity ->
         ?DEBUG("ambit [aae]: (+) ~p", [Name]),
         Tx = ambit_peer:cast(Handoff, {create, Entity}),
         receive
            {Tx, _} ->
               ok
         after ?CONFIG_TIMEOUT_REQ ->
               ok
         end
   end.

