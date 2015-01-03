%% @description
%%   virtual node - anti-entropy
-module(ambit_vnode_ae).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,handshake/3
  ,snapshot/3
]).


%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode, Sup) ->
   pipe:start_link(?MODULE, [Vnode, Sup], []).

init([{_, Addr, Node, Peer}=Vnode, Sup]) ->
   ?DEBUG("ambit [ae]: init ~p", [Vnode]),
   ok = pns:register(vnode, {ae, Addr}, self()),   
   {ok, idle, 
      #{
         vnode => {ae, Addr, Node, Peer},
         sup   => Sup,
         t     => tempus:timer(?CONFIG_CYCLE_AE, reconcile)
      }
   }.

free(_, #{}) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle(reconcile, _, #{vnode := Vnode}=State) ->
   erlang:send(self(), handshake),
   {next_state, handshake, State#{peer => siblings(Vnode)}};

idle({handshake, Peer}, Tx, #{vnode := Vnode}=State) ->
   ?DEBUG("ambit [ae]: ~p -handshake ~p", [Vnode, Peer]),
   pipe:ack(Tx, handshake),
   erlang:send(self(), snapshot),
   {next_state, snapshot, State#{peer => Peer}}.

%%
%% 
handshake(handshake, _, #{peer := [], t := T}=State) ->
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};
   
handshake(handshake, _, #{vnode := Vnode, peer := [Peer|_]}=State) ->
   ambit_peer:cast(Peer, {handshake, Vnode}),
   % @todo: timeout
   {next_state, handshake, State};

handshake({_, handshake}, _, #{vnode := Vnode, peer := [Peer|_]}=State) ->
   ?DEBUG("ambit [ae]: ~p +handshake ~p", [Vnode, Peer]),
   erlang:send(self(), snapshot),
   {next_state, snapshot, State#{peer => Peer}};

handshake({_, {error, _}}, _, #{peer := [_|Peers]}=State) ->
   erlang:send(self(), handshake),
   {next_state, snapshot, State#{peer => Peers}};

handshake({handshake, _}, Tx, State) ->
   pipe:ack(Tx, {error, busy}),
   {next_state, handshake, State};

handshake(_, _, State) ->
   {next_state, handshake, State}.


%%
%% 
snapshot(snapshot, _, #{peer := {_, Addr, _, _}, sup := Sup, t := T}=State) ->
   % ?DEBUG("ambit [ae]: ~b reconcile ~p", [Addr, VNode]),
   Keys = [X || 
            {X, _, _, _} <- supervisor:which_children(Sup),
            belong(X, Addr) =/= false],
   Tree = htree:build(Keys),
   io:format("reconcile ~p~n", [htree:list(Tree)]),
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

snapshot({handshake, _}, Tx, #{t := T}=State) ->
   pipe:ack(Tx, {error, busy}),
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

snapshot(_, _, #{t := T}=State) ->
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}}.


% idle({reconcile, {primary, Addr, _Peer, Pid} = VNode}, Tx, #{sup := Sup}=State) ->
%    _ = erlang:monitor(process, Pid),

% %%
% %%
% reconcile(_, _Tx, State) ->
%    {next_state, reconcile, State}.

% handle(_, _, State) ->
%    {next_state, handle, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% return list of siblings
siblings({_, Addr, _, _}) ->
   case siblings1(ek:successors(ambit, Addr)) ++ siblings1(ek:predecessors(ambit, Addr + 1)) of
      [] ->
         [];
      L  ->
         N = random:uniform(length(L)),
         {A, B} = lists:split(N, L),
         B ++ A
   end.

siblings1(List) ->
   [{ae, Addr, Node, Peer} || 
      {primary, Addr, Node, Peer} <- List,
      erlang:node(Peer) =/= erlang:node()].

%%
%% check if key managed by VNode
belong(Key, Addr) ->
   lists:keyfind(Addr, 2, ek:successors(ambit, Key)).






