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
  ,exchange/3
  ,transfer/3
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
         sup   => Sup %,
         % t     => tempus:timer(?CONFIG_CYCLE_AE, reconcile)
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

idle({handshake, Peer}, _, #{vnode := Vnode}=State) ->
   ?DEBUG("ambit [ae]: ~p -handshake ~p", [Vnode, Peer]),
   ambit_peer:send(Peer, {Vnode, handshake}),
   erlang:send(self(), slave),   
   {next_state, snapshot, State#{peer => Peer}};

idle(_, _, State) ->
   {next_state, idle, State}.


%%
%%
handshake(handshake, _, #{peer := [], t := T}=State) ->
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

handshake(handshake, _, #{vnode := Vnode, peer := [Peer|_]}=State) ->
   ambit_peer:cast(Peer, {handshake, Vnode}),
   % @todo: timeout
   {next_state, handshake, State};

handshake({Peer, handshake}, _, #{vnode := Vnode, peer := [Peer|_]}=State) ->
   ?DEBUG("ambit [ae]: ~p +handshake ~p", [Vnode, Peer]),
   erlang:send(self(), master),
   {next_state, snapshot, State#{peer => Peer}};

handshake({Peer, busy}, _, #{peer := [Peer|Peers]}=State) ->
   erlang:send(self(), handshake),
   {next_state, handshake, State#{peer => Peers}};

handshake({handshake, Peer}, _, #{vnode := Vnode}=State) ->
   ambit_peer:send(Peer, {Vnode, busy}),
   {next_state, handshake, State};

handshake(_, _, State) ->
   {next_state, handshake, State}.


%%
%% 
snapshot(master, _, #{peer := {_, Addr, _, _}, sup := Sup}=State) ->
   %% initiate reconciliation as master
   erlang:send(self(), init),
   {next_state, exchange, State#{l => 0, tree => htbuild(Addr, Sup)}};

snapshot(slave,  _, #{peer := {_, Addr, _, _}, sup := Sup}=State) ->
   %% initiate reconciliation as slave
   {next_state, exchange, State#{l => 0, tree => htbuild(Addr, Sup)}};

snapshot({handshake, Peer}, _, #{vnode := Vnode}=State) ->
   ambit_peer:send(Peer, {Vnode, busy}),
   {next_state, snapshot, State};

snapshot(_, _, State) ->
   {next_state, snapshot, State}.



%%
%%
exchange(init, _, #{peer := Peer, l := L, tree := Tree0}=State) ->
   case htree:hash(L, Tree0) of
      undefined ->
         ambit_peer:send(Peer, htree:hash(Tree0)),
         {next_state, exchange, State};

      HashB     ->
         ambit_peer:send(Peer, HashB),
         {next_state, exchange, State}
   end;

exchange({hash, -1, _}=HashA, _, #{peer := Peer, tree := Tree0, sup := Sup}=State) ->
   HashB = htree:hash(Tree0),
   ambit_peer:send(Peer, HashB),   
   Diff = htree:diff(HashA, HashB),
   Tree = htree:evict(Diff, Tree0),
   erlang:send(self(), transfer),   
   {next_state, transfer, State#{processes => q:new([{Name, X} || {Name, X, _, _} <- supervisor:which_children(Sup), htree:lookup(Name, Tree) =/= undefined])}};

exchange({hash,  _, _}=HashA, _, #{peer := Peer, l := L, tree := Tree0}=State) ->
   case htree:hash(L, Tree0) of
      undefined ->
         ambit_peer:send(Peer, htree:hash(Tree0)),
         {next_state, exchange, State};

      HashB     ->
         ambit_peer:send(Peer, HashB),
         Diff = htree:diff(HashA, HashB),
         Tree = htree:evict(Diff, Tree0),
         {next_state, exchange, State#{l => L + 1, tree => Tree}}          
   end;

exchange({handshake, Peer}, _, #{vnode := Vnode}=State) ->
   ambit_peer:send(Peer, {Vnode, busy}),
   {next_state, exchange, State};

exchange(_, _, State) ->
   {next_state, exchange, State}.

%%
%%
transfer(transfer, _, #{peer := Peer, processes := {}, t := T}=State) ->
   ?NOTICE("ambit [ae]: ~p completed", [Peer]),
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

transfer(transfer, _, #{peer := Peer, processes := Processes}=State) ->
   {Name, Act} = q:head(Processes),
   Service  = ambit_actor:service(Act), 
   ?DEBUG("ambit [ae]: transfer ~p", [Name]),
   P = erlang:setelement(1, Peer, primary),
   ambit_peer:cast(P, {spawn, P, self(), Name, Service}),
   {next_state, transfer, State, 5000}; %% @todo: config

transfer({Vnode, {ok, _}}, _, #{peer := _Vnode, processes := Processes}=State) ->
   erlang:send(self(), transfer),
   {next_state, transfer, State#{processes => q:tail(Processes)}};

transfer({Vnode,  _Error}, _, #{peer := _Vnode}=State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, transfer, State};

transfer(timeout, _, #{peer := _Vnode}=State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, transfer, State};

transfer({handshake, Peer}, _, #{vnode := Vnode, t := T}=State) ->
   ambit_peer:send(Peer, {Vnode, busy}),
   {next_state, transfer, State, 5000};

transfer(_, _, State) ->
   {next_state, transfer, State, 5000}.


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


%%
%% build hash tree
htbuild(Addr, Sup) ->
   htree:build(
      [X || {X, _, _, _} <- supervisor:which_children(Sup),
            belong(X, Addr) =/= false]).

% hash(L, T) ->
%    case htree:hash(L, T) of
%       undefined ->
%          htree:hash(L, T);
%       Hash      ->
%          Hash
%    end.




