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

%% @todo: 
%%  * globally maintained hash tree
%%  * handshake delivery root signature (ack / nack based on it)

%%
%%
idle(reconcile, Pipe, #{vnode := Vnode}=State) ->
   handshake(reconcile, Pipe, State#{peer => siblings(Vnode)});

idle(ae_handshake, Pipe, #{vnode := Vnode}=State) ->
   ?DEBUG("ambit [ae]: -handshake ~p", [Vnode]),
   pipe:a(Pipe, ae_ack),   
   snapshot(passive, Pipe, State);

idle(M, _, State) ->
   io:format("--> idle ~p~n", [M]),
   {next_state, idle, State}.


%%
%%
handshake(reconcile, _, #{peer := [], t := T}=State) ->
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

handshake(reconcile, _, #{vnode := Vnode, peer := [Peer|_]}=State) ->
   % @todo: timeout
   ambit_peer:cast(Peer, ae_handshake),
   {next_state, handshake, State};

handshake({_, ae_ack}, Pipe, #{vnode := Vnode, peer := [Peer|_]}=State) ->
   ?DEBUG("ambit [ae]: ~p +handshake ~p", [Vnode, Peer]),
   snapshot(active, Pipe, State#{peer => Peer});

handshake({_, unreachable}, _Pipe, #{peer := [_Peer|Peers]}=State) ->
   erlang:send(self(), reconcile),
   {next_state, handshake, State#{peer => Peers}};

handshake(ae_handshake, Pipe, #{vnode := Vnode}=State) ->
   pipe:a(Pipe, unreachable),
   {next_state, handshake, State};

handshake(Msg, Pipe, State) ->
   io:format("--> handshake ~p ~p~n", [Msg, Pipe]),
   {next_state, handshake, State}.


%%
%% 
snapshot(active, Pipe, #{peer := {_, Addr, _, _}, sup := Sup}=State) ->
   %% initiate reconciliation as master
   exchange(init, Pipe, State#{l => 0, tree => htbuild(Addr, Sup)});
   % erlang:send(self(), init),
   % {next_state, exchange, };

snapshot(passive,  _, #{vnode := {_, Addr, _, _}, sup := Sup}=State) ->
   %% initiate reconciliation as slave
   {next_state, exchange, State#{l => 0, tree => htbuild(Addr, Sup)}};

snapshot(ae_handshake, Pipe, #{vnode := Vnode}=State) ->
   pipe:a(Pipe, unreachable),
   {next_state, snapshot, State};

snapshot(M, _, State) ->
   io:format("--> snapshot ~p~n", [M]),
   {next_state, snapshot, State}.


%%
%%
exchange(init, _Pipe, #{l := L, tree := Tree0, peer := Peer}=State) ->
   case htree:hash(L, Tree0) of
      undefined ->
         ambit_peer:send(Peer, htree:hash(Tree0)),
         {next_state, exchange, State};

      HashB     ->
         ambit_peer:send(Peer, HashB),
         {next_state, exchange, State}
   end;

exchange({hash, -1, _}=HashA, Pipe, #{tree := Tree0, sup := Sup}=State) ->
   HashB = htree:hash(Tree0),
   pipe:a(Pipe, HashB),   
   Diff = htree:diff(HashA, HashB),
   Tree = htree:evict(Diff, Tree0),
   erlang:send(self(), transfer),   
   {next_state, transfer, State#{processes => q:new([{Name, X} || {Name, X, _, _} <- supervisor:which_children(Sup), htree:lookup(Name, Tree) =/= undefined])}};

exchange({hash,  _, _}=HashA, Pipe, #{l := L, tree := Tree0}=State) ->
   case htree:hash(L, Tree0) of
      undefined ->
         pipe:a(Pipe, htree:hash(Tree0)),
         {next_state, exchange, State};

      HashB     ->
         pipe:a(Pipe, HashB),
         Diff = htree:diff(HashA, HashB),
         Tree = htree:evict(Diff, Tree0),
         {next_state, exchange, State#{l => L + 1, tree => Tree}}          
   end;

exchange(ae_handshake, Pipe, #{vnode := Vnode}=State) ->
   pipe:a(Pipe, unreachable),
   {next_state, exchange, State};

exchange(M, _, State) ->
   io:format("--> exchange ~p~n", [M]),
   {next_state, exchange, State}.

%%
%%
transfer(transfer, _Pipe, #{peer := Peer, processes := {}, t := T}=State) ->
   ?NOTICE("ambit [ae]: ~p completed", [Peer]),
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

transfer(transfer, _Pipe, #{peer := Peer, processes := Processes}=State) ->
   {Name, Act} = q:head(Processes),
   Service  = ambit_actor:service(Act), 
   ?DEBUG("ambit [ae]: transfer ~p", [Name]),
   P = erlang:setelement(1, Peer, primary),
   ambit_coordinator:cast(P, ambit_req:new(vnode, {spawn, Name, Service})),
   {next_state, transfer, State, 5000}; %% @todo: config

transfer({_Tx, ok}, _, #{peer := _Vnode, processes := Processes}=State) ->
   erlang:send(self(), transfer),
   {next_state, transfer, State#{processes => q:tail(Processes)}};

transfer({_Tx,  _Error}, _, #{peer := _Vnode}=State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, transfer, State};

transfer(timeout, _, #{peer := _Vnode}=State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, transfer, State};

transfer(ae_handshake, Pipe, #{vnode := Vnode, t := T}=State) ->
   pipe:a(Pipe, unreachable),
   {next_state, transfer, State, 5000};

transfer(M, _, State) ->
   io:format("--> transfer ~p~n", [M]),
   {next_state, transfer, State, 5000}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% return list of siblings
siblings({_, Addr, _, _}) ->
   case siblings1(ek:successors(ambit, Addr)) of
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
      [{X, Pid} || {X, Pid, _, _} <- supervisor:which_children(Sup),
            belong(X, Addr) =/= false]).


