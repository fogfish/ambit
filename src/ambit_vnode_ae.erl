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

-ifdef(CONFIG_AMBIT_DEBUG_AE).
-define(DEB(Str, Args), ?DEBUG(Str, Args)).
-else.
-define(DEB(Str, Args), ok).
-endif.

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode, Sup) ->
   pipe:start_link(?MODULE, [Vnode, Sup], []).

init([{_, Addr, Node, Peer}=Vnode, Sup]) ->
   ?DEB("ambit [ae]: init ~p", [Vnode]),
   ok = pns:register(vnode, {ae, Addr}, self()),   
   {ok, idle, 
      #{
         vnode    => {ae, Addr, Node, Peer},
         sup      => Sup,
         ht       => htree:new(),
         t        => tempus:timer(?CONFIG_CYCLE_AE, reconcile),
         t_hshake => ?CONFIG_CYCLE_AE
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
idle(reconcile, Pipe, #{vnode := Vnode}=State) ->
   handshake(reconcile, Pipe, State#{peer => siblings(Vnode)});

idle({ae_req_handshake, Vnode}, Pipe, State) ->
   ?DEB("ambit [ae]: -handshake ~p", [Vnode]),
   pipe:a(Pipe, ae_ack_handshake),   
   snapshot(passive, Pipe, State#{peer => Vnode});

idle(Msg, Pipe, State) ->
   handle_event(Msg, Pipe, idle, State).

%%
%%
handshake(reconcile, _, #{peer := [], t := T}=State) ->
   %% reconciliation is completed, all peers iterated
   {next_state, idle, maps:remove(peer, State#{t => tempus:timer(T, reconcile)})};

handshake(reconcile, _, #{vnode := Vnode, peer := [Peer|_], t_hshake := T}=State) ->
   %% propose reconciliation session to peer
   ambit_peer:cast(Peer, {ae_req_handshake, Vnode}),
   {next_state, handshake, State#{t_hshake => tempus:timer(T, ae_timeout)}};

handshake({_, ae_ack_handshake}, Pipe, #{peer := [Peer|_], t_hshake := T}=State) ->
   ?DEB("ambit [ae]: +handshake ~p", [Peer]),
   snapshot(active, Pipe, State#{peer => Peer, t_hshake => tempus:cancel(T)});

handshake({_, ae_nok_handshake}, _Pipe, #{peer := [_Peer|Peers], t_hshake := T}=State) ->
   erlang:send(self(), reconcile),
   {next_state, handshake, State#{peer => Peers, t_hshake => tempus:cancel(T)}};

handshake(ae_timeout, _Pipe, #{peer := [_Peer|Peers]}=State) ->
   erlang:send(self(), reconcile),
   {next_state, handshake, State#{peer => Peers}};

handshake(Msg, Pipe, State) ->
   handle_event(Msg, Pipe, handshake, State).


%%
%% 
snapshot(active, Pipe, #{sup := Sup}=State) ->
   %% initiate reconciliation as master
   exchange(init, Pipe, State#{lsync => 0, ht => htbuild(Sup)});

snapshot(passive,  _, #{sup := Sup}=State) ->
   %% initiate reconciliation as slave
   {next_state, exchange, State#{lsync => 0, ht => htbuild(Sup)}};

snapshot(Msg, Pipe, State) ->
   handle_event(Msg, Pipe, snapshot, State).

%%
%%
exchange(init, _Pipe, #{lsync := L, ht := HT, peer := Peer}=State) ->
   %% initialize sync session
   case htree:hash(L, HT) of
      undefined ->
         ambit_peer:send(Peer, htree:hash(HT)),
         {next_state, exchange, State};

      HashB     ->
         ambit_peer:send(Peer, HashB),
         {next_state, exchange, State}
   end;

exchange({hash, -1, _}=HashA, Pipe, #{ht := HT0}=State) ->
   %% list of leaf hashes is proposed
   HashB = htree:hash(HT0),
   pipe:a(Pipe, HashB),   
   Diff = htree:diff(HashA, HashB),
   HT   = htree:evict(Diff, HT0),
   transfer(exec, Pipe, State#{ht => prepare(HT)});

exchange({hash,  _, _}=HashA, Pipe, #{lsync := L, ht := HT0}=State) ->
   case htree:hash(L, HT0) of
      undefined ->
         pipe:a(Pipe, htree:hash(HT0)),
         {next_state, exchange, State};

      HashB     ->
         pipe:a(Pipe, HashB),
         Diff = htree:diff(HashA, HashB),
         HT   = htree:evict(Diff, HT0),
         {next_state, exchange, State#{lsync => L + 1, tree => HT}}          
   end;

exchange(Msg, Pipe, State) ->
   handle_event(Msg, Pipe, exchange, State).

%%
%%
transfer(exec, _Pipe, #{peer := Peer, ht := [], t := T}=State) ->
   ?DEB("ambit [ae]: ~p completed", [Peer]),
   {next_state, idle, State#{t => tempus:timer(T, reconcile)}};

transfer(exec, _Pipe, #{peer := Peer, ht := [{Name, Pid} | _]}=State) ->
   ?DEB("ambit [ae]: transfer ~p", [Name]),
   Service = ambit_actor:service(Pid), 
   Vnode   = erlang:setelement(1, Peer, primary),
   ambit_peer:cast(Vnode, {spawn, Name, Service}),
   {next_state, transfer, State}; %% @todo: config

transfer({_Tx, ok}, _, #{ht := [_ | Tail]}=State) ->
   erlang:send(self(), exec),
   {next_state, transfer, State#{ht => Tail}};

transfer({_Tx,  _}, _, State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, transfer, State};

% transfer(timeout, _, #{peer := _Vnode}=State) ->
%    erlang:send_after(1000, self(), transfer),
%    {next_state, transfer, State};

% transfer(ae_handshake, Pipe, #{vnode := Vnode, t := T}=State) ->
%    pipe:a(Pipe, unreachable),
%    {next_state, transfer, State, 5000};

transfer(Msg, Pipe, State) ->
   handle_event(Msg, Pipe, transfer, State).

%%
%% all state event handler
handle_event({ae_req_handshake, _}, Pipe, Sid, State) ->
   pipe:a(Pipe, ae_nok_handshake),
   {next_state, Sid, State};

handle_event({hash, _, _}, _Pipe, Sid, State) ->
   {next_state, Sid, State};

handle_event(_Msg, _Pipe, Sid, State) ->
   ?WARNING("ambit [ae]: unknown message ~p at ~s~n", [_Msg, Sid]),
   {next_state, Sid, State}.




%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% return list of siblings (nodes responsible to hold same vnode)
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
%% build hash tree
htbuild(Sup) ->
   htree:build([{X, Pid} || {X, Pid, _, _} <- supervisor:which_children(Sup)]).

%%
%% prepare list of processes to transfer
prepare(HT) ->
   htree:list(HT).
