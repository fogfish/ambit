%% @description
%%   virtual node coordinator process
-module(ambit_vnode).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,primary/3
  ,handoff/3
  ,suspend/3   %% -> offline
  ,transfer/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link(Sup, Vnode) ->
   pipe:start_link(?MODULE, [Sup, Vnode], []).

init([Sup, {primary, Addr, _, _}=Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, Addr, self()),
   {ok, primary, 
      #{
         vnode   => Vnode,
         sup     => Sup   
      }
   };

init([Sup, {handoff, Addr, _, _}=Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, Addr, self()),
   {ok, handoff, 
      #{
         vnode   => Vnode,
         sup     => Sup   
      }
   }.

%%
%%
free(_Reason, #{sup := Sup, vnode := {_, Addr, _, _}}) ->
   ?DEBUG("ambit [vnode]: free ~b ~p", [Addr, _Reason]),
	ok = pns:unregister(vnode, Addr),
   supervisor:terminate_child(pts:i(factory, vnode), Sup),
   ok.

ioctl(vnode, #{vnode := Vnode}) ->
   Vnode;
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
primary({handoff, Peer}, _,  #{vnode := {_, Addr, _, _} = Vnode}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Vnode, Peer]),
   erlang:send(self(), transfer),
   {next_state, suspend, 
      State#{
         handoff => Peer,
         stream  => stream:build(pns:lookup(Addr, '_'))
      }
   };

primary({sync, Peer}, _, #{vnode := Vnode}=State) ->
   ?NOTICE("ambit [vnode]: sync ~p with ~p", [Vnode, Peer]),
   %% @todo: initiate actor sync procedure 
   {next_state, primary, State}.

%%
%%
handoff({handoff, Peer}, _,  #{vnode := {_, Addr, _, _} = Vnode}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Vnode, Peer]),
   erlang:send(self(), transfer),
   {next_state, suspend, 
      State#{
         handoff => Peer,
         stream  => stream:build(pns:lookup(Addr, '_'))
      }
   };

handoff({sync, _Peer}, _, State) ->
   %% do nothing, hint db is not synchronized
   {next_state, handoff, State}.


%%
%%
suspend(transfer, _, #{vnode := _Vnode, stream := {}}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p completed", [_Vnode]),
   {stop, normal, State};

suspend(transfer, _, #{vnode := {_, Addr, _, _}, handoff := Handoff, stream := Stream}=State) ->
   {Name, _Pid} = stream:head(Stream),
   case ambit_actor:service(Addr, Name) of
      %% service died during transfer i/o
      undefined ->
         erlang:send(self(), transfer),
         {next_state, suspend, State#{stream => stream:tail(Stream)}};      

      %% sync removed service
      #entity{val = undefined} = Entity ->
         ?DEBUG("ambit [vnode]: transfer (-) ~p", [Name]),
         Tx = ambit_peer:cast(Handoff, {remove, Entity}),
         {next_state, transfer, State#{tx => Tx}, 10000}; %% @todo: config

      %% sync existed service
      Entity ->
         ?DEBUG("ambit [vnode]: transfer (+) ~p", [Name]),
         Tx = ambit_peer:cast(Handoff, {create, Entity}),
         {next_state, transfer, State#{tx => Tx}, 10000}  %% @todo: config
   end;

suspend(_, _, State) ->
   {next_state, suspend, State}.

%%
%%
transfer({Tx, _Result}, _, #{tx := Tx, vnode := {_, Addr, _, _}, handoff := Handoff, stream := Stream}=State) ->
   ?DEBUG("ambit [vnode]: transfer ~p", [_Result]),
   {Name, _Pid} = stream:head(Stream),
   %% @todo: make asynchronous handoff with long-term expectation of data transfer
   ambit_actor:handoff(Addr, Name, Handoff),
   erlang:send(self(), transfer),
   {next_state, suspend, State#{stream => stream:tail(Stream)}};
   
transfer(timeout, _, State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, suspend,  State};

transfer(_, _, State) ->
   {next_state, transfer, State}.

   
%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   



