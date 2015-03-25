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
  ,active/3   % -> primary/3
  ,transfer/3 % -> handoff/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Sup, Vnode) ->
   pipe:start_link(?MODULE, [Sup, Vnode], []).

init([Sup, {_, Addr, _, _}=Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, Addr, self()),
   {ok, active, 
      #{
         vnode => Vnode,
         sup   => Sup
      }
   }.

free(_, #{sup := Sup, vnode := {_, Addr, _, _}}) ->
   ?DEBUG("ambit [vnode]: free ~b", [Addr]),
	ok = pns:unregister(vnode, Addr),
   supervisor:terminate_child(pts:i(factory, vnode), Sup),
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
active({handoff, Vnode}, _, #{vnode := {_, Addr, _, _} = Self}=State) ->
   %% initial child transfer procedure
   ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Self, Vnode]),
   erlang:send(self(), transfer),
   {next_state, transfer, 
      State#{
         target => Vnode,
         stream => stream:build(pns:lookup(Addr, '_'))
      }
   };

active(_Msg, _, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [_Msg]),
   {next_state, active, State}.

%%
%%
transfer(transfer, _, #{vnode := _Vnode, stream := {}}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p completed", [_Vnode]),
   {stop, normal, State};

transfer(transfer, _, #{vnode := {_, Addr, _, _}, target := Vnode, stream := Stream}=State) ->
   {Name, _Pid} = stream:head(Stream),
   Service      = ambit_actor:service(Addr, Name),
   Tx = ambit_peer:cast(Vnode, {spawn, Name, Service}),
   ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
   {next_state, transfer, State#{tx => Tx}, 5000}; %% @todo: config

transfer({Tx, _}, _, #{tx := Tx, stream := Stream}=State) ->
   erlang:send(self(), transfer),
   {next_state, transfer, State#{stream => stream:tail(Stream)}};

transfer(timeout, _, State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, transfer, State};

transfer(Msg, _, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
   {next_state, transfer, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   



