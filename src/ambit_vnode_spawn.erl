%% @description
%%   virtual node - actor spawner interface
-module(ambit_vnode_spawn).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode) ->
   pipe:start_link(?MODULE, [Vnode], []).

init([{Type, Addr, _, _}=Vnode]) ->
   ?DEBUG("ambit [spawn]: init ~p", [Vnode]),
   ok = pns:register(vnode_sys, {Type, Addr}, self()),
   {ok, handle, Vnode}.

free(_, {Type, Addr, _, _}) ->
   pns:unregister(vnode_sys, {Type, Addr}).

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle({spawn, Name, Service}, Pipe, {_, Addr, _, _}=Vnode) ->
   %% @todo: split ensure from spawn (2 - message)
   pipe:a(Pipe, 
      pts:ensure(Addr, Name, [Vnode, Service])
   ),
   {next_state, handle, Vnode};

%%
handle({free, Name}, Pipe, {_, Addr, _, _}=Vnode) ->
   pts:send(Addr, Name, free),
   pipe:a(Pipe, ok),
   {next_state, handle, Vnode};

%%
handle({whereis, Name}, Pipe, {_, Addr, _, _}=Vnode) ->
   pipe:a(Pipe, 
      pns:whereis(ambit, {Addr, Name})
   ),
   {next_state, handle, Vnode};

handle(_, _Tx, Vnode) ->
   {next_state, handle, Vnode}.



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

