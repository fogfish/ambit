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
   ok = pns:register(vnode, {Type, Addr}, self()),
   {ok, handle, Vnode}.

free(_, _Vnode) ->
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
handle({spawn, Name, Service}, Pipe, {_, Addr, _, _}=Vnode) ->
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

