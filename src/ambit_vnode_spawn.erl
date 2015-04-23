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
handle({spawn, Name, Service}, Pipe, {primary, Addr, _, _}=State) ->
   pipe:a(Pipe, 
      pts:ensure(Addr, Name, [primary, Service])
   ),
   {next_state, handle, State};

handle({spawn, Name, Service}, Pipe, {handoff, Addr, _, _}=State) ->
   pipe:a(Pipe, 
      pts:ensure(Addr, Name, [handoff, Service])
   ),
   {next_state, handle, State};

%%
% handle({{primary, _, _, _}, {free, Name}}, Pipe, State) ->
% handle({{handoff, _, _, _}, {free, Name}}, Pipe, State) ->
handle({free, Name}, Pipe, {_, Addr, _, _}=State) ->
   pts:send(Addr, Name, free),
   pipe:a(Pipe, ok),
   {next_state, handle, State};

%%
% handle({{primary, Addr, _, _}, {whereis, Name}}, Pipe, State) ->
% handle({{handoff, Addr, _, _}, {whereis, Name}}, Pipe, State) ->
handle({whereis, Name}, Pipe, {_, Addr, _, _}=State) ->
   pipe:a(Pipe, 
      pns:whereis(ambit, {Addr, Name})
   ),
   {next_state, handle, State};

handle(_, _Tx, State) ->
   {next_state, handle, State}.



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

