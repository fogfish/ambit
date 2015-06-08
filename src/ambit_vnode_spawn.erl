%% @description
%%   virtual node - actor spawner interface
-module(ambit_vnode_spawn).
-behaviour(pipe).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

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
handle({create, #entity{key = Key}=Entity}, Pipe, {_, Addr, _, _}=Vnode) ->
   case pts:ensure(Addr, Key, [Vnode]) of
      {ok,  _} ->
         pipe:a(Pipe, 
            pts:call(Addr, Key, {create, Entity})
         ),
         {next_state, handle, Vnode};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, Vnode}
   end;
   
handle({remove, #entity{key = Key}=Entity}, Pipe, {_, Addr, _, _}=Vnode) ->
   case pts:call(Addr, Key, {remove, Entity}) of
      %% not found is not a critical error, ping back entity
      {error, not_found} ->
         pipe:a(Pipe, {ok, Entity}),
         {next_state, handle, Vnode};
      Result ->
         pipe:a(Pipe, Result),
         {next_state, handle, Vnode}
   end;

handle({lookup, #entity{key = Key}=Entity}, Pipe, {_, Addr, _, _}=Vnode) ->
   case pts:call(Addr, Key, {lookup, Entity}) of
      %% not found is not a critical error, ping back entity
      {error, not_found} ->
         pipe:a(Pipe, {ok, Entity}),
         {next_state, handle, Vnode};
      Result ->
         pipe:a(Pipe, Result),
         {next_state, handle, Vnode}
   end;

handle({whereis, #entity{key = Key}}, Pipe, {_, Addr, _, _}=Vnode) ->
   pipe:a(Pipe, 
      pns:whereis(ambit, {Addr, Key})
   ),
   {next_state, handle, Vnode};

handle(_, _Tx, Vnode) ->
   {next_state, handle, Vnode}.



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

