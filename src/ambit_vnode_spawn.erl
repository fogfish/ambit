%% @description
%%   virtual node - actor spawner service
-module(ambit_vnode_spawn).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

-define(CHILD(Mode, Addr, Name, Service), 
   {Name, {ambit_actor, start_link, [Mode, Addr, Name, Service]}, permanent, 30000, worker, dynamic}
).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode, Sup) ->
   pipe:start_link(?MODULE, [Vnode, Sup], []).

init([{_, Addr, _, _}=Vnode, Sup]) ->
   ?DEBUG("ambit [spawn]: init ~p", [Vnode]),
   ok = pns:register(vnode, {primary, Addr}, self()),
   ok = pns:register(vnode, {handoff, Addr}, self()),
   {ok, handle, #{sup => Sup, addr => Addr}}.

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
handle({{primary, _, _, _}, {spawn, Name, Service}}, Pipe, State) ->
   pipe:a(Pipe, create(primary, Name, Service, State)),
   {next_state, handle, State};

handle({{handoff, _, _, _}, {spawn, Name, Service}}, Pipe, State) ->
   pipe:a(Pipe, create(handoff, Name, Service, State)),
   {next_state, handle, State};

% handle({spawn, {primary, _, _, _}=Vnode, Pid, Name, Service}, _, State) ->
%    ambit_peer:send(Pid, {Vnode, create(primary, Name, Service, State)}),
%    {next_state, handle, State};

% handle({spawn, {handoff, _, _, _}=Vnode, Pid, Name, Service}, _, State) ->
%    ambit_peer:send(Pid, {Vnode, create(handoff, Name, Service, State)}),
%    {next_state, handle, State};

% handle({free,  Vnode, Pid, Name}, _, State) ->
%    ambit_peer:send(Pid, {Vnode, destroy(Name, State)}),
%    {next_state, handle, State};

handle(_, _Tx, State) ->
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
create(Mode, Name, Service, #{addr := Addr, sup := Sup}) ->
   case supervisor:start_child(Sup, ?CHILD(Mode, Addr, Name, Service)) of
      {ok, Pid} ->
         ok;

      {error, {already_started, Pid}} ->
         ok;

      Error ->
         Error
   end.

%%
%%
destroy(Name, #{sup := Sup}) ->
   supervisor:terminate_child(Sup, Name),
   case supervisor:delete_child(Sup, Name) of
      ok ->
         ok;

      {error, not_found} ->
         ok;

      Error ->
         Error
   end.



