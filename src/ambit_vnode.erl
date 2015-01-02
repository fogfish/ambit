%% @description
%%   virtual node - management process
-module(ambit_vnode).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  % ,idle/3
  ,handle/3
  % ,primary/3
  % ,handoff/3
  ,transfer/3
]).

-define(CHILD(Spawn, Addr, Name, Service), 
   {Name, {ambit_actor, start_link, [Spawn, Addr, Name, Service]}, permanent, 30000, worker, dynamic}
).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(vnode, Addr) ->
   pipe:start_link(?MODULE, [Addr], []).

init([Addr]) ->
   ?DEBUG("ambit [vnode]: init ~b", [Addr]),
   ok = pns:register(vnode, Addr, self()),
   {ok, Sup} = init_sup(Addr),   
   {ok, handle, #{addr => Addr, sup => Sup}}.

free(_, #{addr := Addr}) ->
   free_sup(Addr),
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
handle({primary, Name, Service}, Tx, State0) ->
   {Result, State} = init_service(primary, Name, Service, State0),
   pipe:ack(Tx, Result),
   {next_state, handle, State};

handle({handoff, Name, Service}, Tx, State0) ->
   {Result, State} = init_service(handoff, Name, Service, State0),
   pipe:ack(Tx, Result),
   {next_state, handle, State};

handle({free, Name}, Tx, State0) ->
   {Result, State} = free_service(Name, State0),
   pipe:ack(Tx, Result),
   {next_state, handle, State};

handle({transfer, Peer, Pid}, _, #{addr := Addr, sup := Sup}=State) ->
   %% initial child transfer procedure
   ?NOTICE("ambit [vnode]: transfer ~b to ~s ~p", [Addr, Peer, Pid]),
   erlang:send(self(), transfer),
   {next_state, transfer, 
      State#{
         vnode     => {primary, Addr, Peer, Pid},
         processes => q:new([{Name, X} || {Name, X, _, _} <- supervisor:which_children(Sup)])
      }
   };

handle(Msg, _Tx, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
   {next_state, handle, State}.

%%
%% 
transfer(transfer, _, #{addr := Addr, processes := {}}=State) ->
   ?NOTICE("ambit [vnode]: transfer ~b completed", [Addr]),
   {stop, normal, State};

transfer(transfer, _, State0) ->
   case handoff_service(State0) of
      {ok, State} ->
         erlang:send(self(), transfer),
         {next_state, transfer, State};

      {_,  State} ->
         erlang:send_after(1000, self(), transfer),
         {next_state, transfer, State}
   end;

transfer(Msg, _Tx, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
   {next_state, transfer, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% init vnode supervisor
init_sup(Addr) ->
   supervisor:start_child(ambit_vnode_root_sup, 
      {Addr, {ambit_vnode_sup, start_link, []}, permanent, 5000, supervisor, dynamic}
   ). 

%%
%% free vnode supervisor 
free_sup(Addr) ->
   supervisor:terminate_child(ambit_vnode_root_sup, Addr),
   supervisor:delete_child(ambit_vnode_root_sup, Addr).

%%
%%
init_service(Spawn, Name, Service, #{addr := Addr, sup := Sup}=State) ->
   case supervisor:start_child(Sup, ?CHILD(Spawn, Addr, Name, Service)) of
      {ok, Pid} ->
         {{ok, ambit_actor:process(Pid)}, State};

      {error, {already_started, Pid}} ->
         {{ok, ambit_actor:process(Pid)}, State};

      Error ->
         {Error, State}
   end.

%%
%%
free_service(Name, #{sup := Sup}=State) ->
   supervisor:terminate_child(Sup, Name),
   case supervisor:delete_child(Sup, Name) of
      ok ->
         {ok, State};

      {error, not_found} ->
         {ok, State};

      Error ->
         {Error, State}
   end.

%%
%%
handoff_service(#{vnode := {_, _, _, Pid}=Vnode, processes := Processes}=State) ->
   {Name, Act} = q:head(Processes),
   Service     = ambit_actor:service(Act),
   ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
   case pipe:call(Pid, {Vnode, {init, Name, Service}}) of
      {ok, _} ->
         {ok, State#{processes => q:tail(Processes)}};

      Error   ->
         {Error, State}
   end.




