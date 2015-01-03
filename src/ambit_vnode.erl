%% @description
%%   virtual node process
-module(ambit_vnode).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,active/3
  ,transfer/3
]).

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
   Vnode = hd(ek:successors(ambit, Addr)),
   %% init vnode services
   {ok,   Sup} = init_sup(Addr),
   {ok, Hand1} = ambit_vnode_spawn:start_link(Vnode, Sup),
   {ok, Hand2} = ambit_vnode_ae:start_link(Vnode, Sup),
   {ok, active, 
      #{
         vnode => Vnode,
         sup   => Sup,
         hands => [Hand1, Hand2]
      }
   }.

free(_, #{vnode := {_, Addr, _, _}, hands := Hands}) ->
   free_sup(Addr),
   lists:foreach(fun pipe:free/1, Hands),
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
active({handoff, Vnode}, _, #{vnode := Self, sup := Sup}=State) ->
   %% initial child transfer procedure
   ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Self, Vnode]),
   erlang:send(self(), transfer),
   {next_state, transfer, 
      State#{
         target    => Vnode,
         processes => q:new([{Name, X} || {Name, X, _, _} <- supervisor:which_children(Sup)])
      }
   };

active(Msg, _Tx, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
   {next_state, active, State}.

%%
%% 
transfer(transfer, _, #{vnode := Vnode, processes := {}}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p completed", [Vnode]),
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
handoff_service(#{target := Vnode, processes := Processes}=State) ->
   {Name, Act} = q:head(Processes),
   Service     = ambit_actor:service(Act),
   ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
   case ambit_peer:spawn(Vnode, Name, Service) of
      {ok, _} ->
         {ok, State#{processes => q:tail(Processes)}};

      Error   ->
         {Error, State}
   end.

