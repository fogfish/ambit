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
  ,active/3
  ,transfer/3
]).

-define(CHILD(Name, Service), {Name, Service, permanent, 30000, worker, dynamic}).

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
   {ok, active, 
      #{
         addr     => Addr,
         sup      => Sup,
         services => rbtree:new()
      }
   }.

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

active({init, Name, Service}, Tx, #{sup := Sup, services := Services}=State) ->
   case supervisor:start_child(Sup, ?CHILD(Name, Service)) of
      {ok, Pid} ->
         pns:register(ambit, Name, Pid),
         pipe:ack(Tx, {ok, Pid}),
         {next_state, active, 
            State#{
               services => rbtree:insert(Name, Service, Services)
            }
         };

      {error, {already_started, Pid}} ->
         pipe:ack(Tx, {ok, Pid}),
         {next_state, active, State};

      Error ->
         pipe:ack(Tx, Error),
         {next_state, active, State}
   end;

active({free, Name}, Tx, #{sup := Sup}=State) ->
   %% @todo: free - terminate child / evict remove child (?)
   supervisor:terminate_child(Sup, Name),
   case supervisor:delete_child(Sup, Name) of
      ok ->
         pipe:ack(Tx, ok);
      {error, not_found} ->
         pipe:ack(Tx, ok);
      Error ->
         pipe:ack(Tx, Error)
   end,
   {next_state, active, State};

active({transfer, Peer, Pid}, _, #{addr := Addr, sup := Sup}=State) ->
   %% initial child transfer procedure
   ?NOTICE("ambit [vnode]: transfer ~b to ~s ~p", [Addr, Peer, Pid]),
   erlang:send(self(), transfer),
   {next_state, transfer, 
      State#{
         vnode     => {primary, Addr, Peer, Pid},
         processes => [X || {X, _, _, _} <- supervisor:which_children(Sup)]
      }
   };

active(M, Tx, State) ->
   io:format("=> ~p~n", [M]),
   pipe:ack(Tx, ok),
   {next_state, active, State}.

%%
%% 
transfer(transfer, _, #{addr := Addr, processes := []}=State) ->
   ?NOTICE("ambit [vnode]: transfer ~b completed", [Addr]),
   {stop, normal, State};

transfer(transfer, _, #{vnode := {_, _, _, Pid} = VNode, processes := [Name | Processes], services := Services}=State) ->
   ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
   case pipe:call(Pid, {VNode, {init, Name, rbtree:lookup(Name, Services)}}) of
      {ok,    _} ->
         erlang:send(self(), transfer),
         {next_state, transfer, State#{processes => Processes}};
      {error, _} ->
         erlang:send_after(1000, self(), transfer),
         {next_state, transfer, State}
   end;

transfer(M, Tx, State) ->
   io:format("=> ~p~n", [M]),
   pipe:ack(Tx, ok),
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

