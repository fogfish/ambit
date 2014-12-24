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
  ,idle/3
  ,primary/3
  ,handoff/3
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
   {ok, idle, #{addr => Addr}}.

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
idle(primary, _Tx, #{addr := Addr}=State) ->
   %% activate vnode as primary 
   {ok, Sup} = init_sup(Addr),
   {next_state, primary, 
      State#{
         sup       => Sup,
         services  => bst:new()
      }
   };

idle(handoff, _Tx, State) ->
   %% activate vnode as handoff
   {next_state, handoff, 
      State#{
         services  => bst:new(),
         processes => q:new()
      }
   }.

%%
%%
primary({init, Name, Service}, Tx, State0) ->
   {Result, State} = init_service(Name, Service, State0),
   pipe:ack(Tx, Result),
   {next_state, primary, State};

primary({free, Name}, Tx, State0) ->
   {Result, State} = free_service(Name, State0),
   pipe:ack(Tx, Result),
   {next_state, primary, State};

primary({transfer, Peer, Pid}, _, #{addr := Addr, sup := Sup}=State) ->
   %% initial child transfer procedure
   ?NOTICE("ambit [vnode]: transfer ~b to ~s ~p", [Addr, Peer, Pid]),
   erlang:send(self(), transfer),
   {next_state, transfer, 
      State#{
         vnode     => {primary, Addr, Peer, Pid},
         processes => q:new([X || {X, _, _, _} <- supervisor:which_children(Sup)])
      }
   };

primary(primary, _Tx, State) ->
   {next_state, primary, State};

primary(handoff, _Tx, State) ->
   {next_state, primary, State};

primary(Msg, _Tx, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
   {next_state, primary, State}.

%%
%%
handoff({init, Name, Service}, Tx, State0) ->
   {Result, State} = enq_service(Name, Service, State0),
   pipe:ack(Tx, Result),
   {next_state, handoff, State};

handoff({free, Name}, Tx, State0) ->
   {Result, State} = deq_service(Name, State0),
   pipe:ack(Tx, Result),
   {next_state, handoff, State};

handoff({transfer, Peer, Pid}, _, #{addr := Addr}=State) ->
   %% initial child transfer procedure
   ?NOTICE("ambit [vnode]: transfer ~b to ~s ~p", [Addr, Peer, Pid]),
   erlang:send(self(), transfer),
   {next_state, transfer, 
      State#{
         vnode => {primary, Addr, Peer, Pid}
      }
   };

handoff(primary, _Tx, State) ->
   {next_state, handoff, State};

handoff(handoff, _Tx, State) ->
   {next_state, handoff, State};

handoff(Msg, _Tx, State) ->
   ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
   {next_state, handoff, State}.


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
init_service(Name, Service, #{addr := Addr, sup := Sup, services := Services}=State) ->
   case supervisor:start_child(Sup, ?CHILD(Name, Service)) of
      {ok, Pid} ->
         pns:register(ambit, {Addr, Name}, Pid),
         {{ok, Pid}, State#{services => bst:insert(Name, Service, Services)}};

      {error, {already_started, Pid}} ->
         {{ok, Pid}, State};

      Error ->
         {Error, State}
   end.

%%
%%
free_service(Name, #{sup := Sup, services := Services}=State) ->
   supervisor:terminate_child(Sup, Name),
   case supervisor:delete_child(Sup, Name) of
      ok ->
         {ok, State#{services => bst:remove(Name, Services)}};

      {error, not_found} ->
         {ok, State};

      Error ->
         {Error, State}
   end.

%%
%%
enq_service(Name, Service, #{processes := Processes, services := Services}=State) ->
   {ok, 
      State#{
         services  => bst:insert(Name, Service, Services),
         processes => q:enq(Name, Processes)
      }
   }.

deq_service(Name, #{service := Services}=State) ->
   {ok, State#{services => bst:remove(Name, Services)}}.

%%
%%
handoff_service(#{vnode := {_, _, _, Pid}=Vnode, processes := Processes, services := Services}=State) ->
   Name = q:head(Processes),
   ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
   case bst:lookup(Name, Services) of
      undefined ->
         {ok, State#{processes => q:tail(Processes)}};

      Service   ->
         case pipe:call(Pid, {Vnode, {init, Name, Service}}) of
            {ok, _} ->
               {ok, State#{processes => q:tail(Processes)}};

            Error   ->
               {Error, State}
         end
   end.




