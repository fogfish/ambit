%% @description
%%   actor management process
%%
%% @todo
%%   * actor signaling
%%   * actor ttl
%%   * free actor process but keep empty stub for management and synchronization 
-module(ambit_actor).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/5
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
   %% api
  ,service/2
  ,handoff/3
]).


%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Sup, Ns, Name, Vnode, Service) ->
   pipe:start_link(?MODULE, [Sup, Ns, Name, Vnode, Service], []).

init([Sup, Ns, Name, Vnode, Service]) ->
   ?DEBUG("ambit [actor]: ~p init ~p ~p", [Vnode, Name, Service]),
   %% register actor management process to the pool
   _ = pns:register(Ns, Name, self()),
   erlang:send(self(), spawn),
   {ok, handle, 
      #{
         sup     => Sup, 
         vnode   => Vnode, 
         name    => Name, 
         service => Service,
         actor   => undefined,
         process => undefined
      }
   }.

free(_, #{sup := Sup, vnode := {_, Addr, _, _}}) ->
   supervisor:terminate_child(pts:i(factory, Addr), Sup),
   ok.

%%
%%
ioctl(service, #{service := Service}) ->
   % return actor service specification 
   Service;
ioctl(process, #{process := Process}) ->
   % return actor instance process 
   Process;
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%% return service specification
service(Addr, Name) ->
   pts:call(Addr, Name, service).

%% handoff actor 
handoff(Addr, Name, Vnode) ->
   pts:call(Addr, Name, {handoff, Vnode}).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle(spawn, _, #{sup := Sup, vnode := {_, Addr, _, _} = Vnode, 
                   name := Name, service := {Mod, Fun, Args}} = State) ->
   {ok, Root} = ambit_actor_sup:start_child(Sup, {Mod, Fun, [Vnode | Args]}),
	case erlang:function_exported(Mod, process, 1) of
      true  ->
			{ok, Pid} = Mod:process(Root),
         %% register actor process to the pool
			_ = pns:register(ambit, {Addr, Name}, Pid),
         {next_state, handle, State#{actor := Root, process := Pid}};
      false ->
         %% register actor process to the pool
			_ = pns:register(ambit, {Addr, Name}, Root),
         {next_state, handle, State#{actor := Root, process := Root}}
   end;

handle(free, _, #{vnode := {_, Addr, _, _}, name := Name}=State) ->
   _ = pns:unregister(ambit, {Addr, Name}),
   _ = pns:unregister(Addr, Name),
   {stop, normal, State};

% @deprecated
handle(service, Tx, #{service := Service}=State) ->
   pipe:ack(Tx, Service),
   {next_state, handle, State};

% @deprecated
handle(process, Tx, #{process := Process}=State) ->
   pipe:ack(Tx, Process),
   {next_state, handle, State};

handle({handoff, Vnode}, Tx, #{actor := Root, service := {Mod, _, _}}=State) ->
   case erlang:function_exported(Mod, handoff, 2) of
      true  ->
         pipe:ack(Tx, 
            Mod:handoff(Root, Vnode)
         ),
         {next_state, handle, State};
      false ->
         pipe:ack(Tx, ok),
         {next_state, handle, State}
   end;

handle(_, _, State) ->
   {next_state, handle, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   






