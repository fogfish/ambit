%% @description
%%   actor management process
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
   _ = pns:register(Ns, Name, self()),
   erlang:send(self(), spawn),
   {ok, handle, 
      #{
         sup     => Sup, 
         vnode   => Vnode, 
         name    => Name, 
         service => Service,
         process => undefined
      }
   }.

free(_, #{sup := Sup, vnode := {_, Addr, _, _}}) ->
   supervisor:terminate_child(pts:i(factory, Addr), Sup),
   ok.

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
	case erlang:function_exported(Mod, actor, 1) of
      true  ->
			{ok, Pid} = Mod:actor(Root),
			_ = pns:register(ambit, {Addr, Name}, Pid);
      false ->
			_ = pns:register(ambit, {Addr, Name}, Root)
   end,
   {next_state, handle, State#{process := Root}};

handle(free, _, #{vnode := {_, Addr, _, _}, name := Name}=State) ->
   _ = pns:unregister(ambit, {Addr, Name}),
   _ = pns:unregister(Addr, Name),
   {stop, normal, State};

handle(service, Tx, #{service := Service}=State) ->
   pipe:ack(Tx, Service),
   {next_state, handle, State};

handle(process, Tx, #{process := Process}=State) ->
   pipe:ack(Tx, Process),
   {next_state, handle, State};

handle(_, _, State) ->
   {next_state, handle, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   






