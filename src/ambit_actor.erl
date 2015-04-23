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

start_link(Sup, State, Addr, Name, Service) ->
   pipe:start_link(?MODULE, [Sup, State, Addr, Name, Service], []).

init([Sup, Type, Addr, Name, Service]) ->
   ?DEBUG("ambit [actor]: init ~s ~p (~p) ~p", [Type, Name, Service, {Addr, Name, actor}]),
   _ = pns:register(Addr, Name, self()),
   erlang:send(self(), spawn),
   {ok, handle, 
      #{
         sup     => Sup, 
         addr    => Addr, 
         type    => Type,
         name    => Name, 
         service => Service,
         process => undefined
      }
   }.

free(_, #{sup := Sup, addr := Addr}) ->
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
handle(spawn, _, #{sup := Sup, addr := Addr, type := Type, name := Name, service := {Mod, Fun, Args}} = State) ->
   {ok, Root} = ambit_actor_sup:start_child(Sup, {Mod, Fun, [Type | Args]}),
	case erlang:function_exported(Mod, actor, 1) of
      true  ->
			{ok, Pid} = Mod:actor(Root),
			_ = pns:register(ambit, {Addr, Name}, Pid);
      false ->
			_ = pns:register(ambit, {Addr, Name}, Root)
   end,
   {next_state, handle, State#{process := Root}};

handle(free, _, #{addr := Addr, name := Name}=State) ->
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






