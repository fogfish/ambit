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
  ,primary/3
  ,handoff/3
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

init([Sup, primary, Addr, Name, Service]) ->
   ?DEBUG("ambit [actor]: init primary ~p (~p) ~p", [Name, Service, {Addr, Name, actor}]),
   _ = pns:register(Addr, Name, self()),
   erlang:send(self(), spawn),
   {ok, primary, #{sup => Sup, addr => Addr, name => Name, service => Service, process => undefined}};

init([Sup, handoff, Addr, Name, Service]) ->
   ?DEBUG("ambit [actor]: init handoff ~p (~p) ~p", [Name, Service, {Addr, Name, actor}]),
   _ = pns:register(Addr, Name, self()),
   {ok, handoff, #{sup => Sup, addr => Addr, name => Name, service => Service}}.

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
primary(spawn, _, #{sup := Sup, addr := Addr, name := Name, service := {Mod, _, _} = Service} = State) ->
   {ok, Root} = ambit_actor_sup:start_child(Sup, Service),
	case erlang:function_exported(Mod, actor, 1) of
      true  ->
			{ok, Pid} = Mod:actor(Root),
			_ = pns:register(ambit, {Addr, Name}, Pid);
      false ->
			_ = pns:register(ambit, {Addr, Name}, Root)
   end,
   {next_state, primary, State#{process := Root}};

primary(free, _, #{addr := Addr, name := Name}=State) ->
   _ = pns:unregister(ambit, {Addr, Name}),
   _ = pns:unregister(Addr, Name),
   {stop, normal, State};

primary(service, Tx, #{service := Service}=State) ->
   pipe:ack(Tx, Service),
   {next_state, primary, State};

primary(process, Tx, #{process := Process}=State) ->
   pipe:ack(Tx, Process),
   {next_state, primary, State};

primary(_, _, State) ->
   {next_state, primary, State}.

%%
%%
handoff(service, Tx, #{service := Service}=State) ->
   pipe:ack(Tx, Service),
   {next_state, handoff, State};

handoff(process, Tx, State) ->
   pipe:ack(Tx, undefined),
   {next_state, handoff, State};

handoff(_, _, State) ->
   {next_state, handoff, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   






