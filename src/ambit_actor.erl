%% @description
%%   actor container / supervisor process
-module(ambit_actor).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/4
  ,init/1
  ,free/2
  ,ioctl/2
  ,primary/3
  ,handoff/3
   %% api
  ,service/1
  ,process/1
]).


%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(State, Addr, Name, Service) ->
   pipe:start_link(?MODULE, [State, Addr, Name, Service], []).

init([primary, Addr, Name, {Mod, Fun, Args}=Service]) ->
   ?DEBUG("ambit [actor]: init primary ~p (~p)", [Name, Service]),
   {ok, Pid} = erlang:apply(Mod, Fun, Args),
   _ = pns:register(ambit, {Addr, Name}, Pid),
   {ok, primary, #{service => Service, process => Pid}};

init([handoff, _Addr, Name, Service]) ->
   ?DEBUG("ambit [actor]: init handoff ~p (~p)", [Name, Service]),
   {ok, handoff, #{service => Service}}.

free(_, #{}) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

service(Pid) ->
   pipe:call(Pid, service).

process(Pid) ->
   pipe:call(Pid, process).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
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






