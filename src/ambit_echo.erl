%% @description
%%   echo service (used for RnD)
-module(ambit_echo).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).
%% ambit callback
-export([
   process/1,
   handoff/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Vnode) ->
   pipe:start_link(?MODULE, [Vnode], []).

init([Vnode]) ->
   {ok, handle, Vnode}.

free(_, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% ambit
%%%
%%%----------------------------------------------------------------------------   

process(Root) ->
   {ok, Root}.

handoff(Root, Vnode) ->
   pipe:call(Root, {handoff, Vnode}).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({handoff, Vnode}, Tx, {primary, _, _, _}=State) ->
   ?ERROR("ambit [actor]: data transfer to ~p", [Vnode]),
   pipe:ack(Tx, ok),
   {next_state, handle, State};

handle({handoff, Vnode}, Tx, {handoff, _, _, _}=State) ->
   ?ERROR("ambit [actor]: hint gossip with ~p", [Vnode]),
   pipe:ack(Tx, ok),
   {next_state, handle, State};

handle(Msg, Tx, State) ->
   pipe:ack(Tx, Msg),
   {next_state, handle, State}.


