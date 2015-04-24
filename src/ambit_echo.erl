%% @description
%%   echo service (used for RnD)
-module(ambit_echo).
-behaviour(pipe).

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(_Vnode) ->
   pipe:start_link(?MODULE, [], []).

init(_) ->
   {ok, handle, #{}}.

free(_, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle(Msg, Tx, State) ->
   pipe:ack(Tx, Msg),
   {next_state, handle, State}.


