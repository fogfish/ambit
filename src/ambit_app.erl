%% @description
%%   distributed actors
-module(ambit_app).
-behaviour(application).

-include("ambit.hrl").

-export([
   start/2
  ,stop/1
]).

%%
%%
start(_Type, _Args) ->
   clue:define(meter, {ambit, req, success}, 600 * 1000),
   clue:define(meter, {ambit, req, failure}, 600 * 1000),
   clue:define(meter, {ambit, req, latency}, 600 * 1000),
   ambit_sup:start_link(). 

%%
%%
stop(_State) ->
   ok.
