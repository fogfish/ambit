%%
%%   Copyright 2014 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   basho bench driver
-module(ambit_benchmark).

-export([new/1, run/4]).
-export([run/0, run/1]).

-define(SERVICE, {ambit_echo, start_link, []}).

new(1) ->
   %% @todo: define application config
   lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, debug)),
	ambitz:start(),
   ek:create(ambit, opts:val(ring, ambit)),
   timer:sleep(5000),
	{ok, #{}};
new(_) ->
   {ok, #{}}.

%%
%%
run(spawn, KeyGen, _ValGen, State) ->
   case
      ambitz:spawn(
         ambitz:entity(service, ?SERVICE, 
            ambitz:entity(scalar:s(KeyGen()))
         )
      )
   of
      {error, Reason} ->
         {error, Reason, State};

      _ ->
         {ok,  State}
   end;

run(whereis, KeyGen, _ValGen, State) ->
   case
      ambitz:whereis(
         ambitz:entity(scalar:s(KeyGen()))
      )
   of
      {error, Reason} ->
         {error, Reason, State};

      _ ->
         {ok, State}
   end;


run(_, _KeyGen, _ValGen, State) ->
   {ok, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% stress test
%%%
%%%----------------------------------------------------------------------------   

-define(N,         8).
-define(LOOP,      30 * 1000).
-define(TIMEOUT,  120 * 1000).

%%
%%
run() ->
	run(erlang:node()).	

run(Seed) ->
   ambit:start(),
   net_adm:ping(Seed),
   Seed =/= erlang:node() andalso timer:sleep(10000),
   ok   = init(?N),
   case timer:tc(fun() -> exec(?N) end) of
      {T, ok} ->
         TPU = ?N * ?LOOP / T,
         TPS = ?N * ?LOOP / (T / 1000000),
         {TPU, TPS};
      {_, Error} ->
         Error
   end.

%%
%% init actors for discovery purpose
init(N) ->
   lists:foreach(
      fun(X) ->
         actor( scalar:s(X) )
      end,
      lists:seq(1, N)
   ).
   
actor(N) ->
   Ent0 = ambitz:entity(scalar:s(N)),
   {ok, Ent1} = ambitz:lookup(Ent0),
   {ok, _} = ambitz:spawn(ambitz:entity(service, ?SERVICE, Ent1)).

%%
%%
exec(N) ->
   Self = self(),
   Pids = [spawn_link(fun() -> loop(Self, scalar:s(Id), ?LOOP) end) || Id <- lists:seq(1, N)],
   fold(Pids).

fold([]) -> ok;
fold([Pid | Pids]) ->
   receive
      {ok, Pid} -> fold(Pids)
   after ?TIMEOUT ->
      {error, timeout}
   end.

loop(Pid, _Key, 0) ->
   Pid ! {ok, self()};
loop(Pid,  Key, N) ->
   ambitz:whereis(ambitz:entity(Key)),
   loop(Pid, Key, N - 1).
