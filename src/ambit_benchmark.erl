%% @description
%%   basho bench driver
-module(ambit_benchmark).

-export([new/1, run/4]).
-export([run/0, run/1]).

-define(SERVICE, {ambit_echo, start_link, []}).

new(1) ->
   lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, debug)),
	ambit:start(),
   timer:sleep(5000),
	{ok, #{}};
new(_) ->
   {ok, #{}}.

%%
%%
run(spawn, KeyGen, _ValGen, State) ->
   case
      ambit:spawn(ambit:actor(scalar:s(KeyGen()), ?SERVICE))
   of
      {error, Reason} ->
         {error, Reason, State};

      _ ->
         {ok,  State}
   end;

run(whereis, KeyGen, _ValGen, State) ->
   case
      ambit:whereis(scalar:s(KeyGen()))
   of
      {error, Reason} ->
         {error, Reason, State};

      List when is_list(List), length(List)  > 1 ->
         {ok,  State};

      _ ->
         {error, na, State}
   end;


run(_, _KeyGen, _ValGen, State) ->
   {ok, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% stress test
%%%
%%%----------------------------------------------------------------------------   

-define(N,         4).
-define(LOOP,     60 *  100).
-define(TIMEOUT,  60 * 1000).

run() ->
	run(erlang:node()).	

run(Seed) ->
   ambit:start(),
   net_adm:ping(Seed),
   Seed =/= erlang:node() andalso timer:sleep(10000),
   case timer:tc(fun() -> exec(?N) end) of
      {T, ok} ->
         TPU = ?N * ?LOOP / T,
         TPS = ?N * ?LOOP / (T / 1000000),
         {TPU, TPS};
      {_, Error} ->
         Error
   end.

exec(N) ->
   Self = self(),
   Pids = [spawn_link(fun() -> loop(Self, Id, ?LOOP) end) || Id <- lists:seq(1, N)],
   fold(Pids).

fold([]) -> ok;
fold([Pid | Pids]) ->
   receive
      {ok, Pid} -> fold(Pids)
   after ?TIMEOUT ->
      {error, timeout}
   end.

loop(Pid, _Id, 0) ->
   Pid ! {ok, self()};
loop(Pid,  Id, N) ->
   Key     = <<(scalar:s(Id))/binary, $-, (scalar:s(N))/binary>>,
   ambit:whereis(Key),
   % case ambit:spawn(ambit:actor(Key, ?SERVICE)) of
   %    {error, Reason} ->
   %       io:format("[error] ~p~n", [Reason]);
   %    _ ->
   %       ok
   % end,
   loop(Pid, Id, N - 1).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

