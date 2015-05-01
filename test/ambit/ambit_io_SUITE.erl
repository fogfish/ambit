%% @description
%%    read-write io traffic
-module(ambit_io_SUITE).
-include_lib("common_test/include/ct.hrl").

%% common test
-export([
   all/0,
   groups/0,
   init_per_suite/1,
   end_per_suite/1,
   init_per_group/2,
   end_per_group/2
]).
-export([
   spawn/1
]).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

all() ->
   [{group, io_n1}, {group, io_n2}, {group, io_n3}].

groups() ->
   [
      {io_n1,  [parallel, {repeat, 15}], [spawn]},
      {io_n2,  [parallel, {repeat, 15}], [spawn]},
      {io_n3,  [parallel, {repeat, 15}], [spawn]}
   ].

%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   

%%
init_per_suite(Config) ->
   ambit:start(),
   cluster_pending_peers(3),
   Config.

end_per_suite(_Config) ->
   ok.
   
%%
init_per_group(io_n1, Config) ->
   [{n, 1} | Config];
init_per_group(io_n2, Config) ->
   [{n, 2} | Config];
init_per_group(io_n3, Config) ->
   [{n, 3} | Config];
init_per_group(_, Config) ->
   Config.

%%
end_per_group(_, _Config) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% test cases
%%%
%%%----------------------------------------------------------------------------   

%%
%%
spawn(Config) ->
   N   = opts:val(n, Config),
   Key = key(),
   ok  = ambit:spawn(Key, {ambit_echo, start_link, []}, [{w, N}]),
   Pid = ambit:whereis(Key, [{r, N}]),
   case length(Pid) of
      X when X >= N ->
         ok;
      _ ->
         ct:pal("[spawn] ~p ~p", [Key, [erlang:node(X)||X <- Pid]]),
         exit(noquorum)
   end.

   % ping / pong test
   % lists:foreach(
   %    fun(X) -> test = pipe:call(X, test) end,
   %    ambit:whereis(Ns)
   % ).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% pending N-cluster nodes
cluster_pending_peers(N) ->
   case length(ek:members(ambit)) of
      X when X < N ->
         timer:sleep(1000),
         cluster_pending_peers(N);
      _ ->
         ok
   end.

%%
%%
key() ->
   scalar:s(random:uniform(1 bsl 32)).



