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
   spawn/1,
   free/1,
   ping/1
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
      %%
      %% no quorum - r/w succeeded on single node only
      {io_n1, [parallel, {repeat, 10}], [spawn]},

      %%
      %% sloppy quorum - r + w > n, read and write must succeeded on two nodes
      {io_n2, [parallel, {repeat, 10}], [spawn, free, ping]},

      %%
      %% strict quorum, each sibling must succeeded
      {io_n3, [parallel, {repeat, 10}], [spawn]}
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
   {ok, Rq0} = ambit:actor(Key, {ambit_echo, start_link, []}),
   {ok, Rq1} = ambit:spawn(Rq0, [{w, N}]),
   Pid = ambit:whereis(Rq1, [{r, N}]),
   case length(Pid) of
      X when X >= N ->
         ok;
      _ ->
         ct:pal("[spawn] ~p ~p", [Key, [erlang:node(X)||X <- Pid]]),
         exit(noquorum)
   end.


%%
%%
free(Config) ->
   N   = opts:val(n, Config),
   Key = key(),
   {ok, Rq0} = ambit:actor(Key, {ambit_echo, start_link, []}),
   {ok, Rq1} = ambit:spawn(Rq0),
   {ok, Rq2} = ambit:free(Rq1),
   %% free is not committed by each sibling peer due to eventual consistency.
   %% operation is succeeded when N sibling is committed 
   case ambit:whereis(Rq2) of
      List when length(List) =< 3 - N ->
         ok;
      _ ->
         exit(failed)
   end.

%%
%%
ping(_Config) -> 
   Key  = key(),
   {ok, Rq0} = ambit:actor(Key, {ambit_echo, start_link, []}),
   {ok, Rq1} = ambit:spawn(Rq0),
   Ping = [pipe:call(Pid, ping) || Pid <- ambit:whereis(Rq1)],
   case length(Ping) of
      X when X > 0 ->
         ok;
      _ ->
         exit(noquorum)
   end.


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
   random:seed(erlang:now()),
   scalar:s(random:uniform(1 bsl 32)).
   % scalar:s(random:uniform(1 bsl 4)).



