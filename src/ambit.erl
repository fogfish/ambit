%% @description
%%   distributed actors
-module(ambit).
-include("ambit.hrl").

%% @todo: 
%%   * write-repair (if handoff node do not exist during write)

-export([behaviour_info/1]).
-export([
   new/1,
   new/2,
   get/1
]).
-export([
   spawn/1,
   spawn/2,
   lookup/1,
   lookup/2,
   free/1,
   free/2,
   whereis/1,
   whereis/2
]).

-export([
   successors/1,
   predecessors/1,
   sibling/2,
   i/1,
   start/0
]).

-type(key()    :: binary()).
-type(entity() :: #entity{}).

%%%----------------------------------------------------------------------------   
%%%
%%% actor behavior interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% 
behaviour_info(callbacks) ->
   [
      %%
      %% return pid of actor process 
      %%
      %% -spec(process/2 :: (pid(), atom()) -> {ok, any()} | {error, any()}).
      {process,   1}

      %%
      %% initiate actor handoff procedure
      %%
      %% -spec(handoff/2 :: (pid(), ek:vnode()) -> ok).
     ,{handoff,   2}
	];
behaviour_info(_) ->
   undefined.

%%%----------------------------------------------------------------------------   
%%%
%%% application interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% create service entity
-spec(new/1 :: (binary()) -> entity()).
-spec(new/2 :: (binary(), any()) -> entity()).

new(Key) ->
   #entity{key = Key}.

new(Key, Service) ->
   #entity{key = Key, val = Service}.

%%
%% get service entity property
-spec(get/1 :: (entity()) -> any() | undefined).

get(#entity{val = Service}) ->
   Service.

%%
%% spawn service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(spawn/1 :: (entity()) -> {ok, entity()} | {error, any()}).
-spec(spawn/2 :: (entity(), list()) -> {ok, entity()} | {error, any()}).

spawn(Entity) ->
   ambit:spawn(Entity, []).

spawn(Entity, Opts) ->
   ambit_coordinator:create(Entity, Opts).
   
%%
%% free service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(free/1 :: (entity()) -> {ok, entity()} | {error, any()}).
-spec(free/2 :: (entity(), list()) -> {ok, entity()} | {error, any()}).

free(Entity) ->
	free(Entity, []).

free(Entity, Opts) ->
   ambit_coordinator:remove(Entity, Opts).

%%
%% lookup service on the cluster
%%  Options
%%    r - number of succeeded reads
-spec(lookup/1 :: (key()) -> {ok, entity()} | {error, any()}).
-spec(lookup/2 :: (key(), any()) -> {ok, entity()} | {error, any()}).

lookup(Key) ->
   ambit:lookup(Key, []).

lookup(Key, Opts) ->
   ambit_coordinator:lookup(new(Key), Opts).

%%
%% lookup service processes
%%  Options
%%    r - number of succeeded reads
-spec(whereis/1 :: (any()) -> [pid()]).
-spec(whereis/2 :: (any(), list()) -> [pid()]).

whereis(Key) ->
	whereis(Key, []).

whereis(Key, Opts) ->
   ambit_coordinator:whereis(#entity{key = Key, val = []}, Opts).

%%
%% return list of successor nodes in ambit cluster
-spec(successors/1 :: (any()) -> [ek:vnode()]).

successors(Key) ->
   [{A, Addr, Id, erlang:node(Pid)} || {A, Addr, Id, Pid} <- sibling(fun ek:successors/2, Key)].

%%
%% return list of successor nodes in ambit cluster
-spec(predecessors/1 :: (any()) -> [ek:vnode()]).

predecessors(Key) ->
   [{A, Addr, Id, erlang:node(Pid)} || {A, Addr, Id, Pid} <- sibling(fun ek:predecessors/2, Key)].

%%
%% return list of sibling nodes
-spec(sibling/2 :: (function(), any()) -> [ek:vnode()]).

sibling(Fun, Key) ->
   case
      lists:partition(
         fun({X, _, _, _}) -> X =/= primary end,
         Fun(ambit, Key)
      )
   of
      {Handoff,      []} ->
         Handoff;
      {Handoff, Primary} ->
         Primary ++ Handoff
   end.

%%%----------------------------------------------------------------------------   
%%%
%%% management interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% check system status
%%  Options
%%    * alive - vnode availability
%%    * alloc - vnode allocation
i(alive) ->
	[{X, length(Y)} || X <- ek:vnode(ambit), Y <- [i(X)], length(Y) =/= 0];

i(alloc) ->
	lists:foldl(
		fun(X, Acc) -> orddict:update_counter(X, 1, Acc) end,
		orddict:new(),
		[Y || X <- ek:vnode(ambit), {_, _, Y, _} <- i(X)]
	);

i(Addr) ->
	[X || X <- ek:successors(ambit, Addr), ambit_peer:i(X) =/= undefined].


%%
%% RnD application start
-define(CONFIG, "./priv/app.config").
start() ->
   File = case code:priv_dir(?MODULE) of
      {error, _} ->
         ?CONFIG;
      Path       ->
         filename:join([Path, "app.config"])
   end,
	case filelib:is_file(File) of
		true ->
   		applib:boot(?MODULE, File);
		_    ->
			applib:boot(?MODULE, [])
	end.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   







