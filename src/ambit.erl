%% @description
%%   distributed actors
-module(ambit).

-export([behaviour_info/1]).
-export([
   spawn/2,
   spawn/3,
   free/1,
   free/2,
   whereis/1,
   whereis/2,
   successors/1,
   predecessors/1,
   sibling/2
]).
-export([
   start/0
  ,i/1
]).

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
      %% get identity of actor process 
      %%
      %% -spec(actor/1 :: (pid()) -> {ok, pid()} | {error, any()}).
      {actor, 1}
	];
behaviour_info(_) ->
   undefined.

%%%----------------------------------------------------------------------------   
%%%
%%% application interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% spawn service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(spawn/2 :: (any(), any()) -> ok | {error, any()}).
-spec(spawn/3 :: (any(), any(), list()) -> ok | {error, any()}).

spawn(Key, Service) ->
	ambit:spawn(Key, Service, []).

spawn(Key, Service, Opts) ->
   ambit_coordinator:call(Key, {spawn, Key, Service}, Opts).

%%
%% free service on the cluster
%%  Options
%%    w - number of succeeded writes
-spec(free/1 :: (any()) -> ok | {error, any()}).
-spec(free/2 :: (any(), list()) -> ok | {error, any()}).

free(Key) ->
	free(Key, []).

free(Key, Opts) ->
   ambit_coordinator:call(Key, {free, Key}, Opts).

%%
%% lookup service end-point
%%  Options
%%    r - number of succeeded reads
-spec(whereis/1 :: (any()) -> [pid()]).
-spec(whereis/2 :: (any(), list()) -> [pid()]).

whereis(Key) ->
	whereis(Key, []).

whereis(Key, Opts) ->
   ambit_coordinator:call(Key, {whereis, Key}, Opts).

%%
%% return list of successor nodes in ambit cluster
%% @todo filter unique nodes
-spec(successors/1 :: (any()) -> [ek:vnode()]).

successors(Key) ->
   [{A, Addr, Id, erlang:node(Pid)} || {A, Addr, Id, Pid} <- sibling(fun ek:successors/2, Key)].

%%
%% return list of successor nodes in ambit cluster
%% @todo filter unique nodes
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
	case filelib:is_file(?CONFIG) of
		true ->
   		applib:boot(?MODULE, ?CONFIG);
		_    ->
			applib:boot(?MODULE, [])
	end.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   







