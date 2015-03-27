%% @description
%%   distributed actors
-module(ambit).

-export([start/0]).
-export([
   spawn/2,
   free/1,
   whereis/1,
   successors/1,
   predecessors/1,
   sibling/2
]).

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
%%% application interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% spawn service on the cluster
-spec(spawn/2 :: (any(), any()) -> ok | {error, any()}).

spawn(Key, Service) ->
   ambit_coordinator:call(Key, {spawn, Key, Service}).

%%
%% free service on the cluster
-spec(free/1 :: (any()) -> ok | {error, any()}).

free(Key) ->
   ambit_coordinator:call(Key, {free, Key}).

%%
%% lookup service end-point
-spec(whereis/1 :: (any()) -> [pid()]).

whereis(Key) ->
   ambit_coordinator:call(Key, {whereis, Key}).

%%
%% return list of successor nodes in ambit cluster
%% @todo filter unique nodes
-spec(successors/1 :: (any()) -> [ek:vnode()]).

successors(Key) ->
   [{A, B, C, erlang:node(D)} || {A, B, C, D} <- sibling(fun ek:successors/2, Key)].

%%
%% return list of successor nodes in ambit cluster
%% @todo filter unique nodes
-spec(predecessors/1 :: (any()) -> [ek:vnode()]).

predecessors(Key) ->
   [{A, B, C, erlang:node(D)} || {A, B, C, D} <- sibling(fun ek:predecessors/2, Key)].


%%
%% return list of sibling nodes
-spec(sibling/2 :: (function(), any()) -> [ek:vnode()]).

sibling(Fun, Key) ->
   case
      lists:splitwith(
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



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   







