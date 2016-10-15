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
%%   distributed actors
-module(ambit).
-include("ambit.hrl").

%% @todo: 
%%
%%   * this is bad idea to address vnode services via node type, 
%%     we cannot distinguish them e.g. primary aae / handoff aae -> rework
%% 
%%   * write-repair (if handoff node do not exist during write)
%%     a) make N-primary shard
%%     b) spawn actor + i/o data
%%     c) kill one primary shard
%%     d) i/o data, handoff node misses actor (actor needs to be spawned)

-export([behaviour_info/1]).
-export([start/0]).
-export([
   cast/2, cast/3,
   send/2, send/3,
   call/2, call/3,

   whereis/2,
   successors/2,
   predecessors/2,
   i/2
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
      %% return pid of actor process 
      %%
      %% -spec(process/2 :: (pid(), atom()) -> {ok, any()} | {error, any()}).
      % {process,   1}

      %%
      %% initiate actor handoff procedure
      %%
      %% -spec(handoff/2 :: (pid(), ek:vnode()) -> ok).
      {handoff,   2}

      %%
      %% initiate actor repair procedure
      %%
      %% -spec(sync/2 :: (pid(), ek:vnode()) -> ok).
     ,{sync,      2}
   ];
behaviour_info(_) ->
   undefined.

%%%----------------------------------------------------------------------------   
%%%
%%% application interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% RnD application start
start() ->
   applib:boot(?MODULE, code:where_is_file("app.config")).
 

%%
%% cast message to v-node using cluster fabric
-spec cast(ek:vnode(), _) -> reference().

cast(Vnode, Msg) ->
   pipe:cast(ek:vnode(peer, Vnode), {request, Vnode, Msg}).


%%
%% cast message to actor using cluster fabric
-spec cast(ek:vnode(), _, _) -> reference().

cast(Vnode, Key, Msg) ->
   pipe:cast(ek:vnode(peer, Vnode), {request, Vnode, Key, Msg}).


%%
%% send message to v-node using cluster fabric
-spec send(ek:vnode(), _) -> ok.

send(Vnode, Msg) ->
   pipe:send(ek:vnode(peer, Vnode), {request, Vnode, Msg}).


%%
%% send message to actor using cluster fabric
-spec send(ek:vnode(), _, _) -> reference().

send(Vnode, Key, Msg) ->
   pipe:send(ek:vnode(peer, Vnode), {request, Vnode, Key, Msg}).


%%
%% request v-node using cluster fabric
-spec call(ek:vnode(), _) -> {ok, _} | {error, _}.

call(Vnode, Msg) ->
   pipe:call(ek:vnode(peer, Vnode), {request, Vnode, Msg}, ?CONFIG_TIMEOUT_REQ).


%%
%% send message to actor using cluster fabric
-spec call(ek:vnode(), _, _) -> {ok, _} | {error, _}.

call(Vnode, Key, Msg) ->
   pipe:call(ek:vnode(peer, Vnode), {request, Vnode, Key, Msg}, ?CONFIG_TIMEOUT_REQ).








%%
%% utility function to lookup service processes on local node
%%  
-spec whereis(atom() | ek:vnode(), any()) -> pid() | undefined.

whereis(Ring, Key)
 when is_atom(Ring) ->
   whereis(hd(ek:successors(Ring, Key)), Key);

whereis(Vnode, Key) ->
   whereis(ek:vnode(addr, Vnode), ek:vnode(peer, Vnode), Key).

whereis(Addr, Pid, Key)
 when erlang:node(Pid) =:= erlang:node() ->
   pns:whereis(ambit, {Addr, Key});
whereis(_, _, _) ->
   undefined.

%%
%% return list of successor nodes in ambit cluster
-spec successors(atom(), any()) -> [ek:vnode()].

successors(Ring, Key) ->
   [ek:vnode(peer, erlang:node(ek:vnode(peer, X)), X) || X <- ek:successors(Ring, Key)].

%%
%% return list of successor nodes in ambit cluster
-spec predecessors(atom(), any()) -> [ek:vnode()].

predecessors(Ring, Key) ->
   [ek:vnode(peer, erlang:node(ek:vnode(peer, X)), X) || X <- ek:predecessors(Ring, Key)].

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
%% @todo:
%%    vnode capacity
i(Ring, alive) ->
	[{X, length(Y)} || X <- ek:address(Ring), Y <- [i(Ring, X)], length(Y) =/= 0];

i(Ring, alloc) ->
	lists:foldl(
		fun(X, Acc) -> orddict:update_counter(X, 1, Acc) end,
		orddict:new(),
		[ek:vnode(node, Y) || X <- ek:address(Ring), Y <- i(Ring, X)]
	);

i(Ring, Addr) ->
	[X || X <- ek:successors(Ring, Addr), ambit_peer:i(X) =/= undefined].

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

