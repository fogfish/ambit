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
%%   distributed actor peer - interface for v-node i/o
-module(ambit_peer).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
   %% interface
  ,coordinator/2
  ,i/1
  ,cast/2
  ,cast/3
  ,send/2
  ,send/3
]).

%% @todo: vnode management api

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
   Ring    = opts:val(name, ambit, ambit),
   {ok, _} = ek:seed(opts:val(seed, [], ambit)),
   {ok, _} = ek:create(Ring, opts:val(ring, ?CONFIG_RING, ambit)),
   Node = scalar:s(erlang:node()),
   ok   = ek:join(Ring, Node, self()),
   {ok, handle, 
      #{
         ring => Ring,
         node => Node 
      }
   }.

free(_, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% lease transaction coordinator 
-spec(coordinator/2 :: (ek:vnode(), atom()) -> any()).

coordinator(Vnode, Pool) ->
   pipe:call(ek:vnode(peer, Vnode), {coordinator, Pool}, infinity).

%%
%% get vnode status 
-spec(i/1 :: (ek:vnode()) -> any()).

i(Vnode) ->
	pipe:call(ek:vnode(peer, Vnode), {i, Vnode}).

%%
%% cast message to vnode (or actor)
-spec(cast/2 :: (ek:vnode(), any()) -> reference()).
-spec(cast/3 :: (ek:vnode(), binary(), any()) -> reference()).

cast(Vnode, Msg) ->
   pipe:cast(ek:vnode(peer, Vnode), {cast, Vnode, Msg}).

cast(Vnode, Key, Msg) ->
   pipe:cast(ek:vnode(peer, Vnode), {cast, Vnode, Key, Msg}).

%%
%% send message to vnode (or actor)
-spec(send/2 :: (ek:vnode(), any()) -> ok).
-spec(send/3 :: (ek:vnode(), binary(), any()) -> ok).

send(Vnode, Msg) ->
   pipe:send(ek:vnode(peer, Vnode), {send, Vnode, Msg}).

send(Vnode, Key, Msg) ->
   pipe:send(ek:vnode(peer, Vnode), {send, Vnode, Key, Msg}).


%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle({coordinator, Pool}, Pipe, State) ->
   pipe:ack(Pipe,
      pq:lease(Pool, [{tenant, pipe:a(Pipe)}])
   ),
   {next_state, handle, State};

handle({i, Vnode}, Pipe, State) ->
	pipe:ack(Pipe, 
      pns:whereis(vnode, ek:vnode(addr, Vnode))
   ),
   {next_state, handle, State};

%%
%%
handle({cast, Vnode, Msg}, Pipe, #{node := Node}=State) ->
   case ensure(Node, Vnode) of
      {ok,  Pid} ->
         pipe:emit(Pipe, Pid, Msg),
         {next_state, handle, State};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, State}
   end;

handle({cast, Vnode, Key, Msg}, Pipe, State) ->
   spawn(
      fun() ->
         case ambit:whereis(Vnode, Key) of
            undefined ->
               pipe:ack(Pipe, {error, noroute});
            Pid       ->
               pipe:ack(Pipe, pipe:call(Pid, Msg))
         end
      end
   ),
   {next_state, handle, State};

%%
%%
handle({send, Vnode, Msg}, Pipe, #{node := Node}=State) ->
   case ensure(Node, Vnode) of
      {ok,  Pid} ->
         pipe:emit(Pipe, Pid, Msg),
         {next_state, handle, State};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, State}
   end;

handle({send, Vnode, Key, Msg}, _Pipe, State) ->
   spawn(
      fun() ->
         case ambit:whereis(Vnode, Key) of
            undefined ->
               ok;
            Pid       ->
               pipe:send(Pid, Msg)
         end
      end
   ),
   {next_state, handle, State};

%%
%%
handle({join, Peer, _Pid}, _Tx, #{ring := Ring}=State) ->
   %% new node joined cluster, all local v-nodes needs to be checked
   %% if relocation condition is met
   ?NOTICE("ambit [peer]: join ~p", [Peer]),
	pts:foreach(fun(Addr, Pid) -> dispatch(Ring, Addr, Pid, {join, Peer}) end, vnode),
   {next_state, handle, State};

handle({handoff, _Peer}, _Tx, State) ->
   %%  peer temporary down
   ?NOTICE("ambit [peer]: handoff ~p", [_Peer]),
   % pts:foreach(fun(Addr, Pid) -> dispatch(Addr, Pid, {handoff, Peer}) end, vnode),
   {next_state, handle, State};

handle({leave, _Peer}, _Tx, State) ->
   %%  peer permanently down
   ?NOTICE("ambit [peer]: leave ~p", [_Peer]),
   % pts:foreach(fun(Addr, Pid) -> dispatch(Addr, Pid, {leave, Peer}) end, vnode),
   {next_state, handle, State};

handle(_Msg, _Pipe, State) ->
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% ensure vnode is running
ensure(_Node, Vnode) ->
   ?DEBUG("ambit [peer]: ~p ensure vnode ~p", [_Node, Vnode]),
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(vnode, Addr) of
      undefined ->
         pts:ensure(vnode, Addr, [Vnode]),
         {ok, pns:whereis(vnode, Addr)};
      Pid       ->
         {ok, Pid}
   end.

% %%
% %% lookup service hand of given Vnode
% lookup(Vnode) ->
%    pns:whereis(vnode_sys, {ek:vnode(type, Vnode), ek:vnode(addr, Vnode)}).

%%
%% dispatch cluster event to destination vnode process 
dispatch(_Ring, {_, _}, _Pid, _Msg) ->
   ok;
dispatch(Ring, _, Pid, Msg) ->
   Vnode = pipe:ioctl(Pid, vnode),
   dispatch1(Ring, ek:vnode(type, Vnode), Vnode, Msg).
   
dispatch1(Ring, primary, Vnode, {join, Peer}) ->
   Addr = ek:vnode(addr, Vnode),
   Node = ek:vnode(node, Vnode),
   List = ek:successors(Ring, Addr),
   case
      {lists:keyfind(Peer, 4, List), lists:keyfind(Node, 4, List)}
   of
      %% joined peer is not part of vnode successor list, skip to next one 
      {false,     _} ->
         ok;

      %% joined peer overtake local vnode, initiate handoff operation
      {Vn, false} ->   
         pts:send(vnode, Addr, {handoff, Vn});

      %% joined peer is sibling to local vnode, initiate node repair
      {Vn, _} ->
         pts:send(vnode, Addr, {sync, Vn})
   end;

dispatch1(Ring, handoff, Vnode, {join, Peer}) ->
   Addr = ek:vnode(addr, Vnode),
   Node = ek:vnode(node, Vnode),
   List = ek:successors(Ring, Addr),
   case
      {lists:keyfind(Peer, 4, List), Node =:= Peer}
   of
      %% joined peer is not part of vnode successor list, skip to next one 
      {false, _} ->
         ok;

      %% joined peer overtake local hints, initiate handoff operation
      {Vn, true} ->   
         pts:send(vnode, Addr, {handoff, Vn});
      
      _ ->
         ok
   end.


