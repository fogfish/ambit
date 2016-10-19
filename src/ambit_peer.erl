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
-spec coordinator(ek:vnode(), atom()) -> any().

coordinator(Vnode, Pool) ->
   pipe:call(ek:vnode(peer, Vnode), {coordinator, Pool}, infinity).

%%
%% get vnode status 
-spec i(ek:vnode()) -> any().

i(Vnode) ->
	pipe:call(ek:vnode(peer, Vnode), {i, Vnode}).


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
%% pipe message
handle({request, Vnode, Msg}, Pipe, State) ->
   Pid = ambit_vnode:spawn(Vnode),
   pipe:emit(Pipe, Pid, Msg),
   {next_state, handle, State};


handle({request, Vnode, Key, Msg}, Pipe, State) ->
   spawn(
      fun() ->
         pipe:ack(Pipe, ambit_actor:call(Vnode, Key, Msg))
      end
   ),
   {next_state, handle, State};

%%
%%
handle({join, Peer, _Pid}, _Tx, State) ->
   %% new node joined cluster, all local v-nodes needs to be checked
   %% for relocation/handoff policy 
   ?NOTICE("ambit [peer]: join ~p", [Peer]),
   ambit_vnode:foreach(
      fun(VnodeA) ->
         case ambit_vnode:handoff(Peer, VnodeA) of
            {replica, VnodeB} -> ambit_vnode:send(VnodeA, {sync, VnodeB});
            {handoff, VnodeB} -> ambit_vnode:send(VnodeA, {handoff, VnodeB});
            undefined         -> ok
         end   
      end
   ),
	% pts:foreach(fun(Addr, Pid) -> dispatch(Ring, Addr, Pid, {join, Peer}) end, vnode),
   {next_state, handle, State};

handle({handoff, Peer}, _Tx, State) ->
   %%  peer temporary down
   ?NOTICE("ambit [peer]: handoff ~p", [Peer]),
   % pts:foreach(fun(Addr, Pid) -> dispatch(Ring, Addr, Pid, {handoff, Peer}) end, vnode),
   {next_state, handle, State};

handle({leave, Peer}, _Tx, State) ->
   %%  peer permanently down
   ?NOTICE("ambit [peer]: leave ~p", [Peer]),
   % pts:foreach(fun(Addr, Pid) -> dispatch(Ring, Addr, Pid, {leave, Peer}) end, vnode),
   {next_state, handle, State};

handle(_Msg, _Pipe, State) ->
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

