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
%% @doc
%%   ambit spawn transaction
-module(ambit_req_put).
-behaviour(ambitz).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

%% api
-export([start_link/0]).
%% request behaviour
-export([
   ensure/3,
   guid/1,
   monitor/1,
   cast/3,
   unit/1,
   join/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% 
start_link() ->
   ambitz:start_link(?MODULE, opts:val(pool, ?CONFIG_IO_POOL, ambit)).

%%%----------------------------------------------------------------------------   
%%%
%%% request
%%%
%%%----------------------------------------------------------------------------   

%%
%%
ensure(_Peers, _Key, _Opts) ->
   ok.

%%
%% generate globally unique transaction id
guid(_) ->
   undefined.

%%
%%
monitor(Vnode) ->
   erlang:monitor(process, ek:vnode(peer, Vnode)). 

%%
%% 
cast(Vnode, Entity, Opts) ->
   {lens, Lens} = lists:keyfind(lens, 1, Opts),
   ambit:cast(Vnode, {'$ambitz', {put, Lens}, Entity}).

%%
%%
unit({ok, Entity}) ->
   {1, {ok, Entity}};

unit({error, Reason}) ->
   {0, {error, [Reason]}}.

%%
%%
join({ok, #entity{vnode = VnodeA, val = A} = EntityA}, {ok, #entity{vnode = VnodeB, val = B}}) ->
   {ok, EntityA#entity{vnode = VnodeA ++ VnodeB, val = crdts:join(A, B)}};

join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)}.




