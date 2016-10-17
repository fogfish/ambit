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
-module(ambit_req_create).
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
   cast/4,
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
cast(Vnode, _Key, Req, _Opts) ->
   ambit:cast(Vnode, Req).

%%
%%
unit({ok, Entity}) ->
   {erlang:phash2(ambitz:get(Entity)), {ok, Entity}};

unit({error, Reason}) ->
   {0, {error, [Reason]}}.

%%
%%
join({ok, EntityA}, {ok, EntityB}) ->
   Vnode = ambitz:vnode(EntityA) ++ ambitz:vnode(EntityB),
   {ok, ambitz:vnode(Vnode, ambitz:join(EntityA, EntityB))};
   % #entity{val=Val, vsn=VsnA, vnode = VnodeA}}, {ok, #entity{val=Val, vsn=VsnB, vnode = VnodeB}=B}) ->
   % {ok, B#entity{vsn = uid:join(VsnB, VsnA), vnode = VnodeB ++ VnodeA}};

join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)}.




