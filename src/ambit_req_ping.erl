%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
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
%%   ambit ping request (cluster topology debug)
-module(ambit_req_ping).
-behaviour(ambitz).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

%% api
-export([start_link/0]).
%% request behavior
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
   % pq:start_link(?MODULE, [
   %    {type,     reusable}     
   %   ,{capacity, opts:val(pool, ?CONFIG_IO_POOL, ambit)}    
   %   % ,{worker,   {ambitz, start_link, [?MODULE]}}
   %   ,{worker,   {ambitz, [?MODULE]}}
   % ]).

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
-spec(guid/1 :: (any()) -> any()).

guid(_) ->
   undefined.

%%
%%
monitor(Vnode) ->
   erlang:monitor(process, ek:vnode(peer, Vnode)).

%%
%%
cast(Vnode, Key, Req, _Opts) ->
   % Ref = erlang:make_ref(),
   % erlang:spawn(erlang:node(ek:vnode(peer, Vnode)), ambit, xxx, [{Ref, self()}]),
   % Ref.
   % io:format("==> ~p~n", [Vnode]),
   ambit_peer:cast(Vnode, Key, Req).

%%
%%
unit({ok, List}) ->
   {1, {ok, List}};

unit({error, Reason}) ->
   {0, {error, [Reason]}}.

%%
%%
join({ok, A}, {ok, B}) ->
   {ok, A ++ B};

join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)}.




