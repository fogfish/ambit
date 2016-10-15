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

%%
%% default ring
-ifndef(CONFIG_RING).
-define(CONFIG_RING, [
   {type, ring}            %% ring management algorithms - chord | ring
  ,{m,      64}            %% ring modulo
  ,{q,    4096}            %% number of shards
  ,{n,       3}            %% number of replica
  ,{hash,  sha}            %% hashing algorithm
]).
-endif.

%%
%% default R/W parameters
-define(CONFIG_W,      1).
-define(CONFIG_R,      1).
-define(CONFIG_N,      1).

%%
%% heap of vnode processes
% -define(HEAP_VNODE, [
%    {keylen,      inf}
%   ,{supervisor,  ambit_vnode_sup}
%   ,{factory,     transient}
% ]).

%%
%% heap of actor processes
% -define(HEAP_ACTOR, [
%    {keylen,      inf}
%   ,{entity,      ambit_actor}
%   ,{factory,     temporary}
% ]).

%%
%% is request to spawn hand
-define(is_spawn(X), (
   X =:= create  orelse 
   X =:= remove  orelse 
   X =:= lookup  orelse 
   X =:= process orelse 
   X =:= whereis
)).


%%
%% size of ambit api request pool (capacity of coordinator)
-ifndef(CONFIG_IO_POOL).
-define(CONFIG_IO_POOL,  100).
-endif.

%%
%% request timeout in milliseconds
-ifndef(CONFIG_TIMEOUT_REQ).
-define(CONFIG_TIMEOUT_REQ,  5000).
-endif.

%%
%% active anti entropy configuration
%%  * frequency in milliseconds
%%  * capacity number of parallel sessions
-ifndef(CONFIG_AAE_TIMEOUT).
-define(CONFIG_AAE_TIMEOUT,   {120000, 1.0}).
-endif.

-ifndef(CONFIG_AAE_CAPACITY).
-define(CONFIG_AAE_CAPACITY,  5).
-endif.


%% 
%% logger macros
%%   debug, info, notice, warning, error, critical, alert, emergency
-ifndef(EMERGENCY).
-define(EMERGENCY(Fmt, Args), lager:emergency(Fmt, Args)).
-endif.

-ifndef(ALERT).
-define(ALERT(Fmt, Args), lager:alert(Fmt, Args)).
-endif.

-ifndef(CRITICAL).
-define(CRITICAL(Fmt, Args), lager:critical(Fmt, Args)).
-endif.

-ifndef(ERROR).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-endif.

-ifndef(WARNING).
-define(WARNING(Fmt, Args), lager:warning(Fmt, Args)).
-endif.

-ifndef(NOTICE).
-define(NOTICE(Fmt, Args), lager:notice(Fmt, Args)).
-endif.

-ifndef(INFO).
-define(INFO(Fmt, Args), lager:info(Fmt, Args)).
-endif.

-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), lager:debug(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.
