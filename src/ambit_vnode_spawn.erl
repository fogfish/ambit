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
%%   virtual node - actor spawner interface
-module(ambit_vnode_spawn).
% -behaviour(pipe).

% -include("ambit.hrl").
% -include_lib("ambitz/include/ambitz.hrl").

% -export([
%    start_link/1
%   ,init/1
%   ,free/2
%   ,ioctl/2
%   ,handle/3
% ]).

% %%%----------------------------------------------------------------------------   
% %%%
% %%% Factory
% %%%
% %%%----------------------------------------------------------------------------   

% start_link(Vnode) ->
%    pipe:start_link(?MODULE, [Vnode], []).

% init([Vnode]) ->
%    ?DEBUG("ambit [spawn]: init ~p", [Vnode]),
%    Addr = ek:vnode(addr, Vnode),
%    pipe:ioctl(pns:whereis(vnode, Addr), {spawn, self()}),
%    {ok, handle, Vnode}.

% free(_, _Vnode) ->
%    ok.

% ioctl(_, _) ->
%    throw(not_implemented).

% %%%----------------------------------------------------------------------------   
% %%%
% %%% pipe
% %%%
% %%%----------------------------------------------------------------------------   

% %%
% %%
% handle({'$ambitz', spawn, #entity{key = Key}=Entity}, Pipe, Vnode) ->
%    Pid = ambit_actor:spawn(Vnode, Key),
%    pipe:emit(Pipe, Pid, {'$ambitz', spawn, Entity}),
%    {next_state, handle, Vnode};
%    % Addr = ek:vnode(addr, Vnode),
%    % case pts:ensure(Addr, Key, [Vnode]) of
%    %    {ok,  _} ->
%    %       pipe:a(Pipe, 
%    %          pts:call(Addr, Key, {'$ambitz', spawn, Entity})
%    %       ),
%    %       {next_state, handle, Vnode};
%    %    {error, _} = Error ->
%    %       pipe:a(Pipe, Error),
%    %       {next_state, handle, Vnode}
%    % end;

% handle({'$ambitz', free, #entity{key = Key}=Entity}, Pipe, Vnode) ->
%    Addr = ek:vnode(addr, Vnode),
%    case pts:call(Addr, Key, {'$ambitz', free, Entity}) of
%       %% not found is not a critical error, ping back entity
%       {error, not_found} ->
%          pipe:a(Pipe, {ok, Entity}),
%          {next_state, handle, Vnode};
%       Result ->
%          pipe:a(Pipe, Result),
%          {next_state, handle, Vnode}
%    end;

% handle({'$ambitz', lookup, #entity{key = Key}=Entity}, Pipe, Vnode) ->
%    Addr = ek:vnode(addr, Vnode),
%    case pts:call(Addr, Key, {'$ambitz', lookup, Entity}) of
%       %% not found is not a critical error, ping back entity
%       {error, not_found} ->
%          pipe:a(Pipe, {ok, Entity}),
%          {next_state, handle, Vnode};
%       Result ->
%          pipe:a(Pipe, Result),
%          {next_state, handle, Vnode}
%    end;

% handle({'$ambitz', whereis, #entity{key = Key}=Entity}, Pipe, Vnode) ->
%    Pid = ambit_actor:spawn(Vnode, Key),
%    pipe:emit(Pipe, Pid, {'$ambitz', whereis, Entity}),
%    {next_state, handle, Vnode};
%    % Addr   = ek:vnode(addr, Vnode),
%    % Result = pts:call(Addr, Key, {'$ambitz', whereis, Entity}),
%    % pipe:a(Pipe, Result),
%    % {next_state, handle, Vnode};

% % handle({whereis, #entity{key = Key}}, Pipe, Vnode) ->
% %    Addr = ek:vnode(addr, Vnode),
% %    pipe:a(Pipe, 
% %       pns:whereis(ambit, {Addr, Key})
% %    ),
% %    {next_state, handle, Vnode};

% handle(_, _Tx, Vnode) ->
%    {next_state, handle, Vnode}.



% %%%----------------------------------------------------------------------------   
% %%%
% %%% private
% %%%
% %%%----------------------------------------------------------------------------   

