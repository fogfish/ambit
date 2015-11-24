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
-module(ambit_sup).
-behaviour(supervisor).

-include("ambit.hrl").

-export([
   start_link/0, init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, infinity, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, infinity, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, infinity, Type, dynamic}).

% -define(POOL(X), [
%    {type,     reusable}     
%   ,{capacity, opts:val(pool, ?CONFIG_IO_POOL, ambit)}    
%   ,{worker,   X}    
% ]).

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------

start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).
   
init([]) ->   
   {ok,
      {
         {one_for_one, 4, 1800},
         [
 				%%
				%% transaction pool(s) 
			   ?CHILD(worker, ambit_req_create)
           ,?CHILD(worker, ambit_req_remove)
           ,?CHILD(worker, ambit_req_lookup)
           ,?CHILD(worker, ambit_req_whereis)
           ,?CHILD(worker, ambit_req_call)

            %%
            %% vnode management pool
           ,?CHILD(supervisor, vnode, pts, [vnode, ?HEAP_VNODE])

            %%
            %% peer manager
           ,?CHILD(worker,     ambit_peer)
         ]
      }
   }.
