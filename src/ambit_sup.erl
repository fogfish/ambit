-module(ambit_sup).
-behaviour(supervisor).

-include("ambit.hrl").

-export([
   start_link/0, init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

% -define(VNODE, [
%    'read-through'
%   ,{keylen,   inf}
%   ,{entity,   ambit_vnode}
%   ,{factory,  transient}
% ]).

% -define(POOL, [
%    {type,     reusable}     
%   ,{capacity, opts:val(pool, ?CONFIG_POOL_REQ, ambit)}    
%   ,{worker,   ambit_req}
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
            ?CHILD(worker,     ambit_peer)
           % ,?CHILD(supervisor, pq,  [ambit_req, ?POOL])
           ,?CHILD(supervisor, vnode, pts, [vnode, ?HEAP_VNODE])
         ]
      }
   }.
