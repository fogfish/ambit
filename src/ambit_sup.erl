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

            %%
            %% vnode management pool
           ,?CHILD(supervisor, vnode, pts, [vnode, ?HEAP_VNODE])

            %%
            %% peer manager
           ,?CHILD(worker,     ambit_peer)
         ]
      }
   }.
