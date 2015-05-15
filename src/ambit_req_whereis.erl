%% @doc
%%   ambit spawn request
-module(ambit_req_whereis).
-behaviour(ambit_req).

-include("ambit.hrl").

%% api
-export([
   start_link/0,
   call/2
]).
%% request behaviour
-export([
   lease/1,
   quorum/2,
   monitor/1,
   cast/2,
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
   pq:start_link(?MODULE, [
      {type,     reusable}     
     ,{capacity, opts:val(pool, ?CONFIG_IO_POOL, ambit)}    
     ,{worker,   {ambit_req, start_link, [?MODULE]}}
   ]).

%%
%%
call(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
   ambit_req:call(?MODULE, Key, {whereis, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%%----------------------------------------------------------------------------   
%%%
%%% request
%%%
%%%----------------------------------------------------------------------------   

%%
%% lease coordinator unit-of-work, return unit-of-work descriptor
%%
-spec(lease/1 :: (ek:vnode()) -> any() | {error, any()}).

lease(Vnode) ->
   ambit_peer:coordinator(Vnode, ?MODULE).

%%
%% 
quorum(Key, Opts) ->
   Peers = ek:successors(ambit, Key),
   case 
      opts:val(r, ?CONFIG_R, Opts)
   of
      N when N > length(Peers) ->
         false;
      _ ->
         Peers
   end.

%%
%%
monitor({_, _, _, Pid}) ->
   erlang:monitor(process, Pid).

%%
%%
cast(Vnode, Req) ->
   ambit_peer:cast(Vnode, Req).

%%
%%
unit(Pid)
 when is_pid(Pid) ->
   {0, [Pid]};

unit(undefined) ->
   {0,    []};

unit({error, Reason}) ->
   {1, {error, [Reason]}}.

%%
%%
join(A, B)
 when is_list(A), is_list(B) ->
   A ++ B;
join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)}.




