%% @doc
%%   ambit spawn request
-module(ambit_req_whereis).
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
   pq:start_link(?MODULE, [
      {type,     reusable}     
     ,{capacity, opts:val(pool, ?CONFIG_IO_POOL, ambit)}    
     ,{worker,   {ambitz, start_link, [?MODULE]}}
   ]).

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
cast(Vnode, _Key, Req, _Opts) ->
   ambit_peer:cast(Vnode, Req).

%%
%%
unit({ok, #entity{val = Value}=Entity}) ->
   {1, Entity};

unit({error, Reason}) ->
   {0, {error, [Reason]}}.

%%
%%
join(#entity{val=ValA, vsn=VsnA, vnode = VnodeA}, #entity{val=ValB, vsn=VsnB, vnode = VnodeB}=B) ->
   B#entity{val=ValB ++ ValA, vsn = uid:join(VsnB, VsnA), vnode = VnodeB ++ VnodeA};

join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)}.




