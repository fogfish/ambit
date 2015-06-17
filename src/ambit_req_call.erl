%% @doc
%%   ambit call transaction
-module(ambit_req_call).
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
monitor({_, _, _, Pid}) ->
   erlang:monitor(process, Pid). 

%%
%% 
cast(Vnode, Key, Req, _Opts) ->
   ambit_peer:cast(Vnode, Key, Req).

%%
%%
unit({error, Reason}) ->
   {0, {error, [Reason]}};

unit(Value) ->
   {erlang:phash2(Value), Value}.


%%
%%
join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)};

join(_, Value) ->
   Value.





