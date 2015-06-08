%% @doc
%%   ambit spawn request
-module(ambit_req_remove).
-behaviour(ambitz).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

%% api
-export([
   start_link/0
   % call/2
]).
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

%%
%%
% call(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
%    ambitz:call(?MODULE, Key, {remove, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

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
cast(Vnode, _Key, Req, _Opts) ->
   ambit_peer:cast(Vnode, Req).

%%
%%
unit({ok, #entity{val = Value}=Entity}) ->
   {erlang:phash2(Value), Entity};

unit({error, Reason}) ->
   {0, {error, [Reason]}}.

%%
%%
join(#entity{val=Val, vsn=VsnA}, #entity{val=Val, vsn=VsnB}=B) ->
   B#entity{vsn = uid:join(VsnB, VsnA)};

join({error, A}, {error, B}) ->
   {error, lists:usort(A ++ B)}.




