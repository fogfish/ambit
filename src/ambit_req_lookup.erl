%% @doc
%%   ambit spawn request
-module(ambit_req_lookup).
-behaviour(ambit_req).

-include("ambit.hrl").

%% api
-export([
   start_link/0,
   call/2
]).
%% request behavior
-export([
   % lease/1,
   % quorum/2,
   ensure/3,
   guid/1,
   monitor/1,
   cast/3,
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
   ambit_req:call(?MODULE, Key, {lookup, Entity#entity{vsn = uid:vclock(Vsn)}}, Opts).

%%%----------------------------------------------------------------------------   
%%%
%%% request
%%%
%%%----------------------------------------------------------------------------   

% %%
% %% lease coordinator unit-of-work, return unit-of-work descriptor
% %%
% -spec(lease/1 :: (ek:vnode()) -> any() | {error, any()}).

% lease(Vnode) ->
%    ambit_peer:coordinator(Vnode, ?MODULE).

% %%
% %% 
% quorum(_Key, Opts) ->
%    Peers = opts:val(peers, [], Opts),
%    case 
%       opts:val(r, ?CONFIG_R, Opts)
%    of
%       N when N > length(Peers) ->
%          false;
%       _ ->
%          Peers
%    end.

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
cast(Vnode, _Tx, Req) ->
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




