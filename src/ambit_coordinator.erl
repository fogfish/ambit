%% @description
%%   request coordinator process
-module(ambit_coordinator).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,local/3
  ,domestic/3
  ,foreign/3
   % api
  % ,bind/1
  ,call/2
  ,call/3
  ,cast/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link(?MODULE, [], []).

init(_) ->
   {ok, idle, #{}}.

free(_, _) ->
   ok.

ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   


%%
%% request coordinator
-spec(call/2 :: (any(), any()) -> {ok, any()} | {error, any()}).
-spec(call/3 :: (any(), any(), timeout()) -> {ok, any()} | {error, any()}).

call(Key, Req) ->
   call(Key, Req, 5000).

call(Key, Req, Timeout) ->
   {Pid, Vnode, Peers} = whois(Key),
   Pool = pq:lease(Pid),
   pipe:call(pq:pid(Pool), 
      #{
         is    => global,
         vnode => Vnode, 
         peers => Peers, 
         pool  => Pool,
         req   => Req, 
         t => Timeout
      }
   ).

%%
%% cast message to coordinator
-spec(cast/2 :: (ek:vnode(), any()) -> reference()).

cast({_, _, _, Pid}=Vnode, Req) ->
   Pool = pq:lease(Pid),
   pipe:cast(pq:pid(Pool), 
      #{
         is    => local,
         vnode => Vnode, 
         pool  => Pool, 
         req   => Req, 
         t     => infinity
      }
   ).

% cast(Key, Req) ->


% * global cast as coordinator and domestic-only cast (local)
% * todo lease multiple services from same pool


% %%
% %% bind process to foreign coordinator
% -spec(bind/1 :: (ek:vnode()) -> any()).

% bind({_, _, _, Pid}) ->
%    pq:lease(Pid).



% call(Vnode, Key, Req) ->
%    call(Vnode, Key, Req, 5000).

% call(Vnode, Key, Req, Timeout) ->
%    Tx = bind(Vnode),
%    try
%       pipe:call(pq:pid(Tx), {call, Vnode, Key, Req, Timeout}, Timeout) 
%    after
%       pq:release(Tx)
%    end.

% %%
% %%


%%%----------------------------------------------------------------------------   
%%%
%%% fsm
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle(#{is := global, vnode := Vnode, req := _Req} = State, Pipe, _State) ->
   ?DEBUG("ambit [coord]: global req ~p", [_Req]),
   case ensure(Vnode) of
      {ok, _} ->
         {next_state, domestic, req_self_vnode(State#{pipe => Pipe})};
      {error, _} = Error ->
         {next_state, idle, req_failed(Error, State#{pipe => Pipe})}
   end;

idle(#{is := local, vnode := Vnode, req := _Req} = State, Pipe, _State) ->
   ?DEBUG("ambit [coord]: local req ~p", [_Req]),
   case ensure(Vnode) of
      {ok, _} ->
         {next_state, local, req_self_vnode(State#{pipe => Pipe})};
      {error, _} = Error ->
         {next_state, idle, req_failed(Error, State#{pipe => Pipe})}
   end;

idle(_, _, State) ->
   {next_state, idle, State}.

%%
%%
local({Tx, Value}, _, #{tx := Tx}=State) ->
   {next_state, idle, req_success(Value, State)};

local(timeout, _, State) ->
   {stop, timeout, req_failed({error, timeout}, State)};

local(_, _, State) ->
   {next_state, idle, State}.


%%
%% execute domestic 
domestic({Tx, Value}, _, #{tx := Tx, peers := []}=State) ->
   {next_state, idle, req_success(Value, State)};

domestic({Tx,_Value}, _, #{tx := Tx, t := T}=State) ->
   %% domestic request successes 
   _ = tempus:cancel(T),
   {next_state, foreign, req_peer_vnode(State)};

domestic(timeout, _, State) ->
   {stop, timeout, State};

domestic(_, _, State) ->
   {next_state, idle, State}.

%%
%%
foreign(M, _, State) ->
   io:format("=> ~p~n", [M]),
   {next_state, foreign, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% who is responsible to coordinate key and execute transactions
whois(Key) ->
   Peers = ek:successors(ambit, Key),
   case lists:dropwhile(fun({X, _, _, _}) -> X =/= primary end, Peers) of
      [] ->
         {_, _, _, Pid} = Vnode = hd(Peers),
         Node = erlang:node(Pid),
         {Pid, Vnode, [X || {_, _, _, P} = X <- Peers, erlang:node(P) =/= Node]};
      L  ->
         {_, _, _, Pid} = Vnode = hd(L),
         Node = erlang:node(Pid),
         {Pid, Vnode, [X || {_, _, _, P} = X <- Peers, erlang:node(P) =/= Node]}
   end.

%%
%% lookup service hand of given Vnode
lookup({Hand,  Addr, _, _}) ->
   pns:whereis(vnode, {Hand, Addr}).

%%
%% ensure Vnode presence
ensure({_Hand, Addr, _, _}) ->
   pts:ensure(vnode, Addr).


%%
%% request is failed
req_failed(Error, #{pool := Pool, pipe := Pipe}) ->
   pipe:a(Pipe, Error),
   pq:release(Pool),
   #{}.

%%
%% request is completed
req_success(Value, #{pool := Pool, pipe := Pipe, t := T}) ->
   _ = tempus:cancel(T),
   pipe:a(Pipe, Value),
   pq:release(Pool),
   #{}.

%%
%% request service from vnode executed locally
req_self_vnode(#{vnode := Vnode, req := Req, t := T0}=State) ->
   ?DEBUG("ambit [coord]: cast ~p ~p", [Vnode, Req]),
   Pid = lookup(Vnode),
   Tx  = pipe:cast(Pid, {Vnode, Req}),
   T   = tempus:timer(T0, timeout),
   State#{tx => Tx, t => T}.

%%
%% request service from vnode executed remotely
req_peer_vnode(#{peers := Peers, req := Req, t := T0}=State) ->
   Tx = lists:map(
      fun(Vnode) ->
         ?DEBUG("ambit [coord]: cast ~p ~p", [Vnode, Req]),
         ambit_coordinator:cast(Vnode, Req)
      end,
      Peers
   ),
   T   = tempus:timer(T0, timeout),
   State#{tx => Tx, t => T}.



