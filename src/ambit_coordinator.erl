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
  ,call/2
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

call(Key, Req0) ->
   {Pid, Req1} = ambit_req:whois(Key, Req0),
   {UoW, Req2} = ambit_req:lease(Pid, Req1),
   pipe:call(UoW, Req2).

%%
%% cast message to coordinator
-spec(cast/2 :: (ek:vnode(), any()) -> reference()).

cast({_, _, _, Pid} = Vnode, #{msg := Msg})
 when erlang:node(Pid) =:= erlang:node() ->
   ?DEBUG("ambit [coord]: cast ~p ~p", [Msg, Vnode]),
   pipe:cast(lookup(Vnode), {Vnode, Msg});

cast({_, _, _, Pid} = Vnode, #{msg := _Msg} = Req0) ->
   ?DEBUG("ambit [coord]: cast ~p ~p", [_Msg, Vnode]),
   {Pid, Req1} = ambit_req:whois(Vnode, Req0),
   {UoW, Req2} = ambit_req:lease(Pid,   Req1),
   pipe:cast(UoW, Req2).

%%%----------------------------------------------------------------------------   
%%%
%%% fsm
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle(#{mod := _, msg := _Msg} = Req0, Pipe, _State) ->
   ?DEBUG("ambit [coord]: global req ~p", [_Msg]),
   case ensure(ambit_req:vnode(Req0)) of
      {ok, _} ->
         %% @todo: validate quorum
         {next_state, domestic, 
            req_this_vnode(ambit_req:pipe(Pipe, Req0))
         };
      {error, _} = Error ->
         {_, Req1} = ambit_req:accept(Error, 
            ambit_req:pipe(Pipe, Req0)
         ),
         {next_state, idle, ambit_req:free(Req1)}
   end;

idle(#{msg := _Msg} = Req0, Pipe, _State) ->
   ?DEBUG("ambit [coord]: local req ~p", [_Msg]),
   case ensure(ambit_req:vnode(Req0)) of
      {ok, _} ->
         {next_state, domestic, 
            req_this_vnode(ambit_req:pipe(Pipe, Req0))
         };
      {error, _} = Error ->
         {_, Req1} = ambit_req:accept(Error, 
            ambit_req:pipe(Pipe, Req0)
         ),
         {next_state, idle, ambit_req:free(Req1)}
   end;

idle(_, _, State) ->
   {next_state, idle, State}.

%%
%%
local({Tx, Value}, _, #{tx := Tx}=Req0) ->
   {_, Req1} = ambit_req:accept(Value, Req0),
   {next_state, idle, ambit_req:free(Req1)};

% local(timeout, _, State) ->
%    {stop, timeout, req_commit({error, timeout}, State)};

local(_, _, State) ->
   {next_state, idle, State}.


%%
%% execute domestic 
domestic({Tx, Value}, _,  {[{Tx, Vnode}], Req0}) ->
   Req1 = ambit_req:accept(Value, Req0),
   case ambit_req:peers(Req1) of
      [Vnode] ->
         {next_state, idle, ambit_req:free(Req1)};
      _       ->
         {next_state, foreign, req_peer_vnode(Req1)}
   end;

% domestic(timeout, _, State) ->
%    {stop, timeout, State};

domestic(_, _, State) ->
   {next_state, idle, State}.

%%
%%
foreign({Tx, Value}, _, {List, Req0}) ->
   Req1 = ambit_req:accept(Value, Req0),
   case lists:keytake(Tx, 1, List) of
      {value, {_Tx, _Vnode},   []} ->
         {next_state, idle, ambit_req:free(Req1)};
      {value, {_Tx, _Vnode}, Tail} ->
         {next_state, foreign, {Tail, Req1}}
   end;

foreign(M, _, State) ->
   io:format("=> ~p~n", [M]),
   {next_state, foreign, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% lookup service hand of given Vnode
lookup({Hand,  Addr, _, _}) ->
   pns:whereis(vnode, {Hand, Addr}).

%%
%% ensure Vnode presence
ensure({_Hand, Addr, _, _}) ->
   pts:ensure(vnode, Addr).

%%
%% request service from vnode executed locally
req_this_vnode(Req) ->
   Vnode = hd(ambit_req:peers(Req)),
   Tx    = ambit_coordinator:cast(Vnode, Req),
   {[{Tx, Vnode}], ambit_req:t(timeout, Req)}.


%%
%% request service from vnode executed remotely
req_peer_vnode(Req) ->
   List = lists:map(
      fun(Vnode) ->
         Tx = ambit_coordinator:cast(Vnode, ambit_req:new(Req)),
         {Tx, Vnode}
      end,
      tl(ambit_req:peers(Req))
   ),
   {List, ambit_req:t(timeout, Req)}.




