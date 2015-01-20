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
  ,domestic/3
  ,foreign/3
   % api
  ,call/2
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
   Req1 = ambit_req:new(Req0),
   {Pid, Req2} = ambit_req:whois(Key, Req1),
   {UoW, Req3} = ambit_req:lease(Pid, Req2),
   pipe:call(UoW, Req3).


%%%----------------------------------------------------------------------------   
%%%
%%% fsm
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle(#{msg := _Msg} = Req0, Pipe, _State) ->
   ?DEBUG("ambit [coord]: global req ~p", [_Msg]),
   case ensure(ambit_req:vnode(Req0)) of
      {ok, _} ->
         %% @todo: validate quorum N + W property
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
%% execute domestic 
domestic({Tx, Value}, _,  {[{Tx, Vnode}], Req0}) ->
   Req1 = ambit_req:accept(Value, Req0),
   case ambit_req:peers(Req1) of
      [Vnode] ->
         {next_state, idle, ambit_req:free(Req1)};
      _       ->
         {next_state, foreign, req_peer_vnode(Req1)}
   end;

domestic(timeout, _, {_, Req0}) ->
   Req1 = ambit_req:accept({error, timeout}, Req0),
   {stop, normal, ambit_req:free(Req1)};

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

foreign(timeout, _, {_, Req0}) ->
   Req1 = ambit_req:accept({error, timeout}, Req0),
   {stop, normal, ambit_req:free(Req1)};

foreign(_, _, State) ->
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
   Tx    = pipe:cast(lookup(Vnode), ambit_req:payload(Req)),
   {[{Tx, Vnode}], ambit_req:t(timeout, Req)}.


%%
%% request service from vnode executed remotely
req_peer_vnode(Req) ->
   List = lists:map(
      fun(Vnode) ->
         Tx = ambit_peer:cast(Vnode, ambit_req:payload(Req)),
         {Tx, Vnode}
      end,
      tl(ambit_req:peers(Req))
   ),
   {List, ambit_req:t(timeout, Req)}.




