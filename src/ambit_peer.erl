%% @description
%%   distributed actor peer - interface for v-node i/o
-module(ambit_peer).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
   %% interface
  ,coordinator/2
  ,i/1
  ,cast/2
  ,cast/3
  ,send/2
  ,send/3
]).

%% @todo: vnode management api

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
   {ok, _} = ek:seed(opts:val(seed, [], ambit)),
   {ok, _} = ek:create(ambit, opts:val(ring, ?CONFIG_RING, ambit)),
   Node = scalar:s(erlang:node()),
   ok   = ek:join(ambit, Node, self()),
   {ok, handle, 
      #{
         node => Node 
      }
   }.

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
%% lease transaction coordinator 
-spec(coordinator/2 :: (ek:vnode(), atom()) -> any()).

coordinator({_, _, _, Pid}, Pool) ->
   pipe:call(Pid, {coordinator, Pool}, infinity).

%%
%% get vnode status 
-spec(i/1 :: (ek:vnode()) -> any()).

i({_, _, _, Peer} = Vnode) ->
	pipe:call(Peer, {i, Vnode}).

%%
%% cast message to vnode (or actor)
-spec(cast/2 :: (ek:vnode(), any()) -> reference()).
-spec(cast/3 :: (ek:vnode(), binary(), any()) -> reference()).

cast({_, _, _, Pid} = Vnode, Msg) ->
   pipe:cast(Pid, {cast, Vnode, Msg}).

cast({_, _, _, Pid} = Vnode, Key, Msg) ->
   pipe:cast(Pid, {cast, Vnode, Key, Msg}).

%%
%% send message to vnode (or actor)
-spec(send/2 :: (ek:vnode(), any()) -> ok).
-spec(send/3 :: (ek:vnode(), binary(), any()) -> ok).

send({_, _, _, Pid} = Vnode, Msg) ->
   pipe:send(Pid, {send, Vnode, Msg}).

send({_, _, _, Pid} = Vnode, Key, Msg) ->
   pipe:send(Pid, {send, Vnode, Key, Msg}).


%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle({coordinator, Pool}, Pipe, State) ->
   pipe:ack(Pipe,
      pq:lease(Pool, [{tenant, pipe:a(Pipe)}])
   ),
   {next_state, handle, State};

handle({i, {_, Addr, _, _}}, Pipe, State) ->
	pipe:ack(Pipe, pns:whereis(vnode, Addr)),
   {next_state, handle, State};

%%
%%
handle({cast, Vnode, Msg}, Pipe, #{node := Node}=State) ->
   case ensure(Node, Vnode) of
      {ok,    _} ->
         pipe:emit(Pipe, lookup(Vnode), Msg),
         {next_state, handle, State};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, State}
   end;

handle({cast, Vnode, Key, Msg}, Pipe, State) ->
   spawn(
      fun() ->
         case ambit:whereis(Vnode, Key) of
            undefined ->
               pipe:ack(Pipe, {error, noroute});
            Pid       ->
               pipe:ack(Pipe, pipe:call(Pid, Msg))
         end
      end
   ),
   {next_state, handle, State};

%%
%%
handle({send, Vnode, Msg}, Pipe, #{node := Node}=State) ->
   case ensure(Node, Vnode) of
      {ok,    _} ->
         pipe:emit(Pipe, lookup(Vnode), Msg),
         {next_state, handle, State};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, State}
   end;

handle({send, Vnode, Key, Msg}, _Pipe, State) ->
   spawn(
      fun() ->
         case ambit:whereis(Vnode, Key) of
            undefined ->
               ok;
            Pid       ->
               pipe:send(Pid, Msg)
         end
      end
   ),
   {next_state, handle, State};

%%
%%
handle({join, Peer, _Pid}, _Tx, State) ->
   %% new node joined cluster, all local v-nodes needs to be checked
   %% if relocation condition is met
   ?NOTICE("ambit [peer]: join ~p", [Peer]),
	pts:foreach(fun(Addr, Pid) -> dispatch(Addr, Pid, {join, Peer}) end, vnode),
   {next_state, handle, State};

handle({handoff, _Peer}, _Tx, State) ->
   %%  peer temporary down
   ?NOTICE("ambit [peer]: handoff ~p", [_Peer]),
   % pts:foreach(fun(Addr, Pid) -> dispatch(Addr, Pid, {handoff, Peer}) end, vnode),
   {next_state, handle, State};

handle({leave, _Peer}, _Tx, State) ->
   %%  peer permanently down
   ?NOTICE("ambit [peer]: leave ~p", [_Peer]),
   % pts:foreach(fun(Addr, Pid) -> dispatch(Addr, Pid, {leave, Peer}) end, vnode),
   {next_state, handle, State};

handle(_Msg, _Pipe, State) ->
   io:format("==> fu ~p~n", [_Msg]),
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% ensure vnode is running
ensure(_Node, {handoff, Addr, Peer, Pid}) ->
   case pns:whereis(vnode, Addr) of
      undefined ->
         pts:ensure(vnode, Addr, [{handoff, Addr, Peer, Pid}]);
      Vnode     ->
         {ok, Vnode}
   end;

ensure(_Node, {_, Addr, Peer, Pid}) ->
   case pns:whereis(vnode, Addr) of
      undefined ->
         pts:ensure(vnode, Addr, [{primary, Addr, Peer, Pid}]);
      Vnode     ->
         {ok, Vnode}
   end.

%%
%% lookup service hand of given Vnode
lookup({Hand,  Addr, _, _}) ->
   pns:whereis(vnode_sys, {Hand, Addr}).

%%
%% dispatch cluster event to destination vnode process 
dispatch({_, _}, _Pid, _Msg) ->
   ok;
dispatch(_, Pid, Msg) ->
   dispatch(pipe:ioctl(Pid, vnode), Msg).
   
dispatch({primary, Addr, Node, _}, {join, Peer}) ->
   List = ek:successors(ambit, Addr),
   case
      {lists:keyfind(Peer, 3, List), lists:keyfind(Node, 3, List)}
   of
      %% joined peer is not part of vnode successor list, skip to next one 
      {false,     _} ->
         ok;

      %% joined peer overtake local vnode, initiate handoff operation
      {Vnode, false} ->   
         pts:send(vnode, Addr, {handoff, Vnode});

      %% joined peer is sibling to local vnode, initiate node repair
      {Vnode, _} ->
         pts:send(vnode, Addr, {sync, Vnode})
   end;

dispatch({handoff, Addr, Node, _}, {join, Peer}) ->
   List = ek:successors(ambit, Addr),
   case
      {lists:keyfind(Peer, 3, List), Node =:= Peer}
   of
      %% joined peer is not part of vnode successor list, skip to next one 
      {false, _} ->
         ok;

      %% joined peer overtake local hints, initiate handoff operation
      {Vnode, true} ->   
         pts:send(vnode, Addr, {handoff, Vnode});
      
      _ ->
         ok
   end.


