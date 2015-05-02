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
  ,coordinator/1
  ,i/1
  ,cast/2
  ,send/2
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
   %% @todo: move under root sup
   {ok,    _} = ek:create(ambit, opts:val(ring, ?CONFIG_RING, ambit)),
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
-spec(coordinator/1 :: (ek:vnode()) -> any()).

coordinator({_, _, _, Pid}) ->
   pipe:call(Pid, coordinator, infinity).

%%
%% get vnode status 
-spec(i/1 :: (ek:vnode()) -> any()).

i({_, _, _, Peer} = Vnode) ->
	pipe:call(Peer, {i, Vnode}).

%%
%% cast message to vnode
-spec(cast/2 :: (ek:vnode(), any()) -> reference()).

cast({_, _, _, Pid} = Vnode, Msg) ->
   pipe:cast(Pid, {cast, Vnode, Msg}).

%%
%% send message to vnode
-spec(send/2 :: (ek:vnode(), any()) -> ok).

send({_, _, _, Pid} = Vnode, Msg) ->
   pipe:send(Pid, {send, Vnode, Msg}).



%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle(coordinator, Pipe, State) ->
   pipe:ack(Pipe,
      pq:lease(ambit_coordinator, [{tenant, pipe:a(Pipe)}])
   ),
   {next_state, handle, State};

handle({i, {_, Addr, _, _}}, Pipe, State) ->
	pipe:ack(Pipe, pns:whereis(vnode, Addr)),
   {next_state, handle, State};

handle({cast, Vnode, Msg}, Pipe, #{node := Node}=State) ->
   case ensure(Node, Vnode) of
      {ok,    _} ->
         pipe:emit(Pipe, lookup(Vnode), Msg),
         {next_state, handle, State};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, State}
   end;

handle({send, Vnode, Msg}, Pipe, #{node := Node}=State) ->
   case ensure(Node, Vnode) of
      {ok,    _} ->
         pipe:emit(Pipe, lookup(Vnode), Msg),
         {next_state, handle, State};
      {error, _} = Error ->
         pipe:a(Pipe, Error),
         {next_state, handle, State}
   end;

%%
%%
handle({join, Peer, _Pid}, _Tx, State) ->
   %% new node joined cluster, all local v-nodes needs to be checked
   %% if relocation condition is met
   ?NOTICE("ambit [peer]: join ~p", [Peer]),
	pts:foreach(fun(Addr, _) -> handoff(Addr, Peer) end, vnode),
   {next_state, handle, State};

handle({handoff, _Peer}, _Tx, State) ->
   %%  peer temporary down
   ?NOTICE("ambit [peer]: handoff ~p", [_Peer]),
   {next_state, handle, State};

handle({leave, _Peer}, _Tx, State) ->
   %%  peer permanently down
   ?NOTICE("ambit [peer]: leave ~p", [_Peer]),
   {next_state, handle, State};

handle(_Msg, _Pipe, State) ->
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
   pns:whereis(vnode, {Hand, Addr}).

%%
%%
handoff({_, _},  _) ->
   ok;
handoff(Addr, Peer) ->
   pts:send(vnode, Addr, {handoff, Peer}).
