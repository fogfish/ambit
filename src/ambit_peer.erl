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
  ,cast/2
  ,send/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
   {ok,    _} = ek:create(ambit, opts:val(ring, ?CONFIG_RING, ambit)),
   {ok, Pool} = pq:start_link([
      {type,     reusable}     
     ,{capacity, opts:val(pool, ?CONFIG_IO_POOL, thing)}    
     ,{worker,   ambit_coordinator}    
   ]),
   Node = scalar:s(erlang:node()),
   ok   = ek:join(ambit, Node, self()),
   {ok, handle, 
      #{
         node => Node,
         pool => Pool 
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
%% request coordinator 
-spec(coordinator/1 :: (pid()) -> any()).

coordinator(Peer) ->
   pipe:call(Peer, coordinator, infinity).

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
handle(coordinator, Pipe, #{pool := Pool} = State) ->
   pipe:ack(Pipe,
      pq:lease(Pool, [{tenant, pipe:a(Pipe)}])
   ),
   {next_state, handle, State};

handle({cast, {Hand, Addr, _, _}, Msg}, Pipe, State) ->
   case pts:ensure(vnode, Addr) of
      {ok,    _} ->
         pipe:emit(Pipe, pns:whereis(vnode, {Hand, Addr}), Msg),
         {next_state, handle, State};
      {error, _} ->
         pipe:a(Pipe, unreachable),
         {next_state, handle, State}
   end;

handle({send, {Hand, Addr, _, _}, Msg}, Pipe, State) ->
   case pns:whereis(vnode, {Hand, Addr}) of
      undefined ->
         {next_state, handle, State};
      Pid ->
         pipe:emit(Pipe, Pid, Msg),
         {next_state, handle, State}
   end;

%%
%%
handle({join, Peer, Pid}, _Tx, #{node := Node} = State) ->
   %% new node joined cluster, relocate vnode
   ?NOTICE("ambit [peer]: join ~s", [Peer]),
   lists:foreach(
      fun({Addr, _}) ->
         case pns:whereis(vnode, Addr) of
            %% vnode is not executed by local node, do nothing 
            undefined ->
               ok;

            %% vnode needs to be relocated if local node is not in candidate list   
            X ->
               case 
                  lists:keyfind(Node, 3, ek:successors(ambit, Addr))
               of
                  false ->
                     pipe:send(X, {handoff, {primary, Addr, Peer, Pid}});
                  _     ->
                     ok
               end
         end
      end,
      ek:whois(ambit, Peer)
   ),
   {next_state, handle, State};

% handle({handoff, _Peer}) ->

% handle({leave, _Peer}) ->

handle(Msg, Pipe, State) ->
   io:format("==> ~p ~p~n", [Msg, Pipe]),
   pipe:a(Pipe, Msg),
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

