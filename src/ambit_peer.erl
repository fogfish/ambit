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

  % ,send/2
  % ,cast/2
  % ,call/2
  % ,call/3
  % ,cast/2

  %  %% 
  % ,spawn/3
  % ,spawn_/3
  % ,close/2
  % ,whereis/2
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


% %%
% %% send async message to v-node
% send({_, _, _, Pid}=Vnode, Msg) ->
%    pipe:send(Pid, {send, Vnode, Msg});
% send(Pid, Msg) ->
%    pipe:send(Pid, Msg).

% %%
% %% cast async message to v-node (ensure v-node presence)
% cast({_, _, _, Pid}=Vnode, Msg) ->
%    pipe:send(Pid, {cast, Vnode, Msg}).


% %%
% %% 
% call(Vnode, Req) ->
%    call(Vnode, Req, 5000).

% call({_, _, _, Pid}=Vnode, Req, Timeout) ->
%    pipe:call(Pid, {req, Vnode, Req}, Timeout).

% %%
% %%
% cast({_, _, _, Pid}=Vnode, Req) ->
%    pipe:cast(Pid, {req, Vnode, Req}).


% %%
% %% spawn an actor
% spawn({_, _, _, Pid}=Vnode, Name, Service) ->
%    pipe:call(Pid, {spawn, Vnode, Name, Service}).

% spawn_({_, _, _, Pid}=Vnode, Name, Service) ->
%    pipe:cast(Pid, {spawn, Vnode, Name, Service}).

% %%
% %% close an actor
% close({_, _, _, Pid}=Vnode, Name) ->
%    pipe:cast(Pid, {close, Vnode, Name}).

% %%
% %% discover an actor
% whereis({_, _, _, Pid}=Vnode, Name) ->
%    pipe:cast(Pid, {whereis, Vnode, Name}).


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

% %%
% %%
% handle({send, Vnode, Msg}, Pipe, State) ->
%    case lookup(Vnode) of
%       %% no route to vnode
%       undefined ->
%          pipe:a(Pipe, unreachable),
%          {next_state, handle, State};
%       Pid       ->
%          pipe:send(Pid, Msg),
%          {next_state, handle, State}
%    end;

% handle({cast, {primary, Addr, _, _}=Vnode, {whereis, Pid, Name}}, _Pipe, State) ->
%    pipe:send(Pid, {Vnode, {ok, pns:whereis(ambit, {Addr, Name})}}),
%    {next_state, handle, State};

% handle({cast, {handoff, Addr, _, _}=Vnode, {whereis, Pid, Name}}, _Pipe, State) ->
%    pipe:send(Pid, {Vnode, {ok, undefined}}),
%    {next_state, handle, State};

% handle({cast, Vnode, Msg}, _Pipe, State) ->
%    case ensure(Vnode) of
%       {ok,    _} ->
%          pipe:send(lookup(Vnode), Msg),
%          {next_state, handle, State};
%       {error, _} ->
%          {next_state, handle, State}
%    end;

% %%
% %% handshake vnode service  
% handle({req, Vnode, Req}, Tx, State) ->
%    pipe:ack(Tx, vnode_request(Vnode, Req)),
%    {next_state, handle, State};

% %%
% %% spawn an actor
% handle({spawn, Vnode, Name, Service}, Tx, State) ->
%    pipe:ack(Tx, vnode_spawn(Vnode, Name, Service)),
%    {next_state, handle, State};

% %%
% %% close an actor
% handle({close, Vnode, Name}, Tx, State) ->
%    pipe:ack(Tx, vnode_close(Vnode, Name)),
%    {next_state, handle, State};

% %%
% %% discover an actor
% handle({whereis, Vnode, Name}, Tx, State) ->
%    pipe:ack(Tx, vnode_whereis(Vnode, Name)),
%    {next_state, handle, State};

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
                  lists:keyfind(Node, 3, ek:successors(ambit, Addr - 1))
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

% %%
% %% lookup v-node hand
% lookup({Hand, Addr, _, _}) ->
%    pns:whereis(vnode, {Hand, Addr}).

% %%
% %% ensure v-node
% ensure({_Hand, Addr, _, _}) ->
%    pts:ensure(vnode, Addr).



% %%
% %% request vnode services (using hand)
% vnode_request(Vnode, Req) ->
%    case lookup(Vnode) of
%       undefined ->
%          {error, unreachable};
%       Pid       ->
%          % @todo: any fucking message needs to be acknowledged by hand
%          %        design distributed pipe concept (mesh signaling + peer signaling)
%          io:format("==> peer ~p call ~p ~n", [Vnode, Req]),
%          pipe:call(Pid, Req, infinity)
%    end.

% %%
% %% request vnode to spawn an actor
% vnode_spawn({Hand, Addr, _, _}=Vnode, Name, Service) ->
%    case pts:ensure(vnode, Addr) of
%       {ok, _} ->
%          pipe:call(lookup(Vnode), {Hand, Name, Service});
%       Error   ->
%          Error
%    end.

% %%
% %% request vnode to close an actor
% vnode_close(Vnode, Name) ->
%    case lookup(Vnode) of
%       undefined ->
%          ok;
%       Pid       -> 
%          pipe:call(Pid, {free, Name})
%    end.

% %%
% %% request vnode to discover an actor
% vnode_whereis({primary, Addr, _, _},  Name) ->
%    {ok, pns:whereis(ambit, {Addr, Name})};
% vnode_whereis({handoff,_Addr, _, _}, _Name) ->
%    {ok, undefined}.
