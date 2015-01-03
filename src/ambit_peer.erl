%% @description
%%   distributed actor peer - interface for vnode communication
-module(ambit_peer).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
   %% api
  ,call/2
  ,call/3
  ,cast/2
  ,send/2

   %% 
  ,spawn/3
  ,spawn_/3
  ,close/2
  ,whereis/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
   {ok, _} = ek:create(ambit, opts:val(ring, ?CONFIG_RING, ambit)),
   ok      = ek:join(ambit, scalar:s(erlang:node()), self()),   
   {ok, handle, #{}}.

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
%% 
call(Vnode, Req) ->
   call(Vnode, Req, 5000).

call({_, _, _, Pid}=Vnode, Req, Timeout) ->
   pipe:call(Pid, {req, Vnode, Req}, Timeout).

%%
%%
cast({_, _, _, Pid}=Vnode, Req) ->
   pipe:cast(Pid, {req, Vnode, Req}).

%%
%%
send({_, _, _, Pid}=Vnode, Req) ->
   pipe:send(Pid, {req, Vnode, Req}).

%%
%% spawn an actor
spawn({_, _, _, Pid}=Vnode, Name, Service) ->
   pipe:call(Pid, {spawn, Vnode, Name, Service}).

spawn_({_, _, _, Pid}=Vnode, Name, Service) ->
   pipe:cast(Pid, {spawn, Vnode, Name, Service}).

%%
%% close an actor
close({_, _, _, Pid}=Vnode, Name) ->
   pipe:cast(Pid, {close, Vnode, Name}).

%%
%% discover an actor
whereis({_, _, _, Pid}=Vnode, Name) ->
   pipe:cast(Pid, {whereis, Vnode, Name}).


%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%% handshake vnode service  
handle({req, Vnode, Req}, Tx, State) ->
   pipe:ack(Tx, vnode_request(Vnode, Req)),
   {next_state, handle, State};

%%
%% spawn an actor
handle({spawn, Vnode, Name, Service}, Tx, State) ->
   pipe:ack(Tx, vnode_spawn(Vnode, Name, Service)),
   {next_state, handle, State};

%%
%% close an actor
handle({close, Vnode, Name}, Tx, State) ->
   pipe:ack(Tx, vnode_close(Vnode, Name)),
   {next_state, handle, State};

%%
%% discover an actor
handle({whereis, Vnode, Name}, Tx, State) ->
   pipe:ack(Tx, vnode_whereis(Vnode, Name)),
   {next_state, handle, State};

%%
%%
% handle({primary, Addr, _Peer, _Node, {reconcile, Peer}}, Tx, State) ->
%    %% @todo - discover vnode ae part or proxy through vnode

%%
%%
handle({join, Peer, Pid}, _Tx, State) ->
   %% new node joined cluster, relocate vnode
   ?NOTICE("ambit [peer]: join ~s", [Peer]),
   lists:foreach(
      fun({Addr, _}) ->
         case pns:whereis(vnode, Addr) of
            undefined ->
               ok;
            X ->
               pipe:send(X, {handoff, {primary, Addr, Peer, Pid}})
         end
      end,
      ek:whois(ambit, Peer)
   ),
   {next_state, handle, State};

% handle({handoff, _Peer}) ->

% handle({leave, _Peer}) ->

handle(Msg, _, State) ->
   io:format("==> ~p~n", [Msg]),
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% where is vnode
lookup({Hand, Addr, _, _}) ->
   pns:whereis(vnode, {Hand, Addr}).

%%
%% request vnode services (using hand)
vnode_request(Vnode, Req) ->
   case lookup(Vnode) of
      undefined ->
         {error, unreachable};
      Pid       ->
         pipe:call(Pid, Req)
   end.

%%
%% request vnode to spawn an actor
vnode_spawn({Hand, Addr, _, _}=Vnode, Name, Service) ->
   case pts:ensure(vnode, Addr) of
      {ok, _} ->
         pipe:call(lookup(Vnode), {Hand, Name, Service});
      Error   ->
         Error
   end.

%%
%% request vnode to close an actor
vnode_close(Vnode, Name) ->
   case lookup(Vnode) of
      undefined ->
         ok;
      Pid       -> 
         pipe:call(Pid, {free, Name})
   end.

%%
%% request vnode to discover an actor
vnode_whereis({primary, Addr, _, _},  Name) ->
   {ok, pns:whereis(ambit, {Addr, Name})};
vnode_whereis({handoff,_Addr, _, _}, _Name) ->
   {ok, undefined}.
