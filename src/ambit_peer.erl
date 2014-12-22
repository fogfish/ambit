%% @description
%%   distributed actors
-module(ambit_peer).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
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
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle({{primary, Addr, _Peer, _Node}, {init, Name, Service}}, Tx, State) ->
   pipe:ack(Tx, 
      pts:call(vnode, Addr, {init, Name, Service})
   ),
   {next_state, handle, State};

% handle({spawn, {handoff, Addr, _Peer, _Node}, Name, Opts}, _, State) ->

%%
%%
handle({{primary, Addr, _Peer, _Node}, {free, Name}}, Tx, State) ->
   pipe:ack(Tx, 
      pts:call(vnode, Addr, {free, Name})
   ),
   {next_state, handle, State};

% handle({free,  {handoff, Addr, _Peer, _Node}, Name, Opts}, _, State) ->

%%
%%
handle({{primary,_Addr, _Peer, _Node}, {whereis, Name}}, Tx, State) ->
   pipe:ack(Tx, pns:whereis(ambit, Name)),
   {next_state, handle, State};

handle({{handoff,_Addr, _Peer, _Node}, {whereis,_Name}}, Tx, State) ->
   pipe:ack(Tx, undefined),
   {next_state, handle, State};

%%
%%
handle({join, Peer, Pid}, _Tx, State) ->
   %% new node joined cluster, relocate vnode
   lists:foreach(
      fun({VNode, _}) ->
         case pns:whereis(vnode, VNode) of
            undefined ->
               ok;
            X ->
               pipe:send(X, {transfer, Peer, Pid})
         end
      end,
      ek:ioctl(ambit, {lookup, Peer})
   ),
   {next_state, handle, State};

% handle({handoff, _Peer}) ->

% handle({leave, _Peer}) ->

handle(Msg, _, State) ->
   io:format("==> ~p~n", [Msg]),
   {next_state, handle, State}.


