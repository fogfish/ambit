%% @description
%%   virtual node coordinator process
-module(ambit_vnode).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,primary/3
  ,handoff/3
  ,suspend/3
  ,transfer/3

  % ,active/3   % -> primary/3
  % ,transfer/3 % -> handoff/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link(Sup, Vnode) ->
   pipe:start_link(?MODULE, [Sup, Vnode], []).

init([Sup, {primary, Addr, _, _}=Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, Addr, self()),
   {ok, primary, 
      #{
         vnode   => Vnode,
         sup     => Sup   
      }
   };

init([Sup, {handoff, Addr, _, _}=Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, Addr, self()),
   {ok, handoff, 
      #{
         vnode   => Vnode,
         sup     => Sup   
      }
   }.

%%
%%
free(_Reason, #{sup := Sup, vnode := {_, Addr, _, _}}) ->
   ?DEBUG("ambit [vnode]: free ~b ~p", [Addr, _Reason]),
	ok = pns:unregister(vnode, Addr),
   supervisor:terminate_child(pts:i(factory, vnode), Sup),
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
primary({handoff, Peer}, _,  #{vnode := {_, Addr, _, _} = Vnode}=State) ->
   case handoff_peer(Peer, Vnode) of
      false   ->
         {next_state, primary, State};
      Handoff ->
         ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Vnode, Handoff]),
         erlang:send(self(), transfer),
         {next_state, suspend, 
            State#{
               handoff => Handoff,
               stream  => stream:build(pns:lookup(Addr, '_'))
            }
         }
   end.


%%
%%
handoff({handoff, Peer}, _,  #{vnode := {_, Addr, _, _} = Vnode}=State) ->
   case handoff_peer(Peer, Vnode) of
      false   ->
         {next_state, handoff, State};
      Handoff ->
         ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Vnode, Handoff]),
         erlang:send(self(), transfer),
         {next_state, suspend, 
            State#{
               handoff => Handoff,
               stream  => stream:build(pns:lookup(Addr, '_'))
            }
         }
   end.

%%
%%
suspend(transfer, _, #{vnode := _Vnode, stream := {}}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p completed", [_Vnode]),
   {stop, normal, State};

suspend(transfer, _, #{vnode := {_, Addr, _, _}, handoff := Handoff, stream := Stream}=State) ->
   {Name, _Pid} = stream:head(Stream),
   Service      = ambit_actor:service(Addr, Name),
   Tx = ambit_peer:cast(Handoff, {spawn, Name, Service}),
   ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
   {next_state, transfer, State#{tx => Tx}, 10000}; %% @todo: config

suspend(_, _, State) ->
   {next_state, suspend, State}.

%%
%%
transfer({Tx, _Result}, _, #{tx := Tx, vnode := {_, Addr, _, _}, handoff := Handoff, stream := Stream}=State) ->
   ?DEBUG("ambit [vnode]: transfer ~p", [_Result]),
   {Name, _Pid} = stream:head(Stream),
   ambit_actor:handoff(Addr, Name, Handoff),
   erlang:send(self(), transfer),
   {next_state, suspend, State#{stream => stream:tail(Stream)}};
   
transfer(timeout, _, State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, suspend, State}.
   



% active({handoff, Vnode}, _, #{vnode := {primary, Addr, _, _} = Self}=State) ->
%    %% initial child transfer procedure
%    ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Self, Vnode]),
%    erlang:send(self(), transfer),
%    {next_state, transfer, 
%       State#{
%          target => Vnode,
%          stream => stream:build(pns:lookup(Addr, '_'))
%       }
%    };

% active({handoff, Vnode}, _, #{vnode := {handoff, Addr, _, _} = Self}=State) ->
%    %% initial child transfer procedure
%    %% @todo: actor state management
%    ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Self, Vnode]),
%    {stop, normal, State};

% active(_Msg, _, State) ->
%    ?WARNING("ambit [vnode]: unexpected message ~p", [_Msg]),
%    {next_state, active, State}.


% transfer(transfer, _, #{vnode := {_, Addr, _, _}, target := Vnode, stream := Stream}=State) ->
%    {Name, _Pid} = stream:head(Stream),
%    Service      = ambit_actor:service(Addr, Name),
%    Tx = ambit_peer:cast(Vnode, {spawn, Name, Service}),
%    ?DEBUG("ambit [vnode]: transfer ~p", [Name]),
%    {next_state, transfer, State#{tx => Tx}, 5000}; %% @todo: config

% transfer({Tx, _Result}, _, #{tx := Tx, stream := Stream}=State) ->
%    ?DEBUG("ambit [vnode]: transfer ~p", [_Result]),
%    erlang:send(self(), transfer),
%    {next_state, transfer, State#{stream => stream:tail(Stream)}};

% transfer(timeout, _, State) ->
%    erlang:send_after(1000, self(), transfer),
%    {next_state, transfer, State};

% transfer(Msg, _, State) ->
%    ?WARNING("ambit [vnode]: unexpected message ~p", [Msg]),
%    {next_state, transfer, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% 
handoff_peer(Peer, {primary, Addr, Node, _}) ->
   List = ek:successors(ambit, Addr),
   case
      {lists:keyfind(Peer, 3, List), lists:keyfind(Node, 3, List)}
   of
      {false,     _} ->
         false;
      {Vnode, false} ->   
         Vnode;
      {_, _} ->
         false
   end;

handoff_peer(Peer, {handoff, Addr, Node, _}) ->
   List = ek:successors(ambit, Addr),
   case
      {lists:keyfind(Peer, 3, List), lists:keyfind(Node, 3, List)}
   of
      {false,     _} ->
         false;
      {Vnode, Vnode} ->   
         Vnode;
      {_, _} ->
         false
   end.


