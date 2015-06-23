%% @description
%%   virtual node coordinator process
-module(ambit_vnode).
-behaviour(pipe).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

-export([
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,primary/3
  ,handoff/3
  ,suspend/3   %% -> offline
  ,transfer/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link(Sup, Vnode) ->
   pipe:start_link(?MODULE, [Sup, ek:vnode(type, Vnode), Vnode], []).

init([Sup, primary, Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, ek:vnode(addr, Vnode), self()),
   {ok, primary, 
      #{
         vnode   => Vnode,
         sup     => Sup   
      }
   };

init([Sup, handoff, Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, ek:vnode(addr, Vnode), self()),
   {ok, handoff, 
      #{
         vnode   => Vnode,
         sup     => Sup   
      }
   }.

%%
%%
free(_Reason, #{sup := Sup, vnode := Vnode}) ->
   ?DEBUG("ambit [vnode]: free ~p ~p", [Vnode, _Reason]),
	ok = pns:unregister(vnode, ek:vnode(addr, Vnode)),
   supervisor:terminate_child(pts:i(factory, vnode), Sup),
   ok.

ioctl(vnode, #{vnode := Vnode}) ->
   Vnode;
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
primary({handoff, Peer}, _,  #{vnode := Vnode}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Vnode, Peer]),
   erlang:send(self(), transfer),
   {next_state, suspend, 
      State#{
         handoff => Peer,
         stream  => stream:build(pns:lookup(ek:vnode(addr, Vnode), '_'))
      }
   };

primary({sync, Peer}, _, #{vnode := Vnode}=State) ->
   ?NOTICE("ambit [vnode]: sync ~p with ~p", [Vnode, Peer]),
   %% @todo: sync requires aae + async sync (vs explicit recovery)
   %% @todo: make asynchronous handoff with long-term expectation of data transfer
   %%        handoff is only "create feature"
   Addr = ek:vnode(addr, Vnode),
   lists:foreach(
      fun({Name, _Pid}) ->
         case ambit_actor:service(Addr, Name) of
            %% service died during transfer i/o
            undefined ->
               ok;

            %% service is removed skip sync
            #entity{val = undefined} ->
               ok;

            %% sync existed service
            Entity ->
               case lists:member(Peer, ambitz:entity(vnode, Entity)) of
                  false ->
                     ?NOTICE("ambit [vnode]: sync recovery ~p", [Peer]),
                     Tx = ambit_peer:cast(Peer, {create, Entity}),
                     receive
                        {Tx, _} ->
                           ok
                     after ?CONFIG_TIMEOUT_REQ ->
                        ?ERROR("ambit [vnode]: sync recovery timeout ~p", [Peer])
                     end;
                  true  ->
                     ok
               end,
               ambit_actor:sync(Addr, Name, Peer)
         end
      end,
      pns:lookup(Addr, '_')
   ),
   {next_state, primary, State}.


%%
%%
handoff({handoff, Peer}, _,  #{vnode := Vnode}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p to ~p", [Vnode, Peer]),
   erlang:send(self(), transfer),
   {next_state, suspend, 
      State#{
         handoff => Peer,
         stream  => stream:build(pns:lookup(ek:vnode(addr, Vnode), '_'))
      }
   };

handoff({sync, _Peer}, _, State) ->
   %% do nothing, hint db is not synchronized
   {next_state, handoff, State}.


%%
%%
suspend(transfer, _, #{vnode := _Vnode, stream := {}}=State) ->
   ?NOTICE("ambit [vnode]: handoff ~p completed", [_Vnode]),
   {stop, normal, State};

suspend(transfer, _, #{vnode := Vnode, handoff := Handoff, stream := Stream}=State) ->
   {Name, _Pid} = stream:head(Stream),
   case ambit_actor:service(ek:vnode(addr, Vnode), Name) of
      %% service died during transfer i/o
      undefined ->
         erlang:send(self(), transfer),
         {next_state, suspend, State#{stream => stream:tail(Stream)}};      

      %% sync removed service
      #entity{val = undefined} = Entity ->
         ?DEBUG("ambit [vnode]: transfer (-) ~p", [Name]),
         Tx = ambit_peer:cast(Handoff, {remove, Entity}),
         {next_state, transfer, State#{tx => Tx}, 10000}; %% @todo: config

      %% sync existed service
      Entity ->
         ?DEBUG("ambit [vnode]: transfer (+) ~p", [Name]),
         Tx = ambit_peer:cast(Handoff, {create, Entity}),
         {next_state, transfer, State#{tx => Tx}, 10000}  %% @todo: config
   end;

suspend(_, _, State) ->
   {next_state, suspend, State}.

%%
%%
transfer({Tx, _Result}, _, #{tx := Tx, vnode := Vnode, handoff := Handoff, stream := Stream}=State) ->
   ?DEBUG("ambit [vnode]: transfer ~p", [_Result]),
   {Name, _Pid} = stream:head(Stream),
   %% @todo: make asynchronous handoff with long-term expectation of data transfer
   %%        handoff is only "create feature"
   ambit_actor:handoff(ek:vnode(addr, Vnode), Name, Handoff),
   erlang:send(self(), transfer),
   {next_state, suspend, State#{stream => stream:tail(Stream)}};
   
transfer(timeout, _, State) ->
   erlang:send_after(1000, self(), transfer),
   {next_state, suspend,  State};

transfer(_, _, State) ->
   {next_state, transfer, State}.

   
%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   







