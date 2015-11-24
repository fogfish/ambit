%%
%%   Copyright 2014 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
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
   pipe:start_link(?MODULE, [Sup, Vnode], []).

init([Sup, Vnode]) ->
   ?DEBUG("ambit [vnode]: init ~p", [Vnode]),
   ok = pns:register(vnode, ek:vnode(addr, Vnode), self()),
   {ok, ek:vnode(type, Vnode), 
      #{
         vnode => Vnode,
         sup   => Sup   
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
ioctl({spawn, Pid}, State) ->
   State#{spawn => Pid};
ioctl({aae, Pid}, State) ->
   State#{aae => Pid};
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
primary({Req, _} = Request, Pipe, #{spawn := Pid}=State)
 when ?is_spawn(Req) ->
   pipe:emit(Pipe, Pid, Request),
   {next_state, primary, State};

primary({aae, Req}, Pipe, #{aae := Pid}=State) ->
   pipe:emit(Pipe, Pid, Req),
   {next_state, primary, State};

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
handoff({Req, _} = Request, Pipe, #{spawn := Pid}=State)
 when ?is_spawn(Req) ->
   pipe:emit(Pipe, Pid, Request),
   {next_state, handoff, State};

handoff({aae, Req}, Pipe, #{aae := Pid}=State) ->
   pipe:emit(Pipe, Pid, Req),
   {next_state, handoff, State};


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







