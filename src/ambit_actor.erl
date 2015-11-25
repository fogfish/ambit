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
%%   actor management process
%%
%% @todo
%%   * actor signaling
%%   * actor tth -> hibernate for memory management
%%   * actor auto conflict resolution
-module(ambit_actor).
-behaviour(pipe).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

-export([
   start_link/4
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
   %% api
  ,service/2
  ,handoff/3
  ,sync/3
]).


%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Sup, Addr, Key, Vnode) ->
   pipe:start_link(?MODULE, [Sup, Addr, Key, Vnode], []).

init([Sup, Addr, Key, Vnode]) ->
   ?DEBUG("ambit [actor]: ~p init ~p", [Vnode, Key]),
   %% register actor management process to the pool
   pns:register(Addr, Key, self()),
   erlang:process_flag(trap_exit, true),
   {ok, handle, 
      #{
         sup     => Sup, 
         vnode   => Vnode,
         entity  => #entity{key = Key},
         actor   => undefined,
         process => undefined
      }
   }.

free(_, #{vnode := _Vnode, entity := #entity{key = _Key}}) ->
   ?DEBUG("ambit [actor]: free ~p at ~p", [_Key, _Vnode]),
   ok.

%%
%%
ioctl(process, #{process := Process}) ->
   % return actor instance process 
   Process;
ioctl(service, #{entity  := Service}) ->
   % return entity definition
   Service;
ioctl(_, _) ->
   throw(not_implemented).

%%%----------------------------------------------------------------------------   
%%%
%%% api
%%%
%%%----------------------------------------------------------------------------   

%%
%% return service / entity specification
service(Addr, Key) ->
   case pns:whereis(Addr, Key) of
      undefined ->
         undefined;
      Pid       ->
         pipe:ioctl(Pid, service)
   end.

%% handoff actor 
handoff(Addr, Name, Vnode) ->
   pts:call(Addr, Name, {handoff, Vnode}, infinity).

%% sync actor
sync(Addr, Name, Peer) ->
   pts:call(Addr, Name, {sync, Peer}, infinity).


%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle({create, #entity{key = _Key, vsn = VsnA}=Entity}, Pipe, #{entity := #entity{vsn = VsnB}}=State0) ->
   case {uid:descend(VsnA, VsnB), uid:descend(VsnB, VsnA)} of
      %% request is descend of actor -> create actor if not exists
      {true,  _} ->
         {Result, State1} = create(Entity, State0),
         pipe:ack(Pipe, Result),
         tempus:timer(opts:val(ttl, undefined, ambit), ttl),
         {next_state, handle, State1};

      %% actor is descend of request -> skip create request
      {_,  true} ->
         pipe:ack(Pipe, {ok, Entity}),
         {next_state, handle, State0};

      %% conflict -> @todo: automatic conflict resolution 
      _ ->
         ?DEBUG("ambit [actor]: ~p create conflict with ~p", [_Key, uid:diff(VsnA, VsnB)]),
         pipe:ack(Pipe, {error, conflict}),
         {next_state, handle, State0}
   end;

handle({remove, #entity{key = _Key, vsn = VsnA}=Entity}, Pipe, #{entity := #entity{vsn = VsnB}}=State0) ->
   case {uid:descend(VsnA, VsnB), uid:descend(VsnB, VsnA)} of
      %% request is descend of actor -> remove actor if exists
      {true,  _} ->
         {Result, State1} = remove(Entity, State0),
         pipe:ack(Pipe, Result),
         case tempus:timer(opts:val(ttd, undefined, ambit), ttd) of
            undefined ->
               {stop, normal, State1};
            _         ->
               {next_state, handle, State1}
         end;

      %% actor is descend of request -> ignore request
      {_,  true} ->
         pipe:ack(Pipe, {ok, Entity}),
         {next_state, handle, State0};

      %% conflict -> @todo: automatic conflict resolution 
      _ ->
         ?DEBUG("ambit [actor]: ~p remove conflict with ~p", [_Key, uid:diff(VsnA, VsnB)]),
         pipe:ack(Pipe, {error, conflict}),
         {next_state, handle, State0}
   end;


handle({lookup, _Entity}, Pipe, #{entity := Entity}=State) ->
   pipe:ack(Pipe, {ok, Entity}),
   {next_state, handle, State};

handle({process, _Entity}, Pipe, #{entity := Entity, process := Pid}=State) ->
   %% where is actor (pid discovery)
   pipe:ack(Pipe, {ok, Entity#entity{val = [Pid]}}),
   {next_state, handle, State};


%%
%% actor optional signaling
handle({handoff, Vnode}, Tx, #{actor := Root, entity := #entity{val ={Mod, _, _}}}=State) ->
   case erlang:function_exported(Mod, handoff, 2) of
      true  ->
         pipe:ack(Tx, 
            Mod:handoff(Root, Vnode)
         ),
         {next_state, handle, State};
      false ->
         pipe:ack(Tx, ok),
         {next_state, handle, State}
   end;

handle({sync, Peer}, Tx, #{actor := Root, entity := #entity{val ={Mod, _, _}}}=State) ->
   case erlang:function_exported(Mod, sync, 2) of
      true  ->
         pipe:ack(Tx, 
            Mod:sync(Root, Peer)
         ),
         {next_state, handle, State};
      false ->
         pipe:ack(Tx, ok),
         {next_state, handle, State}
   end;


handle(ttl, Tx, #{entity := #entity{vsn = Vsn}=Entity}=State) ->
   handle({remove, Entity#entity{vsn = uid:vclock(Vsn)}}, Tx, State);

handle(ttd,  _, #{entity := #entity{val = undefined}}=State) ->
   {stop, normal, State};   

handle(_, _, State) ->
   {next_state, handle, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
create(#entity{key = Key, val = {Mod, Fun, Arg}}=Entity0, #{sup := Sup, vnode := Vnode} = State) ->
   Addr = ek:vnode(addr, Vnode),
   case ambit_actor_sup:init_service(Sup, {Mod, Fun, [Vnode | Arg]}) of
      {ok, Root} ->
         case erlang:function_exported(Mod, process, 1) of
            true  ->
               {ok, Pid} = Mod:process(Root),
               %% register actor process to the pool
               _ = pns:register(ambit, {Addr, Key}, Pid),
               Entity1 = entity(Entity0, State),
               {{ok, Entity1}, State#{actor => Root, process => Pid,  entity => Entity1}};

            false ->
               %% register actor process to the pool
               _ = pns:register(ambit, {Addr, Key}, Root),
               Entity1 = entity(Entity0, State),
               {{ok, Entity1}, State#{actor => Root, process => Root, entity => Entity1}}
         end;
      {error, {already_started, _}} ->
         Entity1 = entity(Entity0, State),
         {{ok, Entity1}, State#{entity => Entity1}};
      {error, _} = Error ->
         {Error, State}
   end.

%%
%%
remove(#entity{key = Key} = Entity0, #{sup := Sup, vnode := Vnode} = State) ->
   Addr = ek:vnode(addr, Vnode),
   _ = ambit_actor_sup:free_service(Sup),
   _ = pns:unregister(ambit, {Addr, Key}),
   % _ = pns:unregister(Addr, Key),
   Entity1 = entity(Entity0#entity{val = undefined}, State),
   % @todo: ttl timer 
   {{ok, Entity1}, State#{entity => Entity1}}.

%%
%%
entity(#entity{vsn = VsnA} = Entity0, #{entity := #entity{vsn = VsnB}, vnode := Vnode}) ->
   Entity1 = Entity0#entity{vsn = uid:join(VsnA, VsnB), vnode = [Vnode]},
   ?DEBUG("ambit [actor]: set entity ~p", [Entity1]),
   Entity1.




