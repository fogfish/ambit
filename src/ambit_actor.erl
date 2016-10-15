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
   start_link/3
  ,init/1
  ,free/2
  ,ioctl/2

  ,handle/3

   %% api
  ,service/2
  ,handoff/3
  ,sync/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Addr, Key, Vnode) ->
   pipe:start_link(?MODULE, [Addr, Key, Vnode], []).

init([Addr, Key, Vnode]) ->
   ?DEBUG("ambit [actor]: ~p init ~p", [Vnode, Key]),
   pns:register(Addr, Key, self()),
   erlang:process_flag(trap_exit, true),
   {ok, handle, 
      #{
         entity  => #entity{key = Key, vnode = [Vnode]}
      }
   }.

free(_Reason, #{entity := #entity{key = _Key, vnode = [_Vnode]}}) ->
   ?DEBUG("ambit [actor]: free ~p at ~p with ~p", [_Key, _Vnode, _Reason]),
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
sync(Pid, Peer) ->
   pipe:call(Pid, {sync, Peer}, infinity).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   



%%
%%
handle({'$ambitz', Msg, Entity}, Pipe, State0) ->
   {Result, State1} = accept(Msg, Entity, State0),
   pipe:a(Pipe, Result),
   {next_state, handle, State1};

handle({'EXIT', _, normal}, _Pipe, State) ->
   {next_state, handle, maps:remove(actor, State)};

handle({'EXIT', _, Reason}, _Pipe, State) ->
   {stop, Reason, State};

% %%
% %% actor optional signaling
% handle({handoff, Vnode}, Tx, #{actor := Root, entity := #entity{val ={Mod, _, _}}}=State) ->
%    case erlang:function_exported(Mod, handoff, 2) of
%       true  ->
%          pipe:ack(Tx, 
%             Mod:handoff(Root, Vnode)
%          ),
%          {next_state, handle, State};
%       false ->
%          pipe:ack(Tx, ok),
%          {next_state, handle, State}
%    end;
handle({handoff, Peer}, Pipe, State) ->
   % pipe:ack(Pipe, ok),
   pipe:ack(Pipe, syncwith(Peer, State)),
   {next_state, handle, State};

% handle({sync, Peer}, Tx, #{actor := Root, entity := #entity{val ={Mod, _, _}}}=State) ->
%    case erlang:function_exported(Mod, sync, 2) of
%       true  ->
%          pipe:ack(Tx, 
%             Mod:sync(Root, Peer)
%          ),
%          {next_state, handle, State};
%       false ->
%          pipe:ack(Tx, ok),
%          {next_state, handle, State}
%    end;
handle({sync, Peer}, Pipe, State) ->
   pipe:ack(Pipe, syncwith(Peer, State)),
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
accept(Msg, EntityB, #{entity := EntityA} = State) ->
   accept(descend(EntityA, EntityB), Msg, EntityB, State).

accept(_, lookup, Entity, State) ->
   {lookup(Entity, State), State};

accept(_, whereis, Entity, State) ->
   {discover(Entity, State), State};

accept(accept, spawn, Entity0, State0) ->
   case create(Entity0, State0) of
      {ok, State1} ->
         Entity1 = entity(Entity0, State1),
         {{ok, Entity1}, State1#{entity => Entity1}};
      {error,   _} = Error ->
         {Error, State0}
   end;

accept(accept, free, Entity0, State0) ->
   case remove(Entity0, State0) of
      {ok, State1} ->
         Entity1 = entity(Entity0, State1),
         {{ok, Entity1}, State1#{entity => Entity1}};
      {error,   _} = Error ->
         {Error, State0}
   end;

accept(accept, call, Entity0, State0) ->
   case call(Entity0, State0) of
      {error,   _} = Error ->
         {Error, State0};
      {Value, State1} ->
         Entity1 = entity(Entity0, State1),
         {{ok, Entity1#entity{val = Value}}, State1#{entity => Entity1}}
   end;

accept(accept, snapshot, #entity{val = Snap}=Entity0, #{actor := Pid} = State0) ->
   case pipe:ioctl(Pid, {snapshot, Snap}) of
      {error,   _} = Error ->
         {Error, State0};
      ok ->
         Entity1 = entity(Entity0, State0),
         {{ok, Entity1}, State0#{entity => Entity1}}
   end;
   
accept(skip, _Msg, _EntityB, #{entity := #entity{key =_Key} = EntityA} = State) ->
   ?DEBUG("ambit [actor]: ~p skips ~p", [_Key, _Msg]),
   {{ok, EntityA}, State};

accept(conflict, _Msg, #entity{key = _Key, vsn = _VsnB}, #{entity := #entity{vsn = _VsnA}} = State) ->
   % @todo: conflict resolution
   ?DEBUG("ambit [actor]: ~p ~p conflict with ~p", [_Key, _Msg, uid:diff(_VsnA, _VsnB)]),
   {{error, conflict}, State}.

%%
%%
create(_Entity, #{actor := _} = State) ->
   {ok, State};
create(#entity{val = {M, F, A}}, #{entity := #entity{vnode =[Vnode]}} = State) ->
   case erlang:apply(M, F, [Vnode|A]) of
      {ok, Pid} ->
         {ok, State#{actor => Pid}};
      {error, _} = Error ->
         {Error, State}
   end.

%%
%%
remove(_Entity, #{actor := Pid} = State) ->
   erlang:exit(Pid, normal),
   {ok, maps:remove(actor, State#entity{val = undefined})};
remove(_Entity, State) ->
   {ok, State}.

%%
%%
call(#entity{val = Msg}, #{actor := Pid} = State) ->
   %% @todo: handle timeout
   {pipe:call(Pid, Msg, 60000), State};

call(_, State) ->
   {{error, no_actor}, State}.


%%
%%
lookup(_, #{entity := Entity}) ->
   {ok, Entity}.

%%
%%
discover(_, #{entity := Entity, actor := Pid}) ->
   {ok, Entity#entity{val = [Pid]}};

discover(_, #{entity := Entity}) ->
   {ok, Entity#entity{val = []}}.



% %%
% %%
% create(#entity{vsn = VsnB} = Entity, #{entity := #entity{vsn = VsnA}} = State) ->
%    create(descend(VsnA, VsnB), Entity, State).

% create({true, _}, _, #{entity := Entity} = State) ->
%    % VsnB -> VsnA : skip request
%    {{ok, Entity}, State};

% create({_, true}, Entity0, #{vnode := Vnode} = State) ->
%    % VsnA -> VsnB : create actor
%    case actor_init(Vnode, Entity0, State) of
%       {ok, Pid} ->
%          Entity1 = entity(Entity0, State),
%          {{ok, Entity1}, State#{entity => Entity1, actor => Pid}};
%       {error,_} = Error ->
%          {Error, State}
%    end;

% create({_, _}, #entity{key = _Key, vsn = _VsnB}, #{entity := #entity{vsn = _VsnA}} = State) ->
%    % VsnA || VsnB : conflict
%    % @todo: conflict resolution
%    ?DEBUG("ambit [actor]: ~p create conflict with ~p", [_Key, uid:diff(_VsnA, _VsnB)]),
%    {{error, conflict}, State}.


% %%
% %%
% remove(#entity{vsn = VsnB} = Entity, #{entity := #entity{vsn = VsnA}} = State) ->
%    remove(descend(VsnA, VsnB), Entity, State).

% remove({true, _}, _, #{entity := Entity} = State) ->
%    % VsnB -> VsnA : skip request
%    {{ok, Entity}, State};

% remove({_, true}, Entity0, #{vnode := Vnode} = State) ->
%    % VsnA -> VsnB : create actor
%    case actor_free(Vnode, Entity0, State) of
%       ok ->
%          Entity1 = entity(Entity0, State),
%          {{ok, Entity1}, maps:remove(actor, State#{entity => Entity1})};
%       {error,_} = Error ->
%          {Error, State}
%    end;

% remove({_, _}, #entity{key = _Key, vsn = _VsnB}, #{entity := #entity{vsn = _VsnA}} = State) ->
%    % VsnA || VsnB : conflict
%    % @todo: conflict resolution
%    ?DEBUG("ambit [actor]: ~p remove conflict with ~p", [_Key, uid:diff(_VsnA, _VsnB)]),
%    {{error, conflict}, State}.

% %%
% %%
% actor_init(_, _, #{actor := Pid}) ->
%    {ok, Pid};
% actor_init(Vnode, #entity{val = {M, F, A}}, State) ->
%    erlang:apply(M, F, [Vnode|A]).

% %%
% %%
% actor_free(_, _, #{actor := Pid}) ->
%    erlang:exit(Pid, normal),
%    ok;
% actor_free(_, _, _) ->
%    ok.

%%
%%
descend(#entity{vsn = VsnA}, #entity{vsn = VsnB}) ->
   case {uid:descend(VsnA, VsnB), uid:descend(VsnB, VsnA)} of
      % a -> b : accept request 
      {_, true} -> accept;
      % b -> a : skip request 
      {true, _} -> skip;
      % conflict
      {_,    _} -> conflict 
   end.


%%
%%
% handle({create, #entity{key = _Key, vsn = VsnA}=Entity}, Pipe, #{entity := #entity{vsn = VsnB}}=State0) ->
%    case {uid:descend(VsnA, VsnB), uid:descend(VsnB, VsnA)} of
%       %% request is descend of actor -> create actor if not exists
%       {true,  _} ->
%          {Result, State1} = create(Entity, State0),
%          pipe:ack(Pipe, Result),
%          tempus:timer(opts:val(ttl, undefined, ambit), ttl),
%          {next_state, handle, State1};

%       %% actor is descend of request -> skip create request
%       {_,  true} ->
%          pipe:ack(Pipe, {ok, Entity}),
%          {next_state, handle, State0};

%       %% conflict -> @todo: automatic conflict resolution 
%       _ ->
%          ?DEBUG("ambit [actor]: ~p create conflict with ~p", [_Key, uid:diff(VsnA, VsnB)]),
%          pipe:ack(Pipe, {error, conflict}),
%          {next_state, handle, State0}
%    end;

% handle({remove, #entity{key = _Key, vsn = VsnA}=Entity}, Pipe, #{entity := #entity{vsn = VsnB}}=State0) ->
%    case {uid:descend(VsnA, VsnB), uid:descend(VsnB, VsnA)} of
%       %% request is descend of actor -> remove actor if exists
%       {true,  _} ->
%          {Result, State1} = remove(Entity, State0),
%          pipe:ack(Pipe, Result),
%          case tempus:timer(opts:val(ttd, undefined, ambit), ttd) of
%             undefined ->
%                {stop, normal, State1};
%             _         ->
%                {next_state, handle, State1}
%          end;

%       %% actor is descend of request -> ignore request
%       {_,  true} ->
%          pipe:ack(Pipe, {ok, Entity}),
%          {next_state, handle, State0};

%       %% conflict -> @todo: automatic conflict resolution 
%       _ ->
%          ?DEBUG("ambit [actor]: ~p remove conflict with ~p", [_Key, uid:diff(VsnA, VsnB)]),
%          pipe:ack(Pipe, {error, conflict}),
%          {next_state, handle, State0}
%    end;







% handle(ttl, Tx, #{entity := #entity{vsn = Vsn}=Entity}=State) ->
%    handle({remove, Entity#entity{vsn = uid:vclock(Vsn)}}, Tx, State);

% handle(ttd,  _, #{entity := #entity{val = undefined}}=State) ->
%    {stop, normal, State};   

% handle(_, _, State) ->
%    {next_state, handle, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
% create(#entity{key = Key, val = {Mod, Fun, Arg}}=Entity0, #{sup := Sup, vnode := Vnode} = State) ->
%    Addr = ek:vnode(addr, Vnode),
%    % case ambit_actor_sup:init_service(Sup, {Mod, Fun, [Vnode | Arg]}) of
%    case erlang:apply(Mod, Fun, [Vnode | Arg]) of
%       {ok, Root} ->
%          case erlang:function_exported(Mod, process, 1) of
%             true  ->
%                {ok, Pid} = Mod:process(Root),
%                %% register actor process to the pool
%                _ = pns:register(ambit, {Addr, Key}, Pid),
%                Entity1 = entity(Entity0, State),
%                {{ok, Entity1}, State#{actor => Root, process => Pid,  entity => Entity1}};

%             false ->
%                %% register actor process to the pool
%                _ = pns:register(ambit, {Addr, Key}, Root),
%                Entity1 = entity(Entity0, State),
%                {{ok, Entity1}, State#{actor => Root, process => Root, entity => Entity1}}
%          end;
%       {error, {already_started, _}} ->
%          Entity1 = entity(Entity0, State),
%          {{ok, Entity1}, State#{entity => Entity1}};
%       {error, _} = Error ->
%          {Error, State}
%    end;

% create(#entity{} = Entity0, State) ->
%    Entity1 = entity(Entity0, State),
%    {{ok, Entity1}, State#{entity => Entity1}}.

%%
%%
% remove(#entity{key = Key} = Entity0, #{sup := Sup, vnode := Vnode} = State) ->
%    Addr = ek:vnode(addr, Vnode),
%    _ = ambit_actor_sup:free_service(Sup),
%    _ = pns:unregister(ambit, {Addr, Key}),
%    % _ = pns:unregister(Addr, Key),
%    Entity1 = entity(Entity0#entity{val = undefined}, State),
%    % @todo: ttl timer 
%    {{ok, Entity1}, State#{entity => Entity1}}.

%%
%%
entity(#entity{vsn = VsnA, val = {_, _, _}} = Entity0, #{entity := #entity{vsn = VsnB, vnode = Vnode}}) ->
   Entity1 = Entity0#entity{vsn = uid:join(VsnA, VsnB), vnode = Vnode},
   ?DEBUG("ambit [actor]: set entity ~p", [Entity1]),
   Entity1;

entity(#entity{vsn = VsnB}, #{entity := #entity{vsn = VsnA} = Entity0}) ->
   Entity1 = Entity0#entity{vsn = uid:join(VsnA, VsnB)},
   ?DEBUG("ambit [actor]: set entity ~p", [Entity1]),
   Entity1.



%%
%%
syncwith(Peer, #{entity := #entity{key = Key, val = undefined} = Entity}) ->
   ?DEBUG("ambit [actor]: sync (-) ~p with ~p", [Key, Peer]),
   Tx = ambit_peer:cast(Peer, {'$ambitz', free, Entity}),
   receive
      {Tx, _} ->
         ok
   after ?CONFIG_TIMEOUT_REQ ->
         ok
   end;

syncwith(Peer, #{entity := #entity{key = Key} = Entity, actor := Pid}) ->
   % @todo: sync internal state
   ?NOTICE("ambit [actor]: sync (+) ~p with ~p", [Key, Peer]),
   Tx = ambit_peer:cast(Peer, {'$ambitz', spawn, Entity}),
   receive
      {Tx, _} ->
         syncwith1(Peer, Pid, Entity),
         ok
   after ?CONFIG_TIMEOUT_REQ ->
      ?ERROR("ambit [actor]: sync recovery timeout ~p", [Peer])
   end.

syncwith1(Peer, Pid, #entity{key = Key} = Entity) ->
   try
      %% snapshot feature might not be supported by actor
      case pipe:ioctl(Pid, snapshot) of
         undefined ->
            ok;
         Snapshot ->
            Tx = ambit_peer:cast(Peer, Key, {'$ambitz', snapshot, Entity#entity{val = Snapshot}}),
            receive
               {Tx, _} ->
                  ok
            after ?CONFIG_TIMEOUT_REQ ->
               ?ERROR("ambit [actor]: sync recovery timeout ~p", [Peer])
            end
      end
   catch _:_ ->
      ok
   end.

