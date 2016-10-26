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
-module(ambit_actor_bridge).
-behaviour(pipe).

-include("ambit.hrl").
-include_lib("ambitz/include/ambitz.hrl").

-export([
   start_link/3
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

start_link(Addr, Key, Vnode) ->
   pipe:start_link(?MODULE, [Addr, Key, Vnode], []).

init([Addr, Key, Vnode]) ->
   ?DEBUG("ambit [actor]: ~p init ~p", [Vnode, Key]),
   pns:register(Addr, Key, self()),
   {ok, Sup} = ambit_actor_bridge_sup:start_link(),
   {ok, handle, 
      #{
         sup     => Sup
        ,entity  => #entity{key = Key, vnode = [Vnode]}
      }
   }.

free(_Reason, #{entity := #entity{key = _Key, vnode = [_Vnode]}}) ->
   ?DEBUG("ambit [actor]: free ~p at ~p with ~p", [_Key, _Vnode, _Reason]),
   ok.

%%
%%
ioctl(service, #{entity  := Service}) ->
   % return entity definition
   Service;
ioctl(_, _) ->
   throw(not_implemented).

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

handle({handoff, Peer}, Pipe, State) ->
   pipe:ack(Pipe, syncwith(Peer, State)),
   {next_state, handle, State};

handle({sync, Peer}, Pipe, State) ->
   pipe:ack(Pipe, syncwith(Peer, State)),
   {next_state, handle, State};

handle(ttl,  _, State) ->
   {stop, normal, State};

handle({'DOWN', _, process, Pid, _Reason} = Down, Pipe, #{sup := Sup, actor := Pid} = State) ->
   case 
      lists:keyfind(actor, 1, supervisor:which_children(Sup))
   of
      false ->
         {stop, normal, State};
      {actor, undefined,  _, _} ->
         {stop, normal, State};
      {actor, restarting, _, _} ->
         handle(Down, Pipe, State);
      {actor, New, _, _} ->
         erlang:monitor(process, New),
         {next_state, handle, State#{actor => New}}
   end;

handle({'DOWN', _, process, _Pid, _Reason}, _Pipe, State) ->
   {next_state, handle, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
accept({put, Lens}, Entity, State) ->
   {put(Lens, Entity, State), State};

accept({get, Lens}, Entity, State) ->
   {get(Lens, Entity, State), State};

accept(lookup, Entity, State) ->
   {lookup(Entity, State), State};

accept(whereis, Entity, State) ->
   {discover(Entity, State), State};

accept(Msg, #entity{val = B} = EntityB, #{entity := #entity{val = A}} = State) ->
   accept(
      crdts:descend(A, B),
      crdts:descend(B, A),
      Msg, EntityB, State
   ).

accept(true, false, {spawn, Supervise}, EntityB, State0) ->
   case create(Supervise, EntityB, State0) of
      {ok, #{entity := EntityA} = State1} ->
         Entity = join(EntityA, EntityB),
         {{ok, Entity}, State1#{entity => Entity}};
      {error,   _} = Error ->
         {Error, State0}
   end;

accept(true, false, free, EntityB, State0) ->
   case remove(EntityB, State0) of
      {ok, #{entity := EntityA} = State1} ->
         Entity = join(EntityA, EntityB),
         tempus:timer(opts:val(ttl, undefined, ambit), ttl),
         {{ok, Entity}, State1#{entity => Entity}};
      {error,   _} = Error ->
         {Error, State0}
   end;
   
accept(_, _, _Msg, _EntityB, #{entity := #entity{key =_Key} = EntityA} = State) ->
   ?DEBUG("ambit [actor]: ~p skips ~p", [_Key, _Msg]),
   {{ok, EntityA}, State}.

%%
%%
join(#entity{val = A} = EntityA, #entity{val = B}) ->
   EntityA#entity{val = crdts:join(A, B)}.

%%
%%
create(_, _Entity, #{actor := _} = State) ->
   %% @todo: re-spawn actor if signature different
   {ok, State};
create(Supervise, #entity{val = B}, #{sup := Sup, entity := #entity{vnode = [Vnode]}} = State) ->
   {M, F, A} = crdts:value(B),
   case
      supervisor:start_child(Sup,
         {actor, {M, F, [Vnode|A]} , Supervise, 10000, worker, dynamic}
      )
   of
      {ok, Pid} ->
         erlang:monitor(process, Pid),
         {ok, State#{actor => Pid}};
      {error, _} = Error ->
         {Error, State}
   end.

%%
%%
remove(_Entity, #{sup := Sup, actor := Pid} = State) ->
   supervisor:terminate_child(Sup, actor),
   supervisor:delete_child(Sup, actor),
   erlang:exit(Pid, normal),
   {ok, maps:remove(actor, State)};
remove(_Entity, State) ->
   {ok, State}.

%%
%%
put(Lens, #entity{val = A} = Entity, #{actor := Pid, entity := #entity{vnode = Vnode}}) ->
   {ok, B} = pipe:call(Pid, {put, Lens, A}),
   {ok, Entity#entity{vnode = Vnode, val = B}};

put(_, #entity{} = Entity, #{entity := #entity{vnode = Vnode}}) ->
   {ok, Entity#entity{vnode = Vnode, val = undefined}}.


%%
%%
get(Lens, #entity{} = Entity, #{actor := Pid, entity := #entity{vnode = Vnode}}) ->
   {ok, B} = pipe:call(Pid, {get, Lens}),
   {ok, Entity#entity{vnode = Vnode, val = B}};

get(_, #entity{} = Entity, #{entity := #entity{vnode = Vnode}}) ->
   {ok, Entity#entity{vnode = Vnode, val = undefined}}.


%%
%%
lookup(_, #{entity := Entity}) ->
   {ok, Entity}.

%%
%%
discover(#entity{val = Val} = Entity, #{actor := Pid, entity := #entity{vnode = Vnode}}) ->
   {ok, Entity#entity{vnode = Vnode, val = crdts:update(Pid, Val)}};

discover(#entity{} = Entity, #{entity := #entity{vnode = Vnode}}) ->
   {ok, Entity#entity{vnode = Vnode}}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
syncwith(Peer, #{entity := #entity{key = _Key} = Entity, actor := _Pid}) ->
   % @todo: sync internal state
   ?DEBUG("ambit [actor]: sync (+) ~p with ~p", [_Key, Peer]),
   ambit:call(Peer, {'$ambitz', spawn, Entity}),
   ok;

syncwith(Peer, #{entity := #entity{key = _Key} = Entity}) ->
   ?DEBUG("ambit [actor]: sync (-) ~p with ~p", [_Key, Peer]),
   ambit:call(Peer, {'$ambitz', free, Entity}),
   ok.
