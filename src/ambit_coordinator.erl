%% @description
%%   client request coordinator
-module(ambit_coordinator).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,active/3
   % api
  ,create/2
  ,remove/2
  ,lookup/2
  ,whereis/2
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link(?MODULE, [], []).

init(_) ->
   lager:md([{ambit, req}]),
   {ok, idle, #{}}.

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
create(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
   call(ek:successors(ambit, Key), create, Entity#entity{vsn = uid:vclock(Vsn)}, Opts).

remove(#entity{key = Key, vsn = Vsn}=Entity, Opts) ->
   call(ek:successors(ambit, Key), remove, Entity#entity{vsn = uid:vclock(Vsn)}, Opts).

lookup(#entity{key = Key}=Entity, Opts) ->
   call(ek:successors(ambit, Key), lookup, Entity, Opts).

whereis(#entity{key = Key}=Entity, Opts) ->
   call(ek:successors(ambit, Key), whereis, Entity, Opts).

%%
%%
call([Peer | Peers], Req, Entity, Opts) ->
   case ambit_peer:coordinator(Peer) of
      {error, _} ->
         call(Peers, Req, Entity, Opts);
      UoW ->
         pipe:call(pq:pid(UoW), 
            {req, UoW, Req, Entity, Opts}, 
            opts:val(timeout, ?CONFIG_TIMEOUT_REQ, Opts)
         )
   end;

call([], _Req, _Entity, _Opts) ->
   {error, ebusy}.

%%%----------------------------------------------------------------------------   
%%%
%%% fsm
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle({req, UoW, Req, #entity{key = Key}=Entity, Opts}, Pipe, _State) ->
   ?NOTICE("coordinate ~p for ~p", [Req, Entity]),
   Peers = ek:successors(ambit, Key),
   case 
      opts:val(r, opts:val(w, ?CONFIG_N, Opts), Opts)
   of
      N when N > length(Peers) ->
         %% the ring return N distinct successors, the small cluster might fails
         %% to meet the requirement for some keys due to ring algorithm
         ?NOTICE("no quorum ~p, successors ~p", [Entity, Peers]),
         release(accept({error, quorum}, #{uow => UoW, pipe => Pipe, error => [], n => N})),
         {next_state, idle, #{}};
      N ->
         Tx = [{ambit_peer:cast(Vnode, {Req, Entity}), Vnode} || Vnode <- Peers],
         T  = tempus:timer(opts:val(timeout, ?CONFIG_TIMEOUT_REQ, Opts), timeout),
         {next_state, active, 
            #{
               uow    => UoW,
               pipe   => Pipe,
               req    => Req,
               tx     => Tx,
               t      => T,
               entity => Entity,
               error  => [],
               n      => N
            }
         }
   end;

idle(_, _, State) ->
   {next_state, idle, State}.

%%
%%
active({Tx, Value}, _, #{tx := List}=Req0) ->
   Req1 = accept(Value, Req0),
   case lists:keytake(Tx, 1, List) of
      {value, {_Tx, _Vnode},   []} ->
         release(Req1),
         {next_state, idle, #{}};
      {value, {_Tx, _Vnode}, Tail} ->
         {next_state, active, Req1#{tx => Tail}}
   end;

active(timeout, _, Req0) ->
   Req1 = accept({error, timeout}, Req0),
   release(Req1),
   {stop, normal, #{}};

active(_, _, State) ->
   {next_state, active, State}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
accept({ok, EntityA}, #{n := N, entity := EntityB} = Req) ->
   Req#{n => N - 1, entity => entity(EntityA, EntityB)};

accept(EntityA, #{n := N, entity := EntityB} = Req)
 when is_pid(EntityA) ->
   Req#{n => N - 1, entity => entity(EntityA, EntityB)};

accept(undefined, #{n := N} = Req) ->
   Req#{n => N - 1};

accept({error, Reason}, #{error := Error} = Req) ->
   Req#{error => [Reason | Error]}.


%%
%%
release(#{t := T, uow := UoW, pipe := Pipe}=Req) ->
   tempus:cancel(T),
   Unit = unit(Req),
   pipe:a(Pipe, Unit),
   ?DEBUG("complete ~p", [Unit]),
   pq:release(UoW);

release(#{uow := UoW, pipe := Pipe}=Req) ->
   Unit = unit(Req),
   pipe:a(Pipe, Unit),
   ?DEBUG("failure ~p", [Unit]),
   pq:release(UoW).


%%
%%
unit(#{req := whereis, n := N, entity := #entity{val = Val}})
 when N =< 0 ->
   clue:inc({ambit, req, success}),
   lists:reverse(Val);

unit(#{req := whereis, n := N})
 when N > 0 ->
   clue:inc({ambit, req, failure}),
   [];

unit(#{n := N, entity := Entity})
 when N =< 0 ->
   clue:inc({ambit, req, success}),
   {ok, Entity};

unit(#{n := N, error := []})
 when N > 0  ->
   clue:inc({ambit, req, failure}),
   {error, unity};

unit(#{n := N, error := [Reason]})
 when N > 0  ->
   clue:inc({ambit, req, failure}),
   {error, Reason};

unit(#{n := N, error := Reason})
 when N > 0  ->
   clue:inc({ambit, req, failure}),
   {error, lists:usort(Reason)}.

%%
%% merge entity properties
entity(A, #entity{val = Val}=B)
 when is_pid(A) ->
   B#entity{val = [A | Val]};

entity(#entity{val = Val, vsn=VsnA}, #entity{val = undefined, vsn=VsnB}=B) ->
   B#entity{val = Val, vsn = uid:join(VsnB, VsnA)};

entity(#entity{vsn=VsnA}, #entity{vsn=VsnB}=B) ->
   B#entity{vsn = uid:join(VsnB, VsnA)}.


