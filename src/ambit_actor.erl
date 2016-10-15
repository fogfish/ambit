-module(ambit_actor).
-include("ambit.hrl").

-export([
   spawn/2,
   call/3
]).

%%
%% spawn actor (bridge to idle state)
-spec spawn(ek:vnode(), _) -> pid().

spawn(Vnode, Key) ->
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(Addr, Key) of
      undefined ->
         ?DEBUG("ambit [actor]: ~p spawn ~p", [Vnode, Key]),
         {ok, _} = pts:ensure(Addr, Key, [Vnode]),
         pns:whereis(Addr, Key);
      Pid ->
         Pid
   end.      
   
%%
%% request actor
-spec call(ek:vnode(), _, _) -> {ok, _} | {error, _}.

call(Vnode, Key, Req) ->
   Addr = ek:vnode(addr, Vnode),
   case pns:whereis(Addr, Key) of
      undefined ->
         {error, noroute};
      Pid ->
         pipe:call(Pid, Req)
   end.
