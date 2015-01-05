%% @description
%%   distribute request  
-module(ambit_req).
-behaviour(pipe).

-include("ambit.hrl").

-export([
   start_link/0
  ,init/1
  ,free/2
  ,ioctl/2
  ,idle/3
  ,exec/3
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link() ->
   pipe:start_link(?MODULE, [], []).

init([]) ->
   {ok, idle, #{}}.

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
idle({Policy, Name, Fun}, Tx, _State) ->
   {next_state, exec, 
      #{
         policy  => Policy
        ,req     => request(Name, Fun)
        ,ret     => []
        ,tx      => Tx
      },
      ?CONFIG_TIMEOUT_REQ
   }.

exec({'DOWN', Ref, process, _Pid, Reason}, _Tx, #{policy := Policy, req := Req0, ret := Ret0, tx := Tx}=State) ->
   case lists:keytake(Ref, 2, Req0) of
      {value, {Vnode, _, _},  []} ->
         pipe:ack(Tx, return(Policy, [{type(Vnode), {error, Reason}} | Ret0])),
         {next_state, idle, #{}};

      {value, {Vnode, _, _}, Req} ->
         {next_state, exec, 
            State#{
               req => Req
              ,ret => [{type(Vnode), {error, Reason}} | Ret0]
            }
         }
   end;

exec({Vnode, Result}, _Tx, #{policy := Policy, req := Req0, ret := Ret0, tx := Tx}=State) ->
   case lists:keytake(Vnode, 1, Req0) of
      {value, {Vnode, Mref},   []} ->
         erlang:demonitor(Mref, [flush]),
         pipe:ack(Tx, return(Policy, [{type(Vnode), Result} | Ret0])),
         {next_state, idle, 
            State#{
               ret => [Result | Ret0]
            }
         };

      {value, {Vnode, Mref}, Req} ->
         erlang:demonitor(Mref, [flush]),
         {next_state, exec, 
            State#{
               req => Req
              ,ret => [{type(Vnode), Result} | Ret0]
            }
         }         
   end;

exec(timeout, _Tx, #{tx := Tx}=State) ->
   pipe:ack(Tx, {error, timeout}),
   {stop, normal, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%% return type of vnode
type({Type, _Addr, _Key, _Peer}) ->
   Type.

%%
%% request each vnode
request(Name, Fun) ->
   lists:map(
      fun({_Type, _Addr, _Key, Peer}=Vnode) ->
         ?DEBUG("ambit [req]: vnode ~p", [Vnode]),
         Mref = erlang:monitor(process, Peer),
         _    = Fun(Vnode),
         {Vnode, Mref}
      end,
      ek:successors(ambit, Name)
   ).

%%
%% 
return(any, List) ->
   %% request is successful if it is executed by any primary or handoff shards
   case [X || {primary, {ok, X}} <- List] of
      [] ->
         case [ok || {handoff, ok} <- List] of
            [] ->
               {error, [X || {_, {error, X}} <- List]};
            _  ->
               {ok, []}
         end;
      Val ->
         {ok, Val}
   end;

return(all, List) ->
   %% request is successful if it is executed by each primary shard
   case [X || {primary, {error, X}} <- List] of
      []  ->
         ok;
      Reason ->
         {error, Reason}
   end.


