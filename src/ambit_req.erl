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
idle(Req, Tx, _State) ->
   {next_state, exec, 
      #{
         req  => erlang:element(1, Req)
        ,ref  => request(erlang:element(2, Req), Req)
        ,ret  => []
        ,tx   => Tx
      },
      ?CONFIG_TIMEOUT_REQ
   }.

exec({'DOWN', Ref, process, _Pid, Reason}, _Tx, #{req := Req, ref := Ref0, ret := Ret0, tx := Tx}=State) ->
   case lists:keytake(Ref, 2, Ref0) of
      {value, {Vnode, _, _},   []} ->
         pipe:ack(Tx, return(Req, [{type(Vnode), {error, Reason}} | Ret0])),
         {next_state, idle, #{}};

      {value, {Vnode, _, _}, Refs} ->
         {next_state, exec, 
            State#{
               ref => Refs
              ,ret => [{type(Vnode), {error, Reason}} | Ret0]
            }
         }
   end;

exec({Ref, Result}, _Tx, #{req :=Req, ref := Ref0, ret := Ret0, tx := Tx}=State) ->
   case lists:keytake(Ref, 3, Ref0) of
      {value, {Vnode, Mref, _},   []} ->
         erlang:demonitor(Mref, [flush]),
         pipe:ack(Tx, return(Req, [{type(Vnode), Result} | Ret0])),
         {next_state, idle, 
            State#{
               ret => [Result | Ret0]
            }
         };

      {value, {Vnode, Mref, _}, Refs} ->
         erlang:demonitor(Mref, [flush]),
         {next_state, exec, 
            State#{
               ref => Refs
              ,ret => [{type(Vnode), Result} | Ret0]
            }
         }         
   end;

exec(timeout, _Tx, _State) ->
   {next_state, idle, #{}}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
type({Type, _Addr, _Key, _Peer}) ->
   Type.

%%
%%
request(Ns, Req) ->
   lists:map(
      fun({_Type, _Addr, _Key, Peer}=Vnode) ->
         ?DEBUG("ambit [req]: vnode ~p", [Vnode]),
         Mref = erlang:monitor(process, Peer),
         Ref  = pipe:cast(Peer, {Vnode, Req}),
         {Vnode, Mref, Ref}
      end,
      ek:successors(ambit, Ns)
   ).

%%
%% 
return(init, List) ->
   %% init is successful if it is executed by any primary shard or handoff
   case [X || {primary, {ok, X}} <- List] of
      [] ->
         case [ok || {handoff,  ok} <- List] of
            [] ->
               {error, [X || {_, {error, X}} <- List]};
            _  ->
               {ok, []}
         end;
      Pid ->
         {ok, Pid}
   end;

return(free, List) ->
   %% free is successful if it is executed by each primary shard
   case [X || {primary, {error, X}} <- List] of
      []  ->
         ok;
      Reason ->
         {error, Reason}
   end;

return(whereis, List) ->
   [X || {_, X} <- List, is_pid(X)].



