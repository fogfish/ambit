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
  ,call/3
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
%% request coordinator
-spec(call/3 :: (any(), any(), list()) -> {ok, any()} | {error, any()}).

call(Key, Req, Opts) ->
   Peers = ambit:sibling(fun ek:successors/2, Key),
   call(Peers, Key, Req, Opts).

call([Peer | Peers], Key, Req, Opts) ->
   case ambit_peer:coordinator(Peer) of
      {error, _} ->
         call(Peers, Key, Req, Opts);
      UoW ->
         pipe:call(pq:pid(UoW), 
            {req, UoW, Key, Req, Opts}, 
            opts:val(timeout, ?CONFIG_TIMEOUT_REQ, Opts)
         )
   end;

call([], _Key, _Req0, _Opts) ->
   {error, ebusy}.

   % Req1 = ambit_req:new(Req0, Opts),
   % {Pid, Req2} = ambit_req:whois(Key, Req1),
   % case ambit_req:quorum(Req2) of
   %    false ->
   %       {error, quorum};
   %    true  ->
   %       case ambit_req:lease(Pid, Req2) of
   %          {error, _} = Error ->
   %             Error;
   %          {UoW, Req3} ->
   %             pipe:call(UoW, Req3, ambit_req:t(Req3))
   %       end
   % end.

%%%----------------------------------------------------------------------------   
%%%
%%% fsm
%%%
%%%----------------------------------------------------------------------------   

%%
%%
idle({req, UoW, Key, Req, Opts}, Pipe, _State) ->
   ?NOTICE("request ~p", [Req]),
   Peers = ambit:sibling(fun ek:successors/2, Key), 
   case 
      opts:val(r, opts:val(w, ?CONFIG_N, Opts), Opts)
   of
      N when N > length(Peers) ->
         release(accept({error, quorum}, #{uow => UoW, pipe => Pipe})),
         {next_state, idle, #{}};
      _ ->
         Tx = [{ambit_peer:cast(Vnode, Req), Vnode} || Vnode <- Peers],
         T  = tempus:timer(opts:val(timeout, ?CONFIG_TIMEOUT_REQ, Opts), timeout),
         {next_state, active, 
            #{
               uow   => UoW,
               pipe  => Pipe,
               req   => erlang:element(1, Req),
               tx    => Tx,
               t     => T,
               value => []
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
accept({ok, _}, #{value := Value} = Req) ->
   Req#{value => [ok | Value]};

accept({error,  quorum}, Req) ->
   Req#{value => {error, quorum}};

accept(Vx, #{value := Value} = Req) ->
   Req#{value => [Vx | Value]}.


%%
%%
release(#{value := {error, _} = Err, t := T, uow := UoW, pipe := Pipe}) ->
   clue:inc({ambit, req, failure}),
   tempus:cancel(T),
   pipe:a(Pipe, Err),
   ?DEBUG("failure ~p", [Err]),
   pq:release(UoW);

release(#{value := {error, _} = Err, uow := UoW, pipe := Pipe}) ->
   clue:inc({ambit, req, failure}),
   pipe:a(Pipe, Err),
   ?DEBUG("failure ~p", [Err]),
   pq:release(UoW);

release(#{t := T, uow := UoW, pipe := Pipe}=Req) ->
   tempus:cancel(T),
   Unit = unit(Req),
   pipe:a(Pipe, Unit),
   ?DEBUG("complete ~p", [Unit]),
   pq:release(UoW).

%%
%%
unit(#{req := spawn, value := Values}) ->
   case 
      lists:partition(fun(X) -> X =:= ok end, Values)
   of
      %% no positive results, transaction is failed
      {[], Error} -> 
         clue:inc({ambit, req, failure}),
         hd(Error);
      {_,      _} -> 
         clue:inc({ambit, req, success}),
         ok
   end;

unit(#{req := free, value := Values}) ->
   case
      lists:partition(fun(X) -> X =:= ok end, Values)
   of
      {_,     []} -> 
         clue:inc({ambit, req, success}),
         ok;
      %% there is a negative result 
      {_,  Error} -> 
         clue:inc({ambit, req, failure}),
         hd(Error)
   end;

unit(#{req := whereis, value := Values}) ->
   clue:inc({ambit, req, success}),
   [X || X <- Values, is_pid(X)].

