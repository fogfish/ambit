%% @description
%%   request  
-module(ambit_req).

-include("ambit.hrl").

-export([
   new/2,
   free/1,

   vnode/1,
   peers/1,

   t/1,
   t/2,
   payload/1,

   whois/2,
   lease/2,
   pipe/2,
   accept/2,
   quorum/1
]).

-type(req() :: #{}).

%%
%%
-spec(new/2 :: (any(), list()) -> req()).

new(Msg, Opts) ->
	#{
      req   => erlang:element(1, Msg)
     ,msg   => Msg
     ,at    => os:timestamp()
     ,t     => opts:val(timeout, ?CONFIG_TIMEOUT_REQ, Opts)
     ,n     => opts:val(r, opts:val(w, ?CONFIG_N, Opts), Opts)
     ,value => []
   }.

%%
%%
-spec(free/1 :: (req()) -> ok).

free(#{pipe := Pipe, req := Id, value := Value} = Req) ->
   pipe:a(Pipe, unit(Id, Value)),
   free(maps:remove(pipe, Req));

free(#{t := T} = Req) ->
   tempus:cancel(T),
   free(maps:remove(t, Req));

free(#{uow := UoW} = Req) ->
   pq:release(UoW),
   free(maps:remove(uow, Req));

free(#{at := T}) ->
   clue:usec({ambit, req, latency}, T),
   ok.

%%
%% return vnode associated with request
-spec(vnode/1 :: (req()) -> ek:vnode()).

vnode(#{peer := [Vnode | _]}) ->
   Vnode.

%%
%% return list of peers associated with request
-spec(peers/1 :: (req()) -> [ek:vnode()]).

peers(#{peer := Peers}) ->
   Peers.

%%
%% return request payload
-spec(payload/1 :: (req()) -> [ek:vnode()]).

payload(#{msg := Payload}) ->
   Payload.


%%
%% set timeout
t(Msg, #{t := Timeout} = Req) ->
   Req#{t => tempus:timer(Timeout, Msg)}.

t(#{t := Timeout}) ->
   Timeout.

%%
%% who is responsible to coordinate key and execute request
-spec(whois/2 :: (any(), req()) -> {pid(), req()}).

whois(Key, Req) ->
   [{_, _, _, Pid} | _] = Peers = ambit:sibling(fun ek:successors/2, Key),
   {Pid, Req#{peer => Peers}}.

%%
%% lease unit of work to handle request
-spec(lease/2 :: (pid(), req()) -> {pid(), req()}).

lease(Peer, Req) ->
	case ambit_peer:coordinator(Peer) of
		{error, _} = Error ->
			Error;
		UoW ->
   		{pq:pid(UoW), Req#{uow => UoW}}
	end.

%%
%% bind request with pipe
-spec(pipe/2 :: (any(), req()) -> req()).

pipe(Pipe, Req) ->
   Req#{pipe => Pipe}.

%%
%% accept vnode result
-spec(accept/2 :: (any(), req()) -> {atom(), req()}).

accept({ok, _}, #{value := Value} = Req) ->
	Req#{value => [ok | Value]};
accept(Vx, #{value := Value} = Req) ->
	Req#{value => [Vx | Value]}.

%%
%%
unit(spawn,   Values) ->
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

unit(free,    Values) ->
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

unit(whereis, Values) ->
   clue:inc({ambit, req, success}),
	[X || X <- Values, is_pid(X)].

%%
%% check if requires meet sloppy quorum 
quorum(#{n := N, peer := Peers}) ->
   length(Peers) >= N.



