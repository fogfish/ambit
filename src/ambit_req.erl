%% @description
%%   request  
-module(ambit_req).

-include("ambit.hrl").

-export([
   new/1,
   new/2,
   free/1,

   vnode/1,
   peers/1,

   t/2,
   payload/1,

   whois/2,
   lease/2,
   pipe/2,
   accept/2
]).

-type(req() :: #{}).

%%
%%
-spec(new/1 :: (any()) -> req()).

new(Msg) ->
   new(Msg, 5000).

new(Msg, Timeout)
 when is_integer(Timeout) ->
	#{req => erlang:element(1, Msg), msg => Msg, t => Timeout, value => []}.

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

free(_) ->
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


unit(spawn,   Values) ->
	case 
		lists:partition(fun(X) -> X =:= ok end, Values)
	of
		%% no positive results, transaction is failed
		{[], Error} -> hd(Error);
		{_,      _} -> ok
	end;

unit(free,    Values) ->
	case
		lists:partition(fun(X) -> X =:= ok end, Values)
	of
		{_,     []} -> ok;
		%% there is a negative result 
		{_,  Error} -> hd(Error)
	end;

unit(whereis, Values) ->
	[X || X <- Values, is_pid(X)].




