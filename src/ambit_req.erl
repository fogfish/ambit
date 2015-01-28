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
   new(erlang:element(1, Msg), Msg, Timeout).

new(spawn, Msg, T) ->
   #{mod => ambit_req_spawn, msg => Msg, t => T};

new(free, Msg, T) ->
   #{mod => ambit_req_free, msg => Msg, t => T};

new(whereis, Msg, T) ->
   #{mod => ambit_req_whereis, msg => Msg, t => T}.


%%
%%
-spec(free/1 :: (req()) -> ok).

free(#{pipe := Pipe, value := Value} = Req) ->
   pipe:a(Pipe, Value),
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
   Req#{t => tempus:timer(Msg, Timeout)}.


%%
%% who is responsible to coordinate key and execute request
-spec(whois/2 :: (any(), req()) -> {pid(), req()}).

whois(Key, Req) ->
   [{_, _, _, Pid} | _] = Peers = ambit:sibling(fun ek:successors/2, Key),
   {Pid, Req#{peer => Peers}}.

   % %% @todo filter unique nodes
   % Peers = ek:successors(ambit, Key),
   % case lists:dropwhile(fun({X, _, _, _}) -> X =/= primary end, Peers) of
   %    [] ->
   %       {_, _, _, Pid} = Vnode = hd(Peers),
   %       Node = erlang:node(Pid),
   %       Peer = [X || {_, _, _, P} = X <- Peers, erlang:node(P) =/= Node],
   %       {Pid, Req#{peer => [Vnode | Peer]}};
   %    L  ->
   %       {_, _, _, Pid} = Vnode = hd(L),
   %       Node = erlang:node(Pid),
   %       Peer = [X || {_, _, _, P} = X <- Peers, erlang:node(P) =/= Node],
   %       {Pid, Req#{peer => [Vnode | Peer]}}
   % end.


%%
%% lease unit of work to handle request
-spec(lease/2 :: (pid(), req()) -> {pid(), req()}).

lease(Peer, Req) ->
   UoW = ambit_peer:coordinator(Peer),
   {pq:pid(UoW), Req#{uow => UoW}}.

%%
%% bind request with pipe
-spec(pipe/2 :: (any(), req()) -> req()).

pipe(Pipe, #{} = Req) ->
   Req#{pipe => Pipe}.

%%
%% accept vnode result
-spec(accept/2 :: (any(), req()) -> {atom(), req()}).

accept(Value, #{mod := Mod} = Req) ->
   Mod:accept(Value, Req);

accept(Value, Req) ->
   Req#{value => Value}.

