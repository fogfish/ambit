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
   % message/1,

   whois/2,
   lease/2,
   pipe/2,
   accept/2
]).

-type(req() :: #{}).

%%
%%
-spec(new/1 :: (any()) -> req()).

new(#{msg := Msg, t := T}) ->
   #{msg => Msg, t => T};

new(Msg) ->
   new(Msg, 5000).

new(Msg, Timeout) ->
   new(erlang:element(1, Msg), Msg, Timeout).

new(spawn, Msg, T) ->
   #{mod => ambit_req_spawn, msg => Msg, t => T}.


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
%% set timeout
t(Msg, #{t := Timeout} = Req) ->
   Req#{t => tempus:timer(Msg, Timeout)}.


%%
%% who is responsible to coordinate key and execute request
-spec(whois/2 :: (any(), req()) -> {pid(), req()}).

whois({_, _, _, Pid} = Vnode, Req) ->
   {Pid, Req#{peer => [Vnode]}};

whois(Key, Req) ->
   Peers = ek:successors(ambit, Key),
   case lists:dropwhile(fun({X, _, _, _}) -> X =/= primary end, Peers) of
      [] ->
         {_, _, _, Pid} = Vnode = hd(Peers),
         Node = erlang:node(Pid),
         Peer = [X || {_, _, _, P} = X <- Peers, erlang:node(P) =/= Node],
         {Pid, Req#{peer => [Vnode | Peer]}};
      L  ->
         {_, _, _, Pid} = Vnode = hd(L),
         Node = erlang:node(Pid),
         Peer = [X || {_, _, _, P} = X <- Peers, erlang:node(P) =/= Node],
         {Pid, Req#{peer => [Vnode | Peer]}}
   end.


%%
%% lease unit of work to handle request
-spec(lease/2 :: (pid(), req()) -> {pid(), req()}).

lease(Pool, Req) ->
   UoW = pq:lease(Pool),
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



% -behaviour(pipe).

% -include("ambit.hrl").

% -export([
%    start_link/0
%   ,init/1
%   ,free/2
%   ,ioctl/2
%   ,idle/3
%   ,exec/3
% ]).

% %%%----------------------------------------------------------------------------   
% %%%
% %%% Factory
% %%%
% %%%----------------------------------------------------------------------------   

% start_link() ->
%    pipe:start_link(?MODULE, [], []).

% init([]) ->
%    {ok, idle, #{}}.

% free(_, _) ->
%    ok.

% ioctl(_, _) ->
%    throw(not_implemented).

% %%%----------------------------------------------------------------------------   
% %%%
% %%% pipe
% %%%
% %%%----------------------------------------------------------------------------   

% %%
% %%
% idle({Policy, Name, Fun}, Tx, _State) ->
%    {next_state, exec, 
%       #{
%          policy  => Policy
%         ,req     => request(Name, Fun)
%         ,ret     => []
%         ,tx      => Tx
%       },
%       ?CONFIG_TIMEOUT_REQ
%    }.

% exec({'DOWN', Ref, process, _Pid, Reason}, _Tx, #{policy := Policy, req := Req0, ret := Ret0, tx := Tx}=State) ->
%    case lists:keytake(Ref, 2, Req0) of
%       {value, {Vnode, _, _},  []} ->
%          pipe:ack(Tx, return(Policy, [{type(Vnode), {error, Reason}} | Ret0])),
%          {next_state, idle, #{}};

%       {value, {Vnode, _, _}, Req} ->
%          {next_state, exec, 
%             State#{
%                req => Req
%               ,ret => [{type(Vnode), {error, Reason}} | Ret0]
%             }
%          }
%    end;

% exec({Vnode, Result}, _Tx, #{policy := Policy, req := Req0, ret := Ret0, tx := Tx}=State) ->
%    case lists:keytake(Vnode, 1, Req0) of
%       {value, {Vnode, Mref},   []} ->
%          erlang:demonitor(Mref, [flush]),
%          pipe:ack(Tx, return(Policy, [{type(Vnode), Result} | Ret0])),
%          {next_state, idle, 
%             State#{
%                ret => [Result | Ret0]
%             }
%          };

%       {value, {Vnode, Mref}, Req} ->
%          erlang:demonitor(Mref, [flush]),
%          {next_state, exec, 
%             State#{
%                req => Req
%               ,ret => [{type(Vnode), Result} | Ret0]
%             }
%          }         
%    end;

% exec(timeout, _Tx, #{tx := Tx}=State) ->
%    pipe:ack(Tx, {error, timeout}),
%    {stop, normal, State}.


% %%%----------------------------------------------------------------------------   
% %%%
% %%% private
% %%%
% %%%----------------------------------------------------------------------------   

% %%
% %% return type of vnode
% type({Type, _Addr, _Key, _Peer}) ->
%    Type.

% %%
% %% request each vnode
% request(Name, Fun) ->
%    lists:map(
%       fun({_Type, _Addr, _Key, Peer}=Vnode) ->
%          ?DEBUG("ambit [req]: vnode ~p", [Vnode]),
%          Mref = erlang:monitor(process, Peer),
%          _    = Fun(Vnode),
%          {Vnode, Mref}
%       end,
%       ek:successors(ambit, Name)
%    ).

% %%
% %% 
% return(any, List) ->
%    %% request is successful if it is executed by any primary or handoff shards
%    case [X || {primary, {ok, X}} <- List] of
%       [] ->
%          case [ok || {handoff, ok} <- List] of
%             [] ->
%                {error, [X || {_, {error, X}} <- List]};
%             _  ->
%                {ok, []}
%          end;
%       Val ->
%          {ok, Val}
%    end;

% return(all, List) ->
%    %% request is successful if it is executed by each primary shard
%    case [X || {primary, {error, X}} <- List] of
%       []  ->
%          ok;
%       Reason ->
%          {error, Reason}
%    end.


