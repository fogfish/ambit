%%
%% default ring
-ifndef(CONFIG_RING).
-define(CONFIG_RING, [
   {type, ring}            %% ring management algorithms - chord | ring
  ,{m,      64}            %% ring modulo
  ,{q,    4096}            %% number of shards
  ,{n,       3}            %% number of replica
  ,{hash,  sha}            %% hashing algorithm
]).
-endif.

%%
%% default R/W parameters
-define(CONFIG_N,      1).

%%
%% heap of vnode processes
-define(HEAP_VNODE, [
   {keylen,      inf}
  ,{supervisor,  ambit_vnode_sup}
  ,{factory,     transient}
]).

%%
%% heap of actor processes
-define(HEAP_ACTOR, [
   {keylen,      inf}
  ,{supervisor,  ambit_actor_sup}
  ,{factory,     temporary}
]).


%%
%% size of ambit api request pool (capacity of coordinator)
-ifndef(CONFIG_IO_POOL).
-define(CONFIG_IO_POOL,  100).
-endif.

%%
%% request timeout in milliseconds
-ifndef(CONFIG_TIMEOUT_REQ).
-define(CONFIG_TIMEOUT_REQ,  30000).
-endif.

%%
%% active anti entropy configuration
%%  * frequency in milliseconds
%%  * capacity number of parallel sessions
-ifndef(CONFIG_AAE_TIMEOUT).
-define(CONFIG_AAE_TIMEOUT,   {120000, 1.0}).
-endif.

-ifndef(CONFIG_AAE_CAPACITY).
-define(CONFIG_AAE_CAPACITY,  5).
-endif.

%%
%% ambit entity
-record(entity, {
   %% @todo: address or name so that addr is used for vnode lookup, key as instance naming
   key  = undefined :: binary(),
   val  = undefined :: any(),
   vsn  = []        :: uid:vclock()
}).


%% 
%% logger macros
%%   debug, info, notice, warning, error, critical, alert, emergency
-ifndef(EMERGENCY).
-define(EMERGENCY(Fmt, Args), lager:emergency(Fmt, Args)).
-endif.

-ifndef(ALERT).
-define(ALERT(Fmt, Args), lager:alert(Fmt, Args)).
-endif.

-ifndef(CRITICAL).
-define(CRITICAL(Fmt, Args), lager:critical(Fmt, Args)).
-endif.

-ifndef(ERROR).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-endif.

-ifndef(WARNING).
-define(WARNING(Fmt, Args), lager:warning(Fmt, Args)).
-endif.

-ifndef(NOTICE).
-define(NOTICE(Fmt, Args), lager:notice(Fmt, Args)).
-endif.

-ifndef(INFO).
-define(INFO(Fmt, Args), lager:info(Fmt, Args)).
-endif.

-ifdef(CONFIG_DEBUG).
   -define(DEBUG(Str, Args), lager:debug(Str, Args)).
-else.
   -define(DEBUG(Str, Args), ok).
-endif.
