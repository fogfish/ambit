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
%% size of ambit api request pool (concurrent operations)
-ifndef(CONFIG_POOL_REQ).
-define(CONFIG_POOL_REQ,  100).
-endif.

%%
%% request timeout in milliseconds
-ifndef(CONFIG_TIMEOUT_REQ).
-define(CONFIG_TIMEOUT_REQ,  30000).
-endif.

%%
%% anti entropy frequency in milliseconds
-ifndef(CONFIG_CYCLE_AE).
-define(CONFIG_CYCLE_AE,     10000).
-endif.



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
