%% @description
%%    spawn request data structure (logic)
-module(ambit_req_spawn).

-export([
   accept/2
]).


%%
%%
accept(ok, #{peer := [_]} = Req) ->
   Req#{value => ok};

accept(ok, Req) ->
   Req#{value => ok}.

