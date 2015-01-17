%% @description
%%    free request data structure (logic)
-module(ambit_req_free).

-export([
   accept/2
]).


%%
%%
accept(ok, Req) ->
   Req#{value => ok}.
