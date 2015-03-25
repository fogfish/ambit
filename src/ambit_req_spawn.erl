%% @description
%%    spawn request data structure (logic)
-module(ambit_req_spawn).

-export([
   accept/2
]).


%%
%%
accept({ok, _}, Req) ->
   Req#{value => ok};
accept({error, _} = Err, Req) ->
   Req#{value => Err};
accept(X, Req) ->
   %% {'EXIT',noproc} <- look pts
   io:format("==> ~p~n", [X]),
   Req#{value => {error, fff}}.

