%% @description
%%    where request data structure (logic)
-module(ambit_req_whereis).

-export([
   accept/2
]).

%%
%%
accept(Pid, #{value := Value} = Req)
 when is_pid(Pid) ->
   Req#{value => [Pid | Value]};
accept(_,   #{value := Value} = Req) ->
   Req;
accept(Pid, Req) ->
   accept(Pid, Req#{value => []}).
