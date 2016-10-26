%%
%%   Copyright 2014 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   Supervisor for actor process(es). It manages both actual actor and controlling processes.
%%   The supervisor is required to spawn "arbitrary" client process and integrate it to common
%%   name space. The global actor factory do not work due to actor specific "service" definition  
-module(ambit_actor_sup).

-export([
   start_link/1
]).

start_link(Vnode) ->
   pts:start_link(ek:vnode(addr, Vnode), [
      {keylen,      inf}
     ,{supervisor,  ambit_actor_bridge_sup}
     ,{factory,     temporary}
   ]).
