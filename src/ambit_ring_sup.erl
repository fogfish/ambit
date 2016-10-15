-module(ambit_ring_sup).

-export([
   start_link/0
]).

start_link() ->
   pts:start_link(vnode, [
      {keylen,      inf}
     ,{supervisor,  ambit_vnode_sup}
     ,{factory,     transient}
   ]).
