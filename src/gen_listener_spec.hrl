-ifndef(gen_listener_spec_hrl).
-define(gen_listener_spec_hrl, included).

-include("gen_server_spec.hrl").

-spec handle_event(any(), gen_srv_state()) -> gen_listener:handle_event_return().

-endif.
