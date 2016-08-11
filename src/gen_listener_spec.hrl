-ifndef(gen_listener_spec_hrl).
-define(gen_listener_spec_hrl, included).

-include("gen_server_spec.hrl").

-type gen_listener_handle_event_result() :: 'ignore' |
                            {'ignore', gen_srv_state()} |
                            {'reply', list()} |
                            {'reply', list(), gen_srv_state()}.

-spec handle_event(any(), gen_srv_state()) -> gen_listener_handle_event_result().

-endif.
