-ifndef(ECALLMGR_EXTENSION_HRL).

-include_lib("ecallmgr/src/ecallmgr.hrl").
-include_lib("kazoo_amqp/include/kz_amqp.hrl").

-undef(APP).
-undef(APP_NAME).
-undef(APP_VERSION).

-define(APP, 'ecallmgr-extension').
-define(APP_NAME, <<"ecallmgr-extension">>).
-define(APP_VERSION, <<"4.0.0">>).

-define(EXT_NODE_MODULES,
        [<<"event_amqp_sup">>
        ,<<"metaflow">>
        ]).

-define(ECALLMGR_EXTENSION_HRL, 'true').
-endif.
