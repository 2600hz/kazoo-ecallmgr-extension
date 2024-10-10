-ifndef(ECALLMGR_EXTENSION_HRL).

-include_lib("ecallmgr/src/ecallmgr.hrl").

-undef(APP).
-undef(APP_NAME).
-undef(APP_VERSION).

-define(APP, 'ecallmgr_extension').
-define(APP_NAME, <<"ecallmgr_extension">>).
-define(APP_VERSION, <<"5.0.0">>).

-define(EXT_NODE_MODULES,
        [<<"node">>
        ,<<"notify">>
        ,<<"amqp_resource_sup">>
        ,<<"amqp_fetch_dialplan_sup">>
        ]).

-define(APP_CONFIG_CAT, 'ecallmgr').
-define(CONFIG_CAT, <<"ecallmgr">>).


-define(FS_EXTENSION_EXCLUDE_EVENTS, [{'presence', ['PRESENCE_IN']}
                                     ,{'media', ['DETECTED_TONE', 'DTMF','CHANNEL_PROGRESS','CHANNEL_PROGRESS_MEDIA']}
                                     ,{'fax', ?FAX_EVENTS}
                                     ,{'cdr', ['KZ_CDR']}
                                     ,{'callflow', ['ROUTE_WINNER', 'CHANNEL_EXECUTE_COMPLETE', 'CHANNEL_APP_EXECUTE_COMPLETE', 'CHANNEL_METAFLOW']}
                                     ,{'channel', ['CHANNEL_CREATE', 'CHANNEL_ANSWER', 'CHANNEL_DESTROY']}
                                     ]).

-define(FS_EXTENSION_INCLUDE_EVENTS, [{'create', ['CHANNEL_CREATE']}
                                     ,{'answer', ['CHANNEL_ANSWER']}
                                     ,{'destroy', ['CHANNEL_DESTROY']}
                                     ,{'route', ['ROUTE_WINNER']}
                                     ,{'execute', ['CHANNEL_EXECUTE_COMPLETE']}
                                     ,{'metaflow', ['CHANNEL_METAFLOW']}
                                     ]).

-define(ECALLMGR_EXTENSION_HRL, 'true').
-endif.
