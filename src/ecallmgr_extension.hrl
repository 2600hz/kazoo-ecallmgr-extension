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
        ,<<"resource">>
        ]).

-define(APP_CONFIG_CAT, 'ecallmgr').
-define(CONFIG_CAT, <<"ecallmgr">>).


-define(FS_EXTENSION_EXCLUDE_EVENTS, [{'presence', ['PRESENCE_IN']}
                                     ,{'media', ['DETECTED_TONE', 'DTMF','CHANNEL_PROGRESS','CHANNEL_PROGRESS_MEDIA']}
                                     ,{'fax', ?FAX_EVENTS}
                                     ,{'cdr', ['KZ_CDR']}
                                     ,{'record', ['RECORD_START', 'RECORD_STOP']}
                                     ]).

-define(FS_EXTENSION_INCLUDE_EVENTS, [{'record', ['RECORD_STOP']}
                                     ]).

-define(ECALLMGR_EXTENSION_HRL, 'true').
-endif.
