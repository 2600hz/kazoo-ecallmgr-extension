-ifndef(METAFLOW_HRL).

-include("ecallmgr-extension.hrl").

-define(METAFLOW_TARGET_VAR, <<"metaflow_bind_target">>).

-define(METAFLOW_REG_MSG(UUID), {'metaflow', UUID}).


-define(METAFLOW, 'true').
-endif.
