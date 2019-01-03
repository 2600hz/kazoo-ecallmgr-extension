%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_amqp).


-export([amqp_producers/0]).


-include("ecallmgr_extension.hrl").

-spec amqp_producers() -> kz_json:object().
amqp_producers() ->
    AMQP = application:get_env(?APP, 'amqp', []),
    Producers = props:get_value('producers', AMQP),
    kz_json:from_list_recursive([{<<"producers">>, Producers}]).
