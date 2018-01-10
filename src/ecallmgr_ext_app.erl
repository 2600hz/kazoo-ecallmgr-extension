%%%-------------------------------------------------------------------
%%% @copyright (C) 2012-2016, 2600Hz
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_ext_app).

-behaviour(application).

-include("ecallmgr_extension.hrl").

-export([start/2]).
-export([stop/1]).

-export([freeswitch_node_modules/0]).

%% Application callbacks

%% @public
%% @doc Implement the application start behaviour
-spec start(application:start_type(), any()) -> kz_types:startapp_ret().
start(_StartType, _StartArgs) ->
    _ = declare_exchanges(),
    _ = freeswitch_nodesup_bind(),
    ecallmgr_ext_sup:start_link().

%% @public
%% @doc Implement the application stop behaviour
-spec stop(any()) -> any().
stop(_State) ->
    _ = freeswitch_nodesup_unbind(),
    'ok'.


-spec declare_exchanges() -> 'ok'.
declare_exchanges() ->
    _ = kapi_dialplan:declare_exchanges(),
    _ = kapi_metaflow:declare_exchanges(),
    _ = kapi_freeswitch:declare_exchanges(),
    kapi_self:declare_exchanges().

-spec freeswitch_nodesup_bind() -> 'ok'.
freeswitch_nodesup_bind() ->
    _ = kazoo_bindings:bind(<<"freeswitch.node.modules">>, ?MODULE, 'freeswitch_node_modules'),
    _ = kazoo_bindings:bind(<<"freeswitch.config.amqp.conf">>, 'ecallmgr_fs_amqp_config', 'handle_config_req'),
    'ok'.

-spec freeswitch_nodesup_unbind() -> 'ok'.
freeswitch_nodesup_unbind() ->
    _ = kazoo_bindings:unbind(<<"freeswitch.node.modules">>, ?MODULE, 'freeswitch_node_modules'),
    _ = kazoo_bindings:unbind(<<"freeswitch.config.amqp.conf">>, 'ecallmgr_fs_amqp_config', 'handle_config_req'),
    'ok'.

-spec freeswitch_node_modules() -> kz_term:ne_binaries().
freeswitch_node_modules() ->
    application:get_env(?APP, 'node_modules', ?EXT_NODE_MODULES).
