%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2021, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_ext_app).

-behaviour(application).

-include("ecallmgr_extension.hrl").

-export([start/2]).
-export([stop/1]).

%% Application callbacks

%% @doc Implement the application start behaviour
-spec start(application:start_type(), any()) -> kz_types:startapp_ret().
start(_StartType, _StartArgs) ->
    case ecallmgr_extension_util:authenticate(<<"application">>) of
        'false' ->
            lager:error("ecallmgr extension license is invalid or expired!"),
            {'error', 'invalid_license'};
        'true' ->
            _ = declare_exchanges(),
            _ = check_modules(),
            _ = event_stream_bind(),
            _ = fetch_handlers_bind(),
            ok = build_mod_com_kazoo_config(),
            ecallmgr_ext_sup:start_link()
    end.

%% @doc Implement the application stop behaviour
-spec stop(any()) -> any().
stop(_State) ->
    _ = fetch_handlers_unbind(),
    _ = event_stream_unbind(),
    'ok'.

-spec declare_exchanges() -> 'ok'.
declare_exchanges() ->
    _ = kapi_dialplan:declare_exchanges(),
    _ = kapi_metaflow:declare_exchanges(),
    _ = kapi_freeswitch:declare_exchanges(),
    kapi_self:declare_exchanges().


-define(COM_EVENTSTREAM_MODS, ['metaflow_bind'
                              ]).

-spec event_stream_bind() -> 'ok'.
event_stream_bind() ->
    _ = [Mod:init() || Mod <- ?COM_EVENTSTREAM_MODS],
    'ok'.

-spec event_stream_unbind() -> 'ok'.
event_stream_unbind() ->
    _ = [kazoo_bindings:flush_mod(Mod) || Mod <- ?COM_EVENTSTREAM_MODS],
    'ok'.


-define(COM_FETCH_HANDLERS_MODS, ['mod_com_kazoo_configuration'
                                 ]).

-spec fetch_handlers_bind() -> 'ok'.
fetch_handlers_bind() ->
    _ = [Mod:init() || Mod <- ?COM_FETCH_HANDLERS_MODS],
    'ok'.

-spec fetch_handlers_unbind() -> 'ok'.
fetch_handlers_unbind() ->
    _ = [kazoo_bindings:flush_mod(Mod) || Mod <- ?COM_FETCH_HANDLERS_MODS],
    'ok'.

check_modules() ->
    Key = ?NODE_MODULES_KEY(<<"commercial">>),
    case kapps_config:get_ne_binaries(?CONFIG_CAT, Key, []) of
        ?EXT_NODE_MODULES -> 'ok';
        [] -> lager:notice("commercial modules not configured, updating"),
              _ = kapps_config:set_default(?CONFIG_CAT, Key, ?EXT_NODE_MODULES),
              lager:notice("commercial modules updated");
        List when is_list(List) ->
            case List -- ?EXT_NODE_MODULES of
                [] -> 'ok';
                _Diff -> lager:notice("commercial modules configured : ~p", [_Diff]),
                         _ = kapps_config:set_default(?CONFIG_CAT, Key, ?EXT_NODE_MODULES),
                         lager:notice("commercial modules updated")
            end;
        _Other -> lager:notice("commercial modules configured : ~p", [_Other]),
                  _ = kapps_config:set_default(?CONFIG_CAT, Key, ?EXT_NODE_MODULES),
                  lager:notice("commercial modules updated")
    end.

build_mod_com_kazoo_config() ->
    try mod_com_kazoo_configuration:build_kazoo_config() of
        {'ok', _Xml} -> lager:info("kazoo xml configuration built")
    catch
        Exception:Error:ST ->
            kz_log:log_stacktrace(ST),
            {Exception, Error}
    end.
