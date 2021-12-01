%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2015-2021, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(kapi_freeswitch).

-export([bind_q/2
        ,unbind_q/2
        ]).

-export([event/1, event_v/1]).
-export([config/1, config_v/1]).
-export([api/1, api_v/1]).
-export([bgapi/1, bgapi_v/1]).
-export([json_api/1, json_api_v/1]).
-export([command/1, command_v/1]).
-export([commands/1, commands_v/1]).
-export([version/1, version_v/1]).
-export([message/1, message_v/1]).

-export([publish_event/1, publish_event/2]).
-export([publish_config/1, publish_config/2]).
-export([publish_command/1, publish_command/2]).
-export([publish_commands/1, publish_commands/2]).
-export([publish_api/1, publish_api/2]).
-export([publish_bgapi/1, publish_bgapi/2]).
-export([publish_json_api/1, publish_json_api/2]).
-export([publish_version/1, publish_version/2]).
-export([publish_message/1, publish_message/2]).

-export([publish_directory_reply/4
        ,publish_dialplan_reply/4
        ,publish_configuration_reply/4
        ,publish_channels_reply/4
        ,publish_languages_reply/4
        ,publish_reply/5
        ]).


%%-export([declare_exchanges/0, routing_key_format/0]).
-export([declare_exchanges/0]).

-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_amqp/include/kz_api.hrl").

-define(EXCHANGE_FREESWITCH, <<"freeswitch">>).
-define(TYPE_FREESWITCH, <<"topic">>).

-define(FETH_REPLY_CONTENT_TYPE, <<"text/xml">>).


%% event Request
-define(EVENT_REQ_HEADERS, [<<"Core-UUID">>, <<"Fire-Event-Name">>]).
-define(OPTIONAL_EVENT_REQ_HEADERS, [<<"Headers">>, <<"Call-ID">>, <<"Fire-Event-Subclass">>]).
-define(EVENT_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                          ,{<<"Event-Name">>, <<"sendevent">>}
                          ]).
-define(EVENT_REQ_TYPES, []).

%% configuration Request
-define(CONFIG_REQ_HEADERS, [<<"Core-UUID">>]).
-define(OPTIONAL_CONFIG_REQ_HEADERS, [<<"Section">>]).
-define(CONFIG_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                           ,{<<"Event-Name">>, <<"config">>}
                           ]).
-define(CONFIG_REQ_TYPES, []).

%% api Request
-define(API_REQ_HEADERS, [<<"Core-UUID">>, <<"API-Command">>]).
-define(OPTIONAL_API_REQ_HEADERS, [<<"API-Arguments">>]).
-define(API_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                        ,{<<"Event-Name">>, <<"api">>}
                        ]).
-define(API_REQ_TYPES, [{<<"API-Command">>, fun is_binary/1}
                       ,{<<"API-Arguments">>, fun is_binary/1}
                       ]).

%% bgapi Request
-define(BGAPI_REQ_HEADERS, [<<"Core-UUID">>, <<"API-Command">>]).
-define(OPTIONAL_BGAPI_REQ_HEADERS, [<<"API-Arguments">>]).
-define(BGAPI_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                          ,{<<"Event-Name">>, <<"bgapi">>}
                          ]).
-define(BGAPI_REQ_TYPES, [{<<"API-Command">>, fun is_binary/1}
                         ,{<<"API-Arguments">>, fun is_binary/1}
                         ]).

%% json_api Request
-define(JSON_API_REQ_HEADERS, [<<"Core-UUID">>, <<"Payload">>]).
-define(OPTIONAL_JSON_API_REQ_HEADERS, [<<"Call-ID">>]).
-define(JSON_API_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                             ,{<<"Event-Name">>, <<"json_api">>}
                             ]).
-define(JSON_API_REQ_TYPES, [{<<"Call-ID">>, fun is_binary/1}
                            ,{<<"Payload">>, fun kz_json:is_json_object/1}
                            ]).


%% command Request
-define(COMMAND_REQ_HEADERS, [<<"Core-UUID">>, <<"Call-ID">>, <<"Command">>]).
-define(OPTIONAL_COMMAND_REQ_HEADERS, [<<"Execute-Now">>]).
-define(COMMAND_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                            ,{<<"Event-Name">>, <<"command">>}
                            ]).
-define(COMMAND_REQ_TYPES, [{<<"Call-ID">>, fun is_binary/1}]).

%% commands Request
-define(COMMANDS_REQ_HEADERS, [<<"Core-UUID">>, <<"Call-ID">>, <<"Commands">>]).
-define(OPTIONAL_COMMANDS_REQ_HEADERS, [<<"Execute-Now">>]).
-define(COMMANDS_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                             ,{<<"Event-Name">>, <<"commands">>}
                             ]).
-define(COMMANDS_REQ_TYPES, [{<<"Call-ID">>, fun is_binary/1}]).

%% version Request
-define(VERSION_REQ_HEADERS, [<<"Core-UUID">>]).
-define(OPTIONAL_VERSION_REQ_HEADERS, []).
-define(VERSION_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                            ,{<<"Event-Name">>, <<"version">>}
                            ]).
-define(VERSION_REQ_TYPES, []).


%% message Request
-define(MESSAGE_REQ_HEADERS, [<<"Core-UUID">>, <<"Call-ID">>, <<"Headers">>]).
-define(OPTIONAL_MESSAGE_REQ_HEADERS, []).
-define(MESSAGE_REQ_VALUES, [{<<"Event-Category">>, <<"freeswitch">>}
                            ,{<<"Event-Name">>, <<"sendmsg">>}
                            ]).
-define(MESSAGE_REQ_TYPES, [{<<"Call-ID">>, fun is_binary/1}]).

%%------------------------------------------------------------------------------
%% @doc publish api request to freeswitch
%% @end
%%------------------------------------------------------------------------------
-spec event(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
event(Prop) when is_list(Prop) ->
    case event_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?EVENT_REQ_HEADERS, ?OPTIONAL_EVENT_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch event request"}
    end;
event(JObj) -> event(kz_json:to_proplist(JObj)).

-spec event_v(kz_term:api_terms()) -> boolean().
event_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?EVENT_REQ_HEADERS, ?EVENT_REQ_VALUES, ?EVENT_REQ_TYPES);
event_v(JObj) -> event_v(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @doc publish api request to freeswitch
%% @end
%%------------------------------------------------------------------------------
-spec api(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
api(Prop) when is_list(Prop) ->
    case api_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?API_REQ_HEADERS, ?OPTIONAL_API_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch api request"}
    end;
api(JObj) -> api(kz_json:to_proplist(JObj)).

-spec api_v(kz_term:api_terms()) -> boolean().
api_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?API_REQ_HEADERS, ?API_REQ_VALUES, ?API_REQ_TYPES);
api_v(JObj) -> api_v(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @doc publish json_api request to freeswitch
%% @end
%%------------------------------------------------------------------------------
-spec json_api(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
json_api(Prop) when is_list(Prop) ->
    case json_api_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?JSON_API_REQ_HEADERS, ?OPTIONAL_JSON_API_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch json_api request"}
    end;
json_api(JObj) -> json_api(kz_json:to_proplist(JObj)).

-spec json_api_v(kz_term:api_terms()) -> boolean().
json_api_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?JSON_API_REQ_HEADERS, ?JSON_API_REQ_VALUES, ?JSON_API_REQ_TYPES);
json_api_v(JObj) -> json_api_v(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @doc publish configuration request to freeswitch
%% @end
%%------------------------------------------------------------------------------
-spec config(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
config(Prop) when is_list(Prop) ->
    case config_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?CONFIG_REQ_HEADERS, ?OPTIONAL_CONFIG_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch configuration request"}
    end;
config(JObj) -> config(kz_json:to_proplist(JObj)).

-spec config_v(kz_term:api_terms()) -> boolean().
config_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?CONFIG_REQ_HEADERS, ?CONFIG_REQ_VALUES, ?CONFIG_REQ_TYPES);
config_v(JObj) -> config_v(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @doc publish api request to freeswitch
%% @end
%%------------------------------------------------------------------------------
-spec bgapi(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
bgapi(Prop) when is_list(Prop) ->
    case bgapi_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?BGAPI_REQ_HEADERS, ?OPTIONAL_BGAPI_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch bgapi request"}
    end;
bgapi(JObj) -> bgapi(kz_json:to_proplist(JObj)).

-spec bgapi_v(kz_term:api_terms()) -> boolean().
bgapi_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?BGAPI_REQ_HEADERS, ?BGAPI_REQ_VALUES, ?BGAPI_REQ_TYPES);
bgapi_v(JObj) -> bgapi_v(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @doc publish command request to freeswitch session
%% @end
%%------------------------------------------------------------------------------
-spec command(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
command(Prop) when is_list(Prop) ->
    case command_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?COMMAND_REQ_HEADERS, ?OPTIONAL_COMMAND_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch command request"}
    end;
command(JObj) -> command(kz_json:to_proplist(JObj)).

-spec command_v(kz_term:api_terms()) -> boolean().
command_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?COMMAND_REQ_HEADERS, ?COMMAND_REQ_VALUES, ?COMMAND_REQ_TYPES);
command_v(JObj) -> command_v(kz_json:to_proplist(JObj)).


%%------------------------------------------------------------------------------
%% @doc publish commands request to freeswitch session
%% @end
%%------------------------------------------------------------------------------
-spec commands(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
commands(Prop) when is_list(Prop) ->
    case commands_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?COMMANDS_REQ_HEADERS, ?OPTIONAL_COMMANDS_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch commands request"}
    end;
commands(JObj) -> commands(kz_json:to_proplist(JObj)).

-spec commands_v(kz_term:api_terms()) -> boolean().
commands_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?COMMANDS_REQ_HEADERS, ?COMMANDS_REQ_VALUES, ?COMMANDS_REQ_TYPES);
commands_v(JObj) -> commands_v(kz_json:to_proplist(JObj)).


%%------------------------------------------------------------------------------
%% @doc request version from freeswitch
%% @end
%%------------------------------------------------------------------------------
-spec version(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
version(Prop) when is_list(Prop) ->
    case version_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?VERSION_REQ_HEADERS, ?OPTIONAL_VERSION_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch version request"}
    end;
version(JObj) -> version(kz_json:to_proplist(JObj)).

-spec version_v(kz_term:api_terms()) -> boolean().
version_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?VERSION_REQ_HEADERS, ?VERSION_REQ_VALUES, ?VERSION_REQ_TYPES);
version_v(JObj) -> version_v(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @doc publish message request to freeswitch session
%% @end
%%------------------------------------------------------------------------------
-spec message(kz_term:api_terms()) -> {'ok', iolist()} | {'error', string()}.
message(Prop) when is_list(Prop) ->
    case message_v(Prop) of
        'true' -> kz_api:build_message(Prop, ?MESSAGE_REQ_HEADERS, ?OPTIONAL_MESSAGE_REQ_HEADERS);
        'false' -> {'error', "Proplist failed validation for freeswitch message request"}
    end;
message(JObj) -> message(kz_json:to_proplist(JObj)).

-spec message_v(kz_term:api_terms()) -> boolean().
message_v(Prop) when is_list(Prop) ->
    kz_api:validate(Prop, ?MESSAGE_REQ_HEADERS, ?MESSAGE_REQ_VALUES, ?MESSAGE_REQ_TYPES);
message_v(JObj) -> message_v(kz_json:to_proplist(JObj)).


-spec core_uuid(kz_term:api_terms()) -> binary().
core_uuid(Props) when is_list(Props) ->
    case props:get_value(<<"Core-UUID">>, Props) of
        'undefined' -> props:get_value('core_uuid', Props, <<"*">>);
        CoreUUID -> kz_term:to_binary(CoreUUID)
    end;
core_uuid(JObj) -> core_uuid(kz_json:to_proplist(JObj)).

%%------------------------------------------------------------------------------
%% @end
%%------------------------------------------------------------------------------
-spec bind_q(kz_term:ne_binary(), kz_term:proplist()) -> 'ok'.
bind_q(Queue, Props) ->
    RestrictTo = props:get_value('restrict_to', Props),
    bind_q(Queue, RestrictTo, Props).

-spec bind_q(kz_term:ne_binary(), kz_term:api_binaries(), kz_term:proplist()) -> 'ok'.
bind_q(Queue, 'undefined', _) ->
    kz_amqp_util:bind_q_to_exchange(Queue, <<"#">>, ?EXCHANGE_FREESWITCH);
bind_q(Queue, ['events'|Restrict], Props) ->
    lists:foreach(fun(RK) ->
                          kz_amqp_util:bind_q_to_exchange(Queue, RK, ?EXCHANGE_FREESWITCH)
                  end, event_routing_keys(Props)),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, ['configuration'|Restrict], Props) ->
    kz_amqp_util:bind_q_to_exchange(Queue, configuration_routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, ['dialplan'|Restrict], Props) ->
    kz_amqp_util:bind_q_to_exchange(Queue, dialplan_routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, ['directory'|Restrict], Props) ->
    kz_amqp_util:bind_q_to_exchange(Queue, directory_routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, ['channels'|Restrict], Props) ->
    kz_amqp_util:bind_q_to_exchange(Queue, channels_routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, ['languages'|Restrict], Props) ->
    kz_amqp_util:bind_q_to_exchange(Queue, languages_routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, ['fetch'|Restrict], Props) ->
    kz_amqp_util:bind_q_to_exchange(Queue, dialplan_routing_key(Props), ?EXCHANGE_FREESWITCH),
    kz_amqp_util:bind_q_to_exchange(Queue, directory_routing_key(Props), ?EXCHANGE_FREESWITCH),
    kz_amqp_util:bind_q_to_exchange(Queue, configuration_routing_key(Props), ?EXCHANGE_FREESWITCH),
    kz_amqp_util:bind_q_to_exchange(Queue, channels_routing_key(Props), ?EXCHANGE_FREESWITCH),
    kz_amqp_util:bind_q_to_exchange(Queue, languages_routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, [_|Restrict], Props) ->
    bind_q(Queue, Restrict, Props);
bind_q(_, [], _) -> 'ok'.

-spec unbind_q(kz_term:ne_binary(), kz_term:proplist()) -> 'ok'.
unbind_q(Queue, Props) ->
    RestrictTo = props:get_value('restrict_to', Props),
    unbind_q(Queue, RestrictTo, Props).

-spec unbind_q(kz_term:ne_binary(), kz_term:api_binary(), kz_term:proplist()) -> 'ok'.
unbind_q(Queue, 'undefined', _) ->
    kz_amqp_util:unbind_q_from_exchange(Queue, <<"#">>, ?EXCHANGE_FREESWITCH);
unbind_q(Queue, ['events'|Restrict], Props) ->
    lists:foreach(fun(RK) ->
                          kz_amqp_util:unbind_q_from_exchange(Queue, RK, ?EXCHANGE_FREESWITCH)
                  end
                 ,event_routing_keys(Props)
                 ),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, ['configuration'|Restrict], Props) ->
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, configuration_routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, ['dialplan'|Restrict], Props) ->
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, dialplan_routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, ['directory'|Restrict], Props) ->
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, directory_routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, ['channels'|Restrict], Props) ->
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, channels_routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, ['languages'|Restrict], Props) ->
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, languages_routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, ['fetch'|Restrict], Props) ->
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, dialplan_routing_key(Props), ?EXCHANGE_FREESWITCH),
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, directory_routing_key(Props), ?EXCHANGE_FREESWITCH),
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, configuration_routing_key(Props), ?EXCHANGE_FREESWITCH),
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, channels_routing_key(Props), ?EXCHANGE_FREESWITCH),
    _ = kz_amqp_util:unbind_q_from_exchange(Queue, languages_routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, [_|Restrict], Props) ->
    unbind_q(Queue, Restrict, Props);
unbind_q(_, [], _) -> 'ok'.

%% -spec routing_key_format() -> binary().
%% routing_key_format() -> <<"#FreeSWITCH,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Unique-ID">>.

-spec event_routing_keys(kz_term:proplist()) -> kz_term:binaries().
event_routing_keys(Props) ->
    Events = props:get_value('events', Props, [<<"*">>]),
    Profiles = case props:get_value('profile', Props) of
                   'undefined' -> props:get_value('profiles', Props, [<<"*">>]);
                   P -> [P]
               end,
    [event_routing_key([{'event', Event}, {'profile', Profile} | Props]) || Event <- Events, Profile <- Profiles].

-spec event_routing_key(kz_term:proplist()) -> binary().
event_routing_key(Props) ->
    routing_key([<<"event">>
                ,props:get_value('core_uuid', Props, <<"*">>)
                ,props:get_value('profile', Props, <<"*">>)
                ,props:get_value('event', Props, <<"*">>)
                ,props:get_value('callid', Props, <<"*">>)
                ]).

-spec directory_routing_key(kz_term:proplist()) -> binary().
directory_routing_key(Props) ->
    routing_key([<<"directory">>
                ,props:get_value('type', Props, <<"fetch">>)
                ,props:get_value('core_uuid', Props, <<"*">>)
                ,<<"*">>
                ]).

-spec directory_reply_key(kz_term:ne_binary(), kz_term:ne_binary()) -> binary().
directory_reply_key(CoreUUID, FetchID) ->
    routing_key([<<"directory">>
                ,<<"reply">>
                ,CoreUUID
                ,FetchID
                ]).

-spec configuration_routing_key(kz_term:proplist()) -> binary().
configuration_routing_key(Props) ->
    routing_key([<<"configuration">>
                ,props:get_value('type', Props, <<"fetch">>)
                ,props:get_value('core_uuid', Props, <<"*">>)
                ,<<"*">>
                ]).

-spec configuration_reply_key(kz_term:ne_binary() | atom(), kz_term:ne_binary()) -> binary().
configuration_reply_key(CoreUUID, FetchID) ->
    routing_key([<<"configuration">>
                ,<<"reply">>
                ,kz_term:to_binary(CoreUUID)
                ,FetchID
                ]).

-spec dialplan_routing_key(kz_term:proplist()) -> binary().
dialplan_routing_key(Props) ->
    routing_key([<<"dialplan">>
                ,props:get_value('type', Props, <<"fetch">>)
                ,props:get_value('core_uuid', Props, <<"*">>)
                ,<<"*">>
                ]).

-spec dialplan_reply_key(kz_term:ne_binary() | atom(), kz_term:ne_binary()) -> binary().
dialplan_reply_key(CoreUUID, FetchID) ->
    routing_key([<<"dialplan">>
                ,<<"reply">>
                ,kz_term:to_binary(CoreUUID)
                ,FetchID
                ]).

-spec channels_routing_key(kz_term:proplist()) -> binary().
channels_routing_key(Props) ->
    routing_key([<<"channels">>
                ,props:get_value('type', Props, <<"fetch">>)
                ,props:get_value('core_uuid', Props, <<"*">>)
                ,<<"*">>
                ]).

-spec channels_reply_key(kz_term:ne_binary() | atom(), kz_term:ne_binary()) -> binary().
channels_reply_key(CoreUUID, FetchID) ->
    routing_key([<<"channels">>
                ,<<"reply">>
                ,kz_term:to_binary(CoreUUID)
                ,FetchID
                ]).

-spec languages_routing_key(kz_term:proplist()) -> binary().
languages_routing_key(Props) ->
    routing_key([<<"languages">>
                ,props:get_value('type', Props, <<"fetch">>)
                ,props:get_value('core_uuid', Props, <<"*">>)
                ,<<"*">>
                ]).

-spec languages_reply_key(kz_term:ne_binary() | atom(), kz_term:ne_binary()) -> binary().
languages_reply_key(CoreUUID, FetchID) ->
    routing_key([<<"languages">>
                ,<<"reply">>
                ,kz_term:to_binary(CoreUUID)
                ,FetchID
                ]).

-spec routing_key(kz_term:ne_binaries()) -> kz_term:ne_binary().
routing_key([<<"freeswitch">> | _] = Parts) ->
    kz_binary:join([kz_amqp_util:encode(kz_term:to_binary(Part)) || Part <- Parts], <<".">>);
routing_key(Parts) ->
    routing_key([<<"freeswitch">> | Parts]).

%%------------------------------------------------------------------------------
%% @doc declare the exchanges used by this API
%% @end
%%------------------------------------------------------------------------------
-spec declare_exchanges() -> 'ok'.
declare_exchanges() ->
    kz_amqp_util:new_exchange(?EXCHANGE_FREESWITCH, ?TYPE_FREESWITCH).


-spec publish_directory_reply(kz_term:ne_binary() | atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
publish_directory_reply(_CoreUUID, FetchID, Xml, ServerId)
  when ServerId =/= 'undefined' ->
    Prop = [{'message_id', FetchID}],
    kz_amqp_util:targeted_publish(ServerId, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop);
publish_directory_reply(CoreUUID, FetchID, Xml, _) ->
    Prop = [{'message_id', FetchID}],
    RK = directory_reply_key(CoreUUID, FetchID),
    Exchange = ?EXCHANGE_FREESWITCH,
    kz_amqp_util:basic_publish(Exchange, RK, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop).

-spec publish_dialplan_reply(kz_term:ne_binary() | atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
publish_dialplan_reply(_CoreUUID, FetchID, Xml, ServerId)
  when ServerId =/= 'undefined' ->
    Prop = [{'message_id', FetchID}],
    kz_amqp_util:targeted_publish(ServerId, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop);
publish_dialplan_reply(CoreUUID, FetchID, Xml, _) ->
    Prop = [{'message_id', FetchID}],
    RK = dialplan_reply_key(CoreUUID, FetchID),
    Exchange = ?EXCHANGE_FREESWITCH,
    kz_amqp_util:basic_publish(Exchange, RK, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop).

-spec publish_configuration_reply(kz_term:ne_binary() | atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
publish_configuration_reply(_CoreUUID, FetchID, Xml, ServerId)
  when ServerId =/= 'undefined' ->
    Prop = [{'message_id', FetchID}],
    kz_amqp_util:targeted_publish(ServerId, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop);
publish_configuration_reply(CoreUUID, FetchID, Xml, _) ->
    Prop = [{'message_id', FetchID}],
    RK = configuration_reply_key(CoreUUID, FetchID),
    Exchange = ?EXCHANGE_FREESWITCH,
    kz_amqp_util:basic_publish(Exchange, RK, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop).

-spec publish_channels_reply(kz_term:ne_binary() | atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
publish_channels_reply(_CoreUUID, FetchID, Xml, ServerId)
  when ServerId =/= 'undefined' ->
    Prop = [{'message_id', FetchID}],
    kz_amqp_util:targeted_publish(ServerId, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop);
publish_channels_reply(CoreUUID, FetchID, Xml, _) ->
    Prop = [{'message_id', FetchID}],
    RK = channels_reply_key(CoreUUID, FetchID),
    Exchange = ?EXCHANGE_FREESWITCH,
    kz_amqp_util:basic_publish(Exchange, RK, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop).

-spec publish_languages_reply(kz_term:ne_binary() | atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
publish_languages_reply(_CoreUUID, FetchID, Xml, ServerId)
  when ServerId =/= 'undefined' ->
    Prop = [{'message_id', FetchID}],
    kz_amqp_util:targeted_publish(ServerId, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop);
publish_languages_reply(CoreUUID, FetchID, Xml, _) ->
    Prop = [{'message_id', FetchID}],
    RK = languages_reply_key(CoreUUID, FetchID),
    Exchange = ?EXCHANGE_FREESWITCH,
    kz_amqp_util:basic_publish(Exchange, RK, Xml, ?FETH_REPLY_CONTENT_TYPE, Prop).

-spec publish_reply(kz_term:ne_binary() | atom(), kz_term:ne_binary(), atom(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
publish_reply(CoreUUID, FetchID, 'configuration', Xml, ServerId) ->
    publish_configuration_reply(CoreUUID, FetchID, Xml, ServerId);
publish_reply(CoreUUID, FetchID, 'dialplan', Xml, ServerId) ->
    publish_dialplan_reply(CoreUUID, FetchID, Xml, ServerId);
publish_reply(CoreUUID, FetchID, 'directory', Xml, ServerId) ->
    publish_directory_reply(CoreUUID, FetchID, Xml, ServerId);
publish_reply(CoreUUID, FetchID, 'channels', Xml, ServerId) ->
    publish_channels_reply(CoreUUID, FetchID, Xml, ServerId);
publish_reply(CoreUUID, FetchID, 'languages', Xml, ServerId) ->
    publish_languages_reply(CoreUUID, FetchID, Xml, ServerId);
publish_reply(CoreUUID, FetchID, Section, Xml, _) ->
    lager:error("publish reply not handled : ~p, ~p, ~p, ~p", [CoreUUID, FetchID, Section, Xml]).

-spec publish_config(kz_term:api_terms()) -> 'ok'.
publish_config(JObj) ->
    publish_config(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_config(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_config(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?CONFIG_REQ_VALUES, fun config/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_command(kz_term:api_terms()) -> 'ok'.
publish_command(JObj) ->
    publish_command(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_command(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_command(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?COMMAND_REQ_VALUES, fun command/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_commands(kz_term:api_terms()) -> 'ok'.
publish_commands(JObj) ->
    publish_commands(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_commands(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_commands(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?COMMANDS_REQ_VALUES, fun commands/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_api(kz_term:api_terms()) -> 'ok'.
publish_api(JObj) ->
    publish_api(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_api(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_api(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?API_REQ_VALUES, fun api/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_json_api(kz_term:api_terms()) -> 'ok'.
publish_json_api(JObj) ->
    publish_json_api(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_json_api(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_json_api(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?JSON_API_REQ_VALUES, fun json_api/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_event(kz_term:api_terms()) -> 'ok'.
publish_event(JObj) ->
    publish_event(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_event(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_event(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?EVENT_REQ_VALUES, fun event/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_bgapi(kz_term:api_terms()) -> 'ok'.
publish_bgapi(JObj) ->
    publish_bgapi(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_bgapi(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_bgapi(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?BGAPI_REQ_VALUES, fun bgapi/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_version(kz_term:api_terms()) -> 'ok'.
publish_version(JObj) ->
    publish_version(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_version(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_version(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?VERSION_REQ_VALUES, fun version/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).

-spec publish_message(kz_term:api_terms()) -> 'ok'.
publish_message(JObj) ->
    publish_message(JObj, ?DEFAULT_CONTENT_TYPE).

-spec publish_message(kz_term:api_terms(), kz_term:ne_binary()) -> 'ok'.
publish_message(Req, ContentType) ->
    {'ok', Payload} = kz_api:prepare_api_payload(Req, ?MESSAGE_REQ_VALUES, fun message/1),
    RK = routing_key([<<"commands">>, core_uuid(Req)]),
    kz_amqp_util:basic_publish(?EXCHANGE_FREESWITCH, RK, Payload, ContentType).
