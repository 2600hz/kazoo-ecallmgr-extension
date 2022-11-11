%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2022-, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo).

-export([version/1
        ,version/2
        ]).
-export([noevents/1]).
-export([close/1]).
-export([getpid/1
        ,getpid/2
        ]).
-export([bind/2
        ,bind/3
        ]).
-export([fetch_reply/1]).
-export([api/2
        ,api/3
        ,api/4
        ]).
-export([bgapi/3
        ,bgapi/4
        ,bgapi/5
        ,bgapi/6
        ]).
-export([json_api/2
        ,json_api/3
        ,json_api/4
        ,json_api/5
        ]).
-export([event/2
        ,event/3
        ]).
-export([nixevent/2]).
-export([sendevent/3
        ,sendevent_custom/3
        ]).
-export([sendmsg/3, sendmsgs/3]).

-export([cmd/3, cmds/3]).
-export([cast_cmd/3, cast_cmds/3]).

-export([config/1
        ,bgapi4/5
        ]).

-export([sync_channel/2]).

-export([core_uuid/1]).
-export([no_legacy/1]).

-export([async_api/3]).

-export([contact_api/0]).

-include("ecallmgr_extension.hrl").

-define(TIMEOUT, 5 * ?MILLISECONDS_IN_SECOND).
-define(API_TIMEOUT, 5 * ?MILLISECONDS_IN_SECOND).

-type fs_not_implemented() :: {'error', 'not_implemented'}.
-type fs_api_ok() :: 'ok' | {'ok', binary()}.
-type fs_api_error():: {'error', 'timeout' | 'exception' | 'not_implemented' | binary()}.
-type fs_api_return() :: fs_api_ok() | fs_api_error().
-export_type([fs_api_ok/0
             ,fs_api_error/0
             ,fs_api_return/0
             ]).

-spec version(atom()) -> fs_api_return().
version(Node) ->
    version(Node, ?TIMEOUT).

-spec version(atom(), pos_integer()) -> fs_api_return().
version(Node, Timeout) ->
    send(Node, [], fun kapi_freeswitch:publish_version/1, Timeout).

-spec noevents(atom()) -> fs_api_return().
noevents(_Node) -> 'ok'.

-spec close(atom()) -> 'ok'.
close(_Node) -> 'ok'.

-spec getpid(atom()) -> fs_api_return().
getpid(Node) ->
    getpid(Node, ?TIMEOUT).

-spec getpid(atom(), pos_integer()) -> fs_not_implemented().
getpid(_Node, _Timeout) ->
    {'error', 'not_implemented'}.

-spec bind(atom(), atom()) -> fs_not_implemented().
bind(Node, Type) ->
    bind(Node, Type, ?TIMEOUT).

-spec bind(atom(), atom(), pos_integer()) -> fs_not_implemented().
bind(_Node, _Type, _Timeout) ->
    {'error', 'not_implemented'}.

-spec fetch_reply(map()) -> 'ok'.
fetch_reply(#{node := Node
             ,fetch_id := FetchId
             ,section := Section
             ,reply := Reply
             ,server_id := ServerId
             }) ->
    kapi_freeswitch:publish_reply(core_uuid(Node), FetchId, Section, Reply, ServerId).

-spec api(atom(), kz_term:ne_binary()) -> fs_api_return().
api(Node, Cmd) ->
    api(Node, Cmd, 'undefined').

-spec api(atom(), kz_term:ne_binary(), kz_term:api_ne_binary() | string()) -> fs_api_return().
api(Node, Cmd, Args) ->
    api(Node, Cmd, Args, ?TIMEOUT).

-spec api(atom(), kz_term:ne_binary(), kz_term:api_ne_binary() | string(), timeout()) -> fs_api_return().
api(Node, Cmd, Args, Timeout)
  when not is_binary(Cmd) ->
    api(Node, kz_term:to_binary(Cmd), Args, Timeout);
api(Node, Cmd, Args, Timeout)
  when is_list(Args) ->
    api(Node, Cmd, kz_term:to_binary(Args), Timeout);
api(Node, Cmd, Args, Timeout)
  when is_atom(Node) ->
    Payload = props:filter_undefined(
                [{<<"API-Arguments">>, Args}
                ,{<<"API-Command">>, Cmd}
                ]),
    send(Node, Payload, fun kapi_freeswitch:publish_api/1, Timeout).

%% @doc Make a backgrounded API call to FreeSWITCH. The asynchronous reply is
%% sent to calling process after it is received. This function
%% returns the result of the initial bgapi call or `timeout' if FreeSWITCH fails
%% to respond.
-spec bgapi(atom(), atom(), string() | binary()) -> fs_api_return().
bgapi(Node, Cmd, Args) ->
    bgapi4(Node, Cmd, Args, 'undefined', []).

-spec bgapi(atom(), atom(), string() | binary(), fun()) -> fs_api_return().
bgapi(Node, Cmd, Args, Fun) when is_function(Fun, 2) ->
    bgapi4(Node, Cmd, Args, Fun, []).

-spec bgapi(atom(), atom(), string() | binary(), fun(), list()) -> fs_api_return().
bgapi(Node, Cmd, Args, Fun, CallBackParams) when is_function(Fun, 3) ->
    bgapi4(Node, Cmd, Args, Fun, CallBackParams).

-spec bgapi(atom(), kz_term:ne_binary(), list(), atom(), string() | binary(), fun()) -> fs_api_return().
bgapi(Node, UUID, CallBackParams, Cmd, Args, Fun) when is_function(Fun, 6) ->
    Fun4 = fun(Code, Reply, _Data, [JobId | _CallBackParams]) ->
                   Fun(Code, Node, UUID, CallBackParams, JobId, Reply)
           end,
    bgapi4(Node, Cmd, Args, Fun4, CallBackParams).


-spec json_api(atom(), kz_term:ne_binary() | {kz_term:ne_binary(), kz_json:object()}) -> freeswitch:fs_json_api_return().
json_api(Node, {Cmd, Args}) ->
    json_api(Node, 'undefined', Cmd, Args, ?TIMEOUT);
json_api(Node, Cmd) ->
    json_api(Node, 'undefined', Cmd, 'undefined', ?TIMEOUT).

-spec json_api(atom(), kz_term:api_ne_binary(), kz_term:ne_binary()) -> freeswitch:fs_json_api_return().
json_api(Node, UUID, Cmd) ->
    json_api(Node, UUID, Cmd, 'undefined', ?TIMEOUT).

-spec json_api(atom(), kz_term:api_ne_binary(), kz_term:ne_binary(), kz_term:api_object()) -> freeswitch:fs_json_api_return().
json_api(Node, UUID, Cmd, Args) ->
    json_api(Node, UUID, Cmd, Args, ?TIMEOUT).

-spec json_api(atom(), kz_term:api_ne_binary(), kz_term:ne_binary(), kz_term:api_object() | <<>>, timeout()) -> freeswitch:fs_json_api_return().
json_api(Node, UUID, Cmd, 'undefined', Timeout) ->
    json_api(Node, UUID, Cmd, <<>>, Timeout);
json_api(Node, UUID, Cmd, Data, Timeout) when is_atom(Node) ->
    Params = [{<<"command">>, Cmd}
             ,{<<"data">>, Data}
             ],
    JObj = kz_json:from_list(Params),
    Payload = props:filter_undefined(
                [{<<"Call-ID">>, UUID}
                ,{<<"Payload">>, JObj}
                ]),
    send(Node, Payload, fun kapi_freeswitch:publish_json_api/1, Timeout).

-type event() :: kz_json:object().

-spec event(atom(), event() | [event()]) -> fs_not_implemented().
event(Node, Events) ->
    event(Node, Events, ?TIMEOUT).

-spec event(atom(), event() | [event()], pos_integer()) -> fs_not_implemented().
event(_Node, [_|_]=_Events, _Timeout) ->
    {'error', 'not_implemented'};
event(_Node, _Event, _Timeout) ->
    {'error', 'not_implemented'}.

-spec nixevent(atom(), event() | [event()]) -> fs_not_implemented().
nixevent(_Node, [_|_]=_Events) ->
    {'error', 'not_implemented'};
nixevent(_Node, _Event) ->
    {'error', 'not_implemented'}.

-spec sendevent(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
sendevent(Node, EventName, Headers) ->
    Payload = [{<<"Fire-Event-Name">>, kz_term:to_binary(EventName)}
              ,{<<"Headers">>, kz_json:from_list(Headers)}
              ],
    send(Node, Payload, fun kapi_freeswitch:publish_event/1).

-spec sendevent_custom(atom(), atom(), list()) -> fs_api_return().
sendevent_custom(Node, SubClassName, Headers) ->
    Payload = [{<<"Fire-Event-Name">>, <<"CUSTOM">>}
              ,{<<"Fire-Event-Subclass">>, kz_term:to_binary(SubClassName)}
              ,{<<"Headers">>, kz_json:from_list(Headers)}
              ],
    send(Node, Payload, fun kapi_freeswitch:publish_event/1).

-spec sync_channel(atom(), kz_term:ne_binary()) -> 'ok'.
sync_channel(Node, UUID) ->
    Headers = [{<<"Call-ID">>, UUID}
              ,{<<"Event-PID">>, kz_term:to_binary(self())}
              ,{<<"Event-Node">>, kz_term:to_binary(node())}
              ],
    Payload = [{<<"Call-ID">>, UUID}
              ,{<<"Fire-Event-Name">>, <<"CUSTOM">>}
              ,{<<"Fire-Event-Subclass">>, <<"CHANNEL_SYNC">>}
              ,{<<"Headers">>, kz_json:from_list(Headers)}
              ],
    send(Node, Payload, fun kapi_freeswitch:publish_event/1).

-spec sendmsg(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
sendmsg(Node, UUID, Headers) ->
    Cmd = [{kz_term:to_binary(K), kz_term:to_binary(V)}|| {K, V} <- Headers],
    Payload = [{<<"Call-ID">>, UUID}
              ,{<<"Headers">>, kz_json:from_list(Cmd)}
              ],
    send(Node, Payload, fun kapi_freeswitch:publish_message/1).

-spec sendmsgs(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
sendmsgs(Node, UUID, API) ->
    cmds(Node, UUID, API).

-spec cmd(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
cmd(Node, UUID, Command) ->
    cmd(Node, UUID, call_cmd_sync(), Command).

-spec cmd(atom(), kz_term:ne_binary(), boolean(), list()) -> fs_api_return().
cmd(Node, UUID, Sync, Command) ->
    Cmd = [{kz_term:to_binary(K), kz_term:to_binary(V)}|| {K, V} <- Command],
    Payload = [{<<"Call-ID">>, UUID}
              ,{<<"Command">>, kz_json:from_list(Cmd)}
              ,{<<"Execute-Now">>, Sync}
              ],
    send(Node, props:filter_undefined(Payload), fun kapi_freeswitch:publish_command/1).

-spec cmds(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
cmds(Node, UUID, Commands) ->
    cmds(Node, UUID, call_cmd_sync(), Commands).

-spec cmds(atom(), kz_term:ne_binary(), boolean(), list()) -> fs_api_return().
cmds(Node, UUID, Sync, Commands) ->
    Payload = [{<<"Call-ID">>, UUID}
              ,{<<"Commands">>, kz_json:from_list_recursive(Commands)}
              ,{<<"Execute-Now">>, Sync}
              ],
    send(Node, props:filter_undefined(Payload), fun kapi_freeswitch:publish_commands/1).

-spec cast_cmd(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
cast_cmd(Node, UUID, Command) ->
    cast_cmd(Node, UUID, call_cmd_sync(), Command).

-spec cast_cmd(atom(), kz_term:ne_binary(), boolean(), list()) -> fs_api_return().
cast_cmd(Node, UUID, Sync, Command) ->
    Payload = [{<<"Call-ID">>, UUID}
              ,{<<"Command">>, kz_json:from_list(Command)}
              ,{<<"Execute-Now">>, Sync}
              ],
    kapi_freeswitch:publish_command(payload(Node, Payload)).

-spec cast_cmds(atom(), kz_term:ne_binary(), list()) -> fs_api_return().
cast_cmds(Node, UUID, Commands) ->
    cast_cmds(Node, UUID, call_cmd_sync(), Commands).

-spec cast_cmds(atom(), kz_term:ne_binary(), boolean(), list()) -> fs_api_return().
cast_cmds(Node, UUID, Sync, Commands) ->
    Payload = [{<<"Call-ID">>, UUID}
              ,{<<"Commands">>, kz_json:from_list_recursive(Commands)}
              ,{<<"Execute-Now">>, Sync}
              ],
    kapi_freeswitch:publish_commands(payload(Node, Payload)).

-spec config(atom()) -> 'ok'.
config(Node) ->
    kapi_freeswitch:publish_config(payload(Node)).

-spec bgapi4(atom(), atom(), string() | binary(), fun() | 'undefined', list()) ->
          {'ok', binary()} |
          {'error', 'timeout' | 'exception' | binary()}.
bgapi4(Node, Cmd, Args, Fun, CallBackParams) ->
    Self = self(),
    _ = kz_process:spawn(fun bgapi4/6, [Node, Cmd, Args, Fun, CallBackParams, Self]),
    receive
        {'api', Result} -> Result
    end.

bgapi4(Node, Cmd, Args, Fun, CallBackParams, Self) ->
    Payload = props:filter_undefined(
                [{<<"API-Command">>, kz_term:to_binary(Cmd)}
                ,{<<"API-Arguments">>, kz_term:to_binary(Args)}
                ]),
    case send(Node, Payload, fun kapi_freeswitch:publish_bgapi/1) of
        {'ok', JobId} ->
            Self ! {'api', {'ok', JobId}},
            receive
                {'switch_job_reply', {'bgok', JobId, Reply, _}}
                  when is_function(Fun, 2) -> Fun('ok', Reply);
                {'switch_job_reply', {'bgerror', JobId, Reply, _}}
                  when is_function(Fun, 2) -> Fun('error', Reply);
                {'switch_job_reply', {'bgok', JobId, Reply, _}}
                  when is_function(Fun, 3) -> Fun('ok', Reply, [JobId | CallBackParams]);
                {'switch_job_reply', {'bgerror', JobId, Reply, _}}
                  when is_function(Fun, 3) -> Fun('error', Reply, [JobId | CallBackParams]);
                {'switch_job_reply', {'bgok', JobId, Reply, Data}}
                  when is_function(Fun, 4) -> Fun('ok', Reply, Data, [JobId | CallBackParams]);
                {'switch_job_reply', {'bgerror', JobId, Reply, Data}}
                  when is_function(Fun, 4) -> Fun('error', Reply, Data, [JobId | CallBackParams]);
                {'switch_job_reply', {Code, JobId, _}} -> Self ! {Code, JobId};
                {'switch_job_reply', {Code, JobId, Reply, _}} -> Self ! {Code, JobId, Reply}
            end;
        {'error', _} = Error -> Self ! {'api', Error}
    end.

send(Node, Payload, PublishFun) ->
    send(Node, Payload, PublishFun, ?API_TIMEOUT).

send(Node, Payload, PublishFun, Timeout) ->
    Result = mod_com_kazoo_api:send(payload(Node, Payload), PublishFun),
    send_wait(Result, Timeout).

send_wait({'error', _} = Error, _Timeout) -> Error;
send_wait(MsgId, Timeout) ->
    receive
        {'switch_reply', MsgId, Msg} -> Msg
    after Timeout ->
            {'error', 'timeout'}
    end.

-spec core_uuid(atom()) -> atom().
core_uuid(X)
  when is_atom(X) -> X;
core_uuid(_) -> 'undefined'.

-spec no_legacy(atom()) -> 'ok'.
no_legacy(_Node) -> 'ok'.

-spec payload(atom()) -> kz_term:api_terms().
payload(Node) ->
    payload(Node, []).

-spec payload(atom(), kz_term:api_terms()) -> kz_term:api_terms().
payload(Node, Headers) ->
    Headers ++ payload_headers(Node).

payload_headers(Node) ->
    [{<<"Core-UUID">>, kz_term:to_binary(core_uuid(Node))}
    ,{?KEY_REQUEST_FROM_PID, kz_term:to_binary(self())}
    | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
    ].

%%------------------------------------------------------------------------------
%% @doc Make a background API call to FreeSWITCH and wait for reply.
%% @end
%%------------------------------------------------------------------------------
-spec async_api(atom(), atom(), string() | binary()) -> freeswitch:fs_api_return().
async_api(Node, Cmd, Args) ->
    case bgapi(Node, Cmd, Args) of
        {'error', _} = Error -> Error;
        {'ok', JobId} ->
            receive
                {'bgok', JobId, Result} -> {'ok', Result};
                {'bgok', JobId} -> 'ok';
                {'bgerror', JobId, Error} -> {'error', Error};
                {'bgerror', JobId} -> {'error', <<"unspecified reason">>}
            end
    end.

-spec contact_api() -> kz_term:ne_binary().
contact_api() ->
    case kz_app_config:get_boolean(?APP_CONFIG_CAT, <<"use_proxy_contact_api">>, 'false') of
        'true' -> <<"kz_proxy_contact">>;
        'false' -> <<"kz_contact">>
    end.

call_cmd_sync() -> freeswitch:call_cmd_sync().
