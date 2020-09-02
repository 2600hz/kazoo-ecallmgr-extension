%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2020, 2600Hz
%%% @doc Handle BRIDGE events and request metaflow bind
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_bind).

-export([init/0]).

-export([handle_bridge/1]).
-export([handle_metaflow/1]).

-include("metaflow.hrl").

-spec init() -> 'ok'.
init() ->
    _ = kazoo_bindings:bind(<<"event_stream.process.call_event.CHANNEL_BRIDGE">>, ?MODULE, 'handle_bridge'),
    _ = kazoo_bindings:bind(<<"event_stream.process.call_event.CHANNEL_METAFLOW">>, ?MODULE, 'handle_metaflow'),
    'ok'.

-spec handle_bridge(map()) -> any().
handle_bridge(#{payload := JObj}=Map) ->
    ThisNode = kz_term:to_binary(node()),
    ControlNode = kz_json:get_ne_binary_value([<<"Call-Control">>, <<"Node">>], JObj, ThisNode),
    maybe_request_metaflow(ThisNode, ControlNode, Map).

maybe_request_metaflow(ThisNode, ThisNode, #{node := Node, payload := JObj}) ->
    lager:debug("handling request metaflow"),
    ALeg = kz_json:get_value(<<"Bridge-A-Unique-ID">>, JObj),
    BLeg = kz_json:get_value(<<"Bridge-B-Unique-ID">>, JObj),
    AChannel = ecallmgr_fs_channel:fetch(ALeg, 'record'),
    kz_process:spawn(fun request_metaflow/3, [Node, ALeg, AChannel]),
    BChannel = ecallmgr_fs_channel:fetch(BLeg, 'record'),
    kz_process:spawn(fun request_metaflow/3, [Node, BLeg, BChannel]);
maybe_request_metaflow(ThisNode, OtherNode, _Map) ->
    lager:debug("not handling request metaflow ~s / ~s", [ThisNode, OtherNode]).

-spec request_metaflow(atom(), kz_term:ne_binary(), channel()) -> any().
request_metaflow(Node, _UUID, {'ok', #channel{handling_locally='true'
                                             ,is_loopback='false'
                                             ,account_id=?NE_BINARY=AccountId
                                             ,uuid=UUID
                                             ,authorizing_id=AuthorizingId
                                             ,resource_id=ResourceId
                                             ,callflow_id=CallFlowId
                                             ,node=Node
                                             }}) ->
    kz_log:put_callid(UUID),
    API = [{<<"Account-ID">>, AccountId}
          ,{<<"Call-ID">>, UUID}
          ,{<<"Authorizing-ID">>, AuthorizingId}
          ,{<<"Resource-ID">>, ResourceId}
          ,{<<"CallFlow-ID">>, CallFlowId}
          | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
          ],
    case kz_amqp_worker:call(API, fun kapi_metaflow:publish_bind_req/1, fun kapi_metaflow:binding_v/1) of
        {'ok', JObj} -> freeswitch:json_api(Node, UUID, <<"kz.meta.bind">>, JObj);
        {'error', 'timeout'} -> lager:debug("no metaflow available");
        _Else -> lager:debug("error requesting metaflow binding : ~p", [_Else])
    end;
request_metaflow(_Node, _UUID, _Channel) -> lager:debug("channel not found : ~s", [_UUID]).


-spec handle_metaflow(map()) -> any().
handle_metaflow(Map) ->
    Routines = [fun send_request/1
               ,fun maybe_send_route_win/1
               ],
    kz_maps:exec(Routines, Map#{fetch_id => kz_binary:rand_hex(16)}).

-spec send_request(map()) -> any().
send_request(#{fetch_id := FetchId, payload := Payload}=Map) ->
    CRProps = [{<<"Metaflow-Request-Type">>, <<"in-call">>}
              ,{<<"Other-Leg-Call-ID">>, kz_json:get_binary_value(<<"Other-Leg-Call-ID">>, Payload)}
              ,{<<"Metaflow-Request">>, kz_json:get_binary_value(<<"Metaflow-Collected-Digits">>, Payload)}
              ],
    Props = [{<<"Event-Category">>, <<"dialplan">>}
            ,{<<"Event-Name">>, <<"route_req">>}
            ,{<<"Context">>, <<"metaflow">>}
            ,{<<"Resource-Type">>, <<"metaflow">>}
            ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRProps)}
            ,{<<"Msg-ID">>, FetchId}
            ,{[<<"Custom-Channel-Vars">>, <<"Fetch-ID">>], FetchId}
            | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
            ],
    Request = kz_json:set_values(Props, Payload),
    ReqResp = kz_amqp_worker:call(Request
                                 ,fun kapi_route:publish_req/1
                                 ,fun kapi_route:is_actionable_resp/1
                                 ,ecallmgr_fs_node:fetch_timeout()
                                 ),
    case ReqResp of
        {'error', R} ->
            lager:info("did not receive route response for metaflow ~s: ~p", [FetchId, R]),
            Map#{metaflow => #{error => R}};
        {'ok', JObj} ->
            lager:debug_unsafe("METAFLOW REPLY ~s", [kz_json:encode(JObj, ['pretty'])]),
            'true' = kapi_route:resp_v(JObj),
            ControllerQ = kz_api:server_id(JObj),
            Map#{controller_q => ControllerQ, metaflow => #{payload => JObj}}
    end.

-spec maybe_send_route_win(map()) -> map().
maybe_send_route_win(#{metaflow := #{payload := Reply}}=Map) ->
    case kz_json:get_ne_binary_value(<<"Method">>, Reply) =:= <<"application">> of
        'true' -> send_metaflow_win(Map);
        'false' -> Map
    end.

-spec send_metaflow_win(map()) -> map().
send_metaflow_win(#{fetch_id := FetchId, call_id := CallId, payload := _Payload, controller_q := ControllerQ}=Map) ->
    ControlQ = gen_listener:queue_name('metaflow_listener'),
    Pid = kz_process:spawn(fun metaflow_receiver/1, [Map]),
    Win = [{<<"Msg-ID">>, FetchId}
          ,{<<"Call-ID">>, CallId}
          ,{<<"Control-Queue">>, list_to_binary(["pid://", kz_term:to_binary(Pid), "/", ControlQ])}
                                                %          ,{<<"Control-PID">>, ControlP}
          | kz_api:default_headers(ControlQ, <<"dialplan">>, <<"route_win">>, ?APP_NAME, ?APP_VERSION)
          ],
    lager:debug("sending metaflow route_win to ~s", [ControllerQ]),
    Publisher = fun(API) -> kapi_route:publish_win(ControllerQ, API) end,
    _ = kz_amqp_worker:cast(Win, Publisher),
    Map#{receiver => Pid}.

metaflow_receiver(#{node := Node} = Map) ->
    receive
        {'kapi', {{<<"callctl">>, _, _}, _, JObj}} ->
            metaflow_control:exec_payload(Node, JObj),
            metaflow_receiver(Map)
    after
        15000 -> lager:debug("metaflow receiver exit")
    end.
