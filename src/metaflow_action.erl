%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2020, 2600Hz
%%% @doc Receive route(dialplan) requests from FS, request routes and respond
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_action).


-export([handle_req/2]).

-include("metaflow.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    Node = props:get_value('FSNode', Props),
    ControlQ = props:get_value('Control-Q', Props),
    UUID = kz_api:call_id(JObj),
    case ecallmgr_fs_channel:fetch(UUID, 'record') of
        {'error', 'not_found'} -> lager:debug("channel ~s not found locally, exiting", [UUID]);
        {'ok', #channel{handling_locally='true'}} ->
            lager:debug("getting channel data for ~s", [UUID]),
            case ecallmgr_fs_channel:channel_data(Node, UUID) of
                {'ok', ChannelJObj} ->
                    lager:debug("got channel data for ~s : ~s", [UUID, kz_json:encode(ChannelJObj, ['pretty'])]),
                    handle_metaflow_action(UUID, JObj, ControlQ, ChannelJObj, Node);
                {'error', Error} -> lager:debug("error ~p getting channel data for ~s, exiting", [Error, UUID])
            end;
        {'ok', #channel{}} -> lager:debug("channel ~s not handled on this node, exiting", [UUID])
    end.

-spec handle_metaflow_action(kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary(), kz_evt_freeswitch:data(), atom()) -> 'ok'.
handle_metaflow_action(UUID, JObj, ControlQ, ChannelJObj, Node) ->
    Metaflow = [{<<"module">>, kz_json:get_value(<<"Action">>, JObj)}
               ,{<<"data">>, kz_json:get_value(<<"Data">>, JObj)}
               ],
    CRHs = [{<<"Metaflow-Request-Type">>, <<"metaflow">>}
           ,{<<"Metaflow">>, kz_json:from_list(Metaflow)}
           ,{<<"Other-Leg-Call-ID">>, kz_evt_freeswitch:other_leg_call_id(ChannelJObj)}
           ],

    ReqProps = [{<<"Resource-Type">>,<<"metaflow">>}
               ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
               ,{<<"Application-Logical-Direction">>, <<"inbound">>}
               ,{<<"Control-Queue">>, ControlQ}
               ],

    ChannelJObj1 = kz_json:set_values(ReqProps, ChannelJObj),
    route_metaflow_action(UUID, ChannelJObj1, Node).

-spec route_metaflow_action(kz_term:ne_binary(), kz_evt_freeswitch:data(), atom()) -> 'ok'.
route_metaflow_action(UUID, ChannelJObj, Node) ->
    FetchId = kz_binary:rand_hex(16),
    MoreProps = [{<<"Call-ID">>, UUID}
                ,{[<<"Custom-Channel-Vars">>, <<"Fetch-ID">>], FetchId}
                 | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
                ],
    Request = kz_json:set_values(MoreProps, kz_api:remove_defaults(ChannelJObj)),
    ReqResp = kz_amqp_worker:call(Request
                                 ,fun kapi_route:publish_req/1
                                 ,fun kapi_route:is_actionable_resp/1
                                 ,ecallmgr_fs_node:fetch_timeout(Node)
                                 ),
    case ReqResp of
        {'error', _R} ->
            lager:info("did not receive route response for request ~s: ~p", [FetchId, _R]);
        {'ok', JObj} ->
            'true' = kapi_route:resp_v(JObj),
            start_metaflow_handling(Node, FetchId, UUID, JObj, ChannelJObj)
    end.

-spec start_metaflow_handling(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object(), kz_evt_freeswitch:data()) -> 'ok'.
start_metaflow_handling(_Node, FetchId, CallId, JObj, ChannelJObj) ->
    ControlQ = kz_json:get_value(<<"Control-Queue">>, ChannelJObj),
    CCVs = [{[<<"Custom-Channel-Vars">>, <<"Application-Name">>], kz_json:get_value(<<"App-Name">>, JObj)}
           ,{[<<"Custom-Channel-Vars">>, <<"Application-Node">>], kz_json:get_value(<<"Node">>, JObj)}
           ],
    send_metaflow_win(ControlQ, FetchId, CallId, kz_json:set_values(CCVs, JObj)).

-spec send_metaflow_win(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object()) -> 'ok'.
send_metaflow_win(ControlQ, FetchId, CallId, JObj) ->
    lager:debug("ROUTE REP ~s", [kz_json:encode(JObj, ['pretty'])]),
    ControllerQ = kz_api:server_id(JObj),
    CCVs = kz_json:get_value(<<"Custom-Channel-Vars">>, JObj),
    Win = [{<<"Msg-ID">>, FetchId}
          ,{<<"Call-ID">>, CallId}
          ,{<<"Control-Queue">>, ControlQ}
          ,{<<"Custom-Channel-Vars">>, CCVs}
           | kz_api:default_headers(ControlQ, <<"dialplan">>, <<"route_win">>, ?APP_NAME, ?APP_VERSION)
          ],
    lager:debug("sending metaflow route_win to ~s", [ControllerQ]),
    Publisher = fun(API) -> kapi_route:publish_win(ControllerQ, API) end,
    kz_amqp_worker:cast(Win, Publisher).
