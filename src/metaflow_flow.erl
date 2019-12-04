%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2019, 2600Hz
%%% @doc Receive flow requests
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_flow).


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
                {'ok', ChannelData} -> handle_metaflow_flow(UUID, JObj, ControlQ, ChannelData, Node);
                {'error', Error} -> lager:debug("error ~p getting channel data for ~s, exiting", [Error, UUID])
            end;
        {'ok', #channel{}} -> lager:debug("channel ~s not handled on this node, exiting", [UUID])
    end.

-spec handle_metaflow_flow(kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary(), kz_json:object(), atom()) -> 'ok'.
handle_metaflow_flow(UUID, JObj, ControlQ, ChannelJObj, Node) ->
    CRHs = [{<<"Metaflow-Request-Type">>, <<"metaflow">>}
           ,{<<"Metaflow">>, kz_json:get_value(<<"Flow">>, JObj)}
           ,{<<"Other-Leg-Call-ID">>, kz_json:get_ne_binary_value(<<"Other-Leg-Call-ID">>, ChannelJObj)}
           ],

    FetchId = kz_binary:rand_hex(16),
    ReqProps = [{<<"Resource-Type">>,<<"metaflow">>}
               ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
               ,{<<"Application-Logical-Direction">>, <<"inbound">>}
               ,{<<"Control-Queue">>, ControlQ}
               ,{<<"Call-ID">>, UUID}
               ,{<<"Msg-ID">>, FetchId}
               ,{[<<"Custom-Channel-Vars">>, <<"Fetch-ID">>], null}
                | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
               ],
    Request = kz_json:set_values(ReqProps, kz_api:remove_defaults(ChannelJObj)),
    ReqResp = kz_amqp_worker:call(Request
                                 ,fun kapi_route:publish_req/1
                                 ,fun kapi_route:is_actionable_resp/1
                                 ,ecallmgr_fs_node:fetch_timeout(Node)
                                 ),
    case ReqResp of
        {'error', _R} ->
            lager:info("did not receive route response for request ~s: ~p", [FetchId, _R]);
        {'ok', Resp} ->
            'true' = kapi_route:resp_v(Resp),
            start_metaflow_handling(Node, FetchId, UUID, Resp, ControlQ)
    end.

-spec start_metaflow_handling(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary()) -> 'ok'.
start_metaflow_handling(_Node, FetchId, CallId, JObj, ControlQ) ->
    CCVs = [{[<<"Custom-Channel-Vars">>, <<"Application-Name">>], kz_json:get_value(<<"App-Name">>, JObj)}
           ,{[<<"Custom-Channel-Vars">>, <<"Application-Node">>], kz_json:get_value(<<"Node">>, JObj)}
           ],
    send_metaflow_win(ControlQ, FetchId, CallId, kz_json:set_values(CCVs, JObj)).

-spec send_metaflow_win(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object()) -> 'ok'.
send_metaflow_win(ControlQ, FetchId, CallId, JObj) ->
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
