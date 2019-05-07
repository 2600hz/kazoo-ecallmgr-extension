%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2019, 2600Hz
%%% @doc Receive route(dialplan) requests from FS, request routes and respond
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
                {'ok', ChannelProps} ->
                    lager:debug("got channel data for ~s : ~s", [UUID, kz_json:encode(ChannelProps, ['pretty'])]),
                    handle_metaflow_action(UUID, JObj, ControlQ, ChannelProps, Node);
                {'error', Error} -> lager:debug("error ~p getting channel data for ~s, exiting", [Error, UUID])
            end;
        {'ok', #channel{}} -> lager:debug("channel ~s not handled on this node, exiting", [UUID])
    end.

-spec handle_metaflow_action(kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary(), kz_term:proplist(), atom()) -> 'ok'.
handle_metaflow_action(UUID, JObj, ControlQ, FSProps, Node) ->
    Metaflow = [{<<"module">>, kz_json:get_value(<<"Action">>, JObj)}
               ,{<<"data">>, kz_json:get_value(<<"Data">>, JObj)}
               ],
    CRHs = [{<<"Metaflow-Request-Type">>, <<"metaflow">>}
           ,{<<"Metaflow">>, kz_json:from_list(Metaflow)}
           ,{<<"Other-Leg-Call-ID">>, kz_evt_freeswitch:other_leg_call_id(FSProps)}
           ],

    ReqProps = [{<<"Resource-Type">>,<<"metaflow">>}
               ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
               ,{<<"Application-Logical-Direction">>, <<"inbound">>}
               ,{<<"Control-Queue">>, ControlQ}
               ],
%%    Props = props:set_values(ReqProps, FSProps),
    Props = kz_json:set_values(ReqProps, FSProps),
    route_metaflow_action(UUID, Props, Node).

-spec route_metaflow_action(kz_term:ne_binary(), kz_term:proplist(), atom()) -> 'ok'.
route_metaflow_action(UUID, Props, Node) ->
    FetchId = kz_binary:rand_hex(16),
%%    Request = ecallmgr_fs_router_util:route_req(UUID, FetchId, Props, Node),
%%    lager:debug("METAFLOW ACVTION ~p", [Request]),
    MoreProps = [{<<"Call-ID">>, UUID}
                ,{[<<"Custom-Channel-Vars">>, <<"Fetch-ID">>], FetchId}
                  | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
                ],
    Request = kz_json:set_values(MoreProps, kz_api:remove_defaults(Props)),
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
            start_metaflow_handling(Node, FetchId, UUID, JObj, Props)
    end.

-spec start_metaflow_handling(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object(), kz_term:proplist()) -> 'ok'.
start_metaflow_handling(_Node, FetchId, CallId, JObj, Props) ->
    ControlQ = kz_json:get_value(<<"Control-Queue">>, Props),
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
