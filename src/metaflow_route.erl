%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2018, 2600Hz
%%% @doc Receive route(dialplan) requests from FS, request routes and respond
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_route).

-export([handle_metaflow_route/5]).

-import(ecallmgr_fs_xml, [action_el/2
                         ,condition_el/1
                         ,context/2, context_el/2
                         ,extension_el/3
                         ,section_el/3
                         ]).

-include("metaflow.hrl").

-spec handle_metaflow_route(atom(), atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:proplist()) -> 'ok'.
handle_metaflow_route(Section, Node, FetchId, UUID, FSProps) ->
    kz_util:put_callid(UUID),
    Props = init_metaflow_props(Node, FSProps),
    case ecallmgr_fs_router_util:search_for_route(Section, Node, FetchId, UUID, Props, 'false') of
        'ok' -> lager:debug("xml fetch metaplan ~s finished without success", [FetchId]);
        {'ok', JObj} -> start_metaflow_handling(Node, FetchId, UUID, JObj, Props)
    end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec init_metaflow_props(atom(), kz_term:proplist()) -> kz_term:proplist().
init_metaflow_props(Node, Props) ->
    Routines = [fun add_metaflow_missing_props/1
               ],
    lists:foldl(fun(F,P) -> F(P) end, [{<<"FreeSWITCH-Node">>, Node} | Props], Routines).

-spec add_metaflow_missing_props(kz_term:proplist()) -> kz_term:proplist().
add_metaflow_missing_props(Props) ->
    Number = metaflow_number(props:get_value(<<"Hunt-Destination-Number">>, Props)),
    CRHs = [{<<"Metaflow-Request-Type">>, <<"in-call">>}
           ,{<<"Metaflow-Request">>, Number}
           ,{<<"Other-Leg-Call-ID">>, kzd_freeswitch:other_leg_call_id(Props)}
           ],
    AddProps = props:filter_undefined(
                 [{<<"Resource-Type">>,<<"metaflow">>}
                 ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
                 ,{<<"Route-Resp-Xml-Fun">>, fun route_resp_xml/4}
                 ]),
    props:set_values(AddProps, Props).

-spec metaflow_number(binary()) -> binary().
metaflow_number(<<"*", Number/binary>>) -> Number;
metaflow_number(Number) -> Number.

-spec start_metaflow_handling(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object(), kz_term:proplist()) -> 'ok'.
start_metaflow_handling(_Node, FetchId, CallId, JObj, Props) ->
    ControlQ = props:get_value(<<"Control-Queue">>, Props),
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

-spec route_resp_xml(kz_term:ne_binary(), kz_json:objects(), kz_json:object(), kz_term:proplist()) -> {'ok', iolist()}.
route_resp_xml(<<"application">>, _Routes, JObj, Props) ->
    Node = props:get_value(<<"FreeSWITCH-Node">>, Props),
    UUID = props:get_value(<<"Unique-UUID">>, Props),
    Exten = [route_resp_log_winning_node()
             | route_resp_ccvs(Node, UUID, JObj)
            ],
    ParkExtEl = extension_el(<<"metaflow">>, 'undefined', [condition_el(Exten)]),
    Context = context(JObj, Props),
    ContextEl = context_el(Context, [ParkExtEl]),
    SectionEl = section_el(<<"dialplan">>, <<"Metaflow Application Response">>, ContextEl),
    {'ok', xmerl:export([SectionEl], 'fs_xml')}.

-spec route_resp_log_winning_node() -> kz_types:xml_el().
route_resp_log_winning_node() ->
    action_el(<<"log">>, [<<"NOTICE log|${uuid}|", (kz_term:to_binary(node()))/binary, " won metaflow control">>]).

-spec route_resp_ccvs(atom(), kz_term:ne_binary(), kz_json:object()) -> kz_types:xml_els().
route_resp_ccvs(Node, UUID, JObj) ->
    case kz_json:get_value(<<"Custom-Channel-Vars">>, JObj) of
        'undefined' -> [];
        CCVs -> [action_el(<<"kz_multiset">>, ecallmgr_util:multi_set_args(Node, UUID, kz_json:to_proplist(CCVs)))]
    end.
