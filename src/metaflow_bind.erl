%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2018, 2600Hz
%%% @doc Handle BRIDGE events and request metaflow bind
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_bind).


-export([handle_bridge/3]).

-include("metaflow.hrl").


-spec handle_bridge(atom(), kz_term:api_binary(), kz_term:proplist()) -> any().
handle_bridge(Node, _UUID, Props) ->
    ALeg = props:get_value(<<"Bridge-A-Unique-ID">>, Props),
    BLeg = props:get_value(<<"Bridge-B-Unique-ID">>, Props),
    AChannel = ecallmgr_fs_channel:fetch(ALeg, 'record'),
    kz_util:spawn(fun request_metaflow/4, [Node, <<"A">>, BLeg, AChannel]),
    BChannel = ecallmgr_fs_channel:fetch(BLeg, 'record'),
    kz_util:spawn(fun request_metaflow/4, [Node, <<"B">>, ALeg, BChannel]).

-spec request_metaflow(atom(), kz_term:ne_binary(), kz_term:ne_binary(), channel()) -> any().
request_metaflow(Node, Leg, OtherLegUUID, {'ok', #channel{handling_locally='true'
                                                         ,account_id=?NE_BINARY=AccountId
                                                         ,uuid=UUID
                                                         ,authorizing_id=AuthorizingId
                                                         ,resource_id=ResourceId
                                                         ,callflow_id=CallFlowId
                                                         ,node=Node
                                                         }=Channel
                                          }) ->
    kz_util:put_callid(UUID),
    API = [{<<"Account-ID">>, AccountId}
          ,{<<"Binding-Leg">>, Leg}
          ,{<<"Call-ID">>, UUID}
          ,{<<"Authorizing-ID">>, AuthorizingId}
          ,{<<"Resource-ID">>, ResourceId}
          ,{<<"CallFlow-ID">>, CallFlowId}
           | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
          ],
    case gproc:lookup_values({'p', 'l', ?METAFLOW_REG_MSG(UUID)}) =:= []
        andalso kz_amqp_worker:call(API, fun kapi_metaflow:publish_bind_req/1, fun kapi_metaflow:binding_v/1)
    of
        {'ok', JObj} -> process_metaflow(Node, UUID, OtherLegUUID, JObj, Channel);
        'false' -> lager:debug("metaflow for ~s already in place", [UUID]);
        _Else -> lager:debug("error requesting metaflow binding : ~p", [_Else])
    end;
request_metaflow(_Node, _Leg, _OtherLegUUID, _Channel) -> 'ok'.

-spec process_metaflow(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object(), channel()) -> no_return().
process_metaflow(Node, UUID, OtherLegUUID, JObj, _Channel) ->
    lager:debug("metaflow binding received"),
    Patterns = kz_json:get_json_value(<<"Patterns">>, JObj, kz_json:new()),
    Numbers = kz_json:get_json_value(<<"Numbers">>, JObj, kz_json:new()),

    lager:debug("numbers: ~p", [Numbers]),
    lager:debug("patterns: ~p", [Patterns]),

    case kz_json:get_keys(Patterns) =/= []
        orelse kz_json:get_keys(Numbers) =/= []
    of
        'true' -> metaflow_fsm_sup:new(Node, UUID, OtherLegUUID, JObj);
        'false' -> 'ok'
    end.
