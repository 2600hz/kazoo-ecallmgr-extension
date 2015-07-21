%%%-------------------------------------------------------------------
%%% @copyright (C) 2012-2015, 2600Hz INC
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_amqp_handler).

-export([init/0, handle_req/2]).

-include("../../ecallmgr/src/ecallmgr.hrl").

-define(FSMAP, [{<<"">>, <<"">>}]).


-spec init() -> 'ok'.
init() -> 'ok'.

-spec handle_req(wh_json:object(), wh_proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    lager:debug("FS AMQP ~p", [JObj]),
%    Node = props:get_value('FSNode', Props), 
    UUID = wh_json:get_value(<<"Unique-ID">>, JObj), 
    Event = wh_json:get_value(<<"Event-Subclass">>, JObj, wh_json:get_value(<<"Event-Name">>, JObj)),
    EventSubclass = wh_json:get_value(<<"Event-Subclass">>, JObj),
    EventProps = [{<<"Switch-URL">>, props:get_value('Switch-URL', Props)}
                  ,{<<"Switch-URI">>, props:get_value('Switch-URI', Props)}
                  | wh_json:to_proplist(JObj)
                 ],
    lager:debug("processing freeswitch amqp event ~s , ~s, ~s", [Event, EventSubclass, UUID]),
    ecallmgr_call_events:process_channel_event(EventProps).

%%    process_event(Event, UUID, EventProps, Node),
%%    maybe_send_event(Event, UUID, EventProps, Node).

%% -spec process_event(ne_binary(), api_binary(), wh_proplist(), atom()) -> any().
%% process_event(<<"CHANNEL_CREATE">>, UUID, _Props, Node) ->
%%     wh_util:put_callid(UUID),
%%     maybe_start_event_listener(Node, UUID);
%% process_event(?CHANNEL_MOVE_RELEASED_EVENT_BIN, _, Props, Node) ->
%%     UUID = props:get_value(<<"old_node_channel_uuid">>, Props),
%%     wh_util:put_callid(UUID),
%%     gproc:send({'p', 'l', ?CHANNEL_MOVE_REG(Node, UUID)}
%%                ,?CHANNEL_MOVE_RELEASED_MSG(Node, UUID, Props)
%%               );
%% process_event(?CHANNEL_MOVE_COMPLETE_EVENT_BIN, _, Props, Node) ->
%%     UUID = props:get_value(<<"old_node_channel_uuid">>, Props),
%%     wh_util:put_callid(UUID),
%%     gproc:send({'p', 'l', ?CHANNEL_MOVE_REG(Node, UUID)}
%%                ,?CHANNEL_MOVE_COMPLETE_MSG(Node, UUID, Props)
%%               );
%% process_event(<<"sofia::register">>, _UUID, Props, Node) ->
%%     gproc:send({'p', 'l', ?REGISTER_SUCCESS_REG}, ?REGISTER_SUCCESS_MSG(Node, Props));
%% process_event(<<"loopback::bowout">>, _UUID, Props, Node) ->
%%     ResigningUUID = props:get_value(?RESIGNING_UUID, Props),
%%     wh_util:put_callid(ResigningUUID),
%%     lager:debug("bowout detected on ~s, transferring to ~s"
%%                 ,[ResigningUUID, props:get_value(?ACQUIRED_UUID, Props)]
%%                ),
%%     gproc:send({'p', 'l', ?LOOPBACK_BOWOUT_REG(ResigningUUID)}, ?LOOPBACK_BOWOUT_MSG(Node, Props));
%% process_event(_, _, _, _) -> 'ok'.
%% 
%% -spec maybe_send_event(ne_binary(), api_binary(), wh_proplist(), atom()) -> any().
%% maybe_send_event(<<"HEARTBEAT">>, _UUID, _Props, _Node) -> 'ok';
%% maybe_send_event(<<"CHANNEL_BRIDGE">>=EventName, UUID, Props, Node) ->
%%     wh_util:put_callid(UUID),
%%     BridgeID = props:get_value(<<"variable_bridge_uuid">>, Props),
%%     DialPlan = props:get_value(<<"Caller-Dialplan">>, Props),
%%     Direction = props:get_value(<<"Call-Direction">>, Props),
%%     App = props:get_value(<<"variable_current_application">>, Props),
%%     Destination = props:get_value(<<"Caller-Destination-Number">>, Props),
%% 
%%     case {BridgeID, Direction, DialPlan, App, Destination} of
%%         {'undefined', _, _, _, _} ->
%%             gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, EventName)}, {'event', [UUID | Props]}),
%%             maybe_send_call_event(UUID, Props, Node);
%%         {BridgeID, <<"inbound">>, <<"inline">>, <<"intercept">>, 'undefined'} ->
%%             SwappedProps = ecallmgr_call_events:swap_call_legs(Props),
%%             gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, EventName)}, {'event', [BridgeID | SwappedProps]}),
%%             maybe_send_call_event(BridgeID, SwappedProps, Node);
%%         _Else ->
%%             gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, EventName)}, {'event', [UUID | Props]}),
%%             maybe_send_call_event(UUID, Props, Node)
%%     end;
%% maybe_send_event(<<"loopback::bowout">> = EventName, _UUID, Props, Node) ->
%%     ResigningUUID = props:get_value(?RESIGNING_UUID, Props),
%%     _AcquiringUUID = props:get_value(?ACQUIRED_UUID, Props),
%%     wh_util:put_callid(ResigningUUID),
%% 
%%     lager:debug("bowout for '~s', resigning ~s acquiring ~s", [_UUID, ResigningUUID, _AcquiringUUID]),
%% 
%%     send_event(EventName, ResigningUUID, Props, Node);
%% maybe_send_event(<<"CHANNEL_DESTROY">> = EventName, UUID, Props, Node) ->
%%     wh_util:put_callid(UUID),
%%     case ecallmgr_fs_channel:node(UUID) of
%%         {'ok', Node} ->
%%             gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, EventName)}, {'event', [UUID | Props]}),
%%             maybe_send_call_event(UUID, Props, Node);
%%         {'ok', _OtherNode} ->
%%             lager:debug("dropping channel destroy from ~s (expected ~s)", [Node, _OtherNode]);
%%         {'error', 'not_found'} ->
%%             lager:debug("dropping channel destroy from ~s (no such channel)", [Node])
%%     end;
%% maybe_send_event(EventName, UUID, Props, Node) ->
%%     wh_util:put_callid(UUID),
%%     case wh_util:is_true(props:get_value(<<"variable_channel_is_moving">>, Props)) of
%%         'true' -> 'ok';
%%         'false' ->
%%             send_event(EventName, UUID, Props, Node)
%%     end.
%% 
%% send_event(EventName, UUID, Props, Node) ->
%%     gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, EventName)}, {'event', [UUID | Props]}),
%%     maybe_send_call_event(UUID, Props, Node).
%% 
%% -spec maybe_send_call_event(api_binary(), wh_proplist(), atom()) -> any().
%% maybe_send_call_event('undefined', _, _) -> 'ok';
%% maybe_send_call_event(CallId, Props, Node) ->
%%     gproc:send({'p', 'l', ?FS_CALL_EVENT_REG_MSG(Node, CallId)}, {'event', [CallId | Props]}).
%% 
%% -spec maybe_start_event_listener(atom(), ne_binary()) -> 'ok' | sup_startchild_ret().
%% maybe_start_event_listener(Node, UUID) ->
%%     case wh_cache:fetch_local(?ECALLMGR_UTIL_CACHE, {UUID, 'start_listener'}) of
%%         {'ok', 'true'} -> ecallmgr_call_sup:start_event_process(Node, UUID);
%%         _E -> 'ok'
%%     end.
