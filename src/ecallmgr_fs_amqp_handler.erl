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

-spec init() -> 'ok'.
init() -> 'ok'.

-spec handle_req(kz_json:object(), kz_proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    Node = props:get_value('FSNode', Props),
    UUID = kz_json:get_value(<<"Unique-ID">>, JObj),
    Event = kz_json:get_value(<<"Event-Subclass">>, JObj, kz_json:get_value(<<"Event-Name">>, JObj)),
    EventProps = [{<<"Switch-URL">>, props:get_value('Switch-URL', Props)}
                  ,{<<"Switch-URI">>, props:get_value('Switch-URI', Props)}
                  ,{<<"Switch-Node">>, kz_util:to_binary(Node)}
                  | kz_json:to_proplist(JObj)
                 ],
    process_event(Node, UUID, Event, EventProps).

process_event(Node, UUID, <<"CHANNEL_CREATE">> = Event, Props) ->
    NewProps = ecallmgr_fs_loopback:filter(Node, UUID, Props),
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    ecallmgr_call_events:process_channel_event(NewProps);
process_event(_Node, UUID, <<"CHANNEL_DESTROY">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    ecallmgr_call_events:process_channel_event(Props);
process_event(_Node, UUID, <<"CHANNEL_HANGUP_COMPLETE">> = Event, _Props) ->
    lager:debug("ignoring freeswitch amqp event ~s for callid ~s", [Event, UUID]);
process_event(_Node, UUID, Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    ecallmgr_call_events:process_channel_event(Props).
