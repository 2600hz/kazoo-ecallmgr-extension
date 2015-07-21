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

-spec handle_req(wh_json:object(), wh_proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    UUID = wh_json:get_value(<<"Unique-ID">>, JObj), 
    Event = wh_json:get_value(<<"Event-Subclass">>, JObj, wh_json:get_value(<<"Event-Name">>, JObj)),
    EventProps = [{<<"Switch-URL">>, props:get_value('Switch-URL', Props)}
                  ,{<<"Switch-URI">>, props:get_value('Switch-URI', Props)}
                  | wh_json:to_proplist(JObj)
                 ],
    lager:debug("processing freeswitch amqp event ~s , ~s, ~s", [Event, UUID]),
    ecallmgr_call_events:process_channel_event(EventProps).
