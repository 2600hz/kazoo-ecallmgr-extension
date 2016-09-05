%%%-------------------------------------------------------------------
%%% @copyright (C) 2012-2015, 2600Hz INC
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_amqp_handler).

-export([init/0, handle_req/2]).

-include("ecallmgr-extension.hrl").

-spec init() -> 'ok'.
init() -> 'ok'.

-spec handle_req(kz_json:object(), kz_proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    Node = props:get_value('FSNode', Props),
    UUID = kz_json:get_value(<<"Unique-ID">>, JObj),
    kz_util:put_callid(UUID),
    Event = kz_json:get_value(<<"Event-Subclass">>, JObj, kz_json:get_value(<<"Event-Name">>, JObj)),
    EventProps = [{<<"Switch-URL">>, props:get_value('Switch-URL', Props)}
                 ,{<<"Switch-URI">>, props:get_value('Switch-URI', Props)}
                 ,{<<"Switch-Node">>, kz_util:to_binary(Node)}
                  | kz_json:to_proplist(JObj)
                 ],
    process_event(Node, UUID, Event, decode(EventProps)).

-spec decode(kz_proplist()) -> kz_proplist().
decode(Props) ->
    lists:map(fun url_decode/1, Props).

-spec url_decode(tuple() | binary()) -> tuple() | binary().
url_decode({K, V}=KV)
  when is_binary(V) ->
    try kz_util:uri_decode(V) of
        V1 -> {K, V1}
    catch
        _:_ -> KV
    end;
url_decode({K, V})
  when is_list(V) ->
    {K, lists:map(fun url_decode/1, V)};
url_decode(V)
  when is_binary(V)->
    try kz_util:uri_decode(V) of
        V1 -> V1
    catch
        _:_ -> V
    end;
url_decode(KV) -> KV.

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
