%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_amqp_handler).

-export([init/0, handle_req/2]).

-include("ecallmgr_extension.hrl").

-spec init() -> 'ok'.
init() -> 'ok'.

-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    Node = props:get_value('FSNode', Props),
    UUID = kz_json:get_value(<<"Unique-ID">>, JObj),
    kz_util:put_callid(UUID),
    Event = kz_json:get_value(<<"Event-Subclass">>, JObj, kz_api:event_name(JObj)),
    EventProps = [{<<"Switch-URL">>, props:get_value('Switch-URL', Props)}
                 ,{<<"Switch-URI">>, props:get_value('Switch-URI', Props)}
                 ,{<<"Switch-Node">>, kz_term:to_binary(Node)}
                 ,{<<"Switch-Nodename">>, kz_term:to_binary(Node)}
                 ,{<<"Force-Publish-Event-State">>, 'true'}
                  | kz_json:to_proplist(JObj)
                 ],
    process_event(Node, UUID, Event, decode(EventProps)).

-spec decode(kz_term:proplist()) -> kz_term:proplist().
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
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    NewProps = ecallmgr_fs_loopback:filter(Node, UUID, Props),
    ecallmgr_call_events:process_channel_event(NewProps);
process_event(_Node, UUID, <<"CHANNEL_DESTROY">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    ecallmgr_call_events:process_channel_event(Props);
process_event(Node, UUID, <<"PRESENCE_IN">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, Event)}, {'event', [UUID | Props]});
process_event(Node, UUID, <<"KZ_PRESENCE_DIALOG">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, Event)}, {'event', [UUID | Props]});
process_event(Node, UUID, <<"conference::maintenance">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, Event)}, {'event', [UUID | Props]});
process_event(Node, UUID, <<"RECORD_START">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, Event)}, {'event', [UUID | Props]});
process_event(Node, UUID, <<"RECORD_STOP">> = Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    gproc:send({'p', 'l', ?FS_EVENT_REG_MSG(Node, Event)}, {'event', [UUID | Props]});
process_event(_Node, UUID, Event, Props) ->
    lager:debug("processing freeswitch amqp event ~s for callid ~s", [Event, UUID]),
    ecallmgr_call_events:process_channel_event(Props).
