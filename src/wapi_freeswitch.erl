%%%-------------------------------------------------------------------
%%% @copyright (C) 2015, 2600Hz
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(wapi_freeswitch).

-export([bind_q/2
         ,unbind_q/2
        ]).

-export([declare_exchanges/0, routing_key_format/0]).

-include_lib("whistle/include/wh_api.hrl").

-define(EXCHANGE_FREESWITCH, <<"freeswitch">>).
-define(TYPE_FREESWITCH, <<"fanout">>).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec bind_q(ne_binary(), wh_proplist()) -> 'ok'.
bind_q(Queue, Props) ->
    declare_exchanges(),
    RestrictTo = props:get_value('restrict_to', Props),
    bind_q(Queue, RestrictTo, Props).

-spec bind_q(ne_binary(), api_binaries(), wh_proplist()) -> 'ok'.
bind_q(Queue, 'undefined', _) ->
    amqp_util:bind_q_to_exchange(Queue, <<"#">>, ?EXCHANGE_FREESWITCH);
bind_q(Queue, ['key'|Restrict], Props) ->
    amqp_util:bind_q_to_exchange(Queue, routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, [_|Restrict], Props) ->
    bind_q(Queue, Restrict, Props);
bind_q(_, [], _) -> 'ok'.

-spec unbind_q(ne_binary(), wh_proplist()) -> 'ok'.
unbind_q(Queue, Props) ->
    RestrictTo = props:get_value('restrict_to', Props),
    unbind_q(Queue, RestrictTo, Props).

-spec unbind_q(ne_binary(), api_binary(), wh_proplist()) -> 'ok'.
unbind_q(Queue, 'undefined', _) ->
    amqp_util:unbind_q_from_exchange(Queue, <<"#">>, ?EXCHANGE_FREESWITCH);
unbind_q(Queue, ['key'|Restrict], Props) ->
    amqp_util:unbind_q_from_exchange(Queue, routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, [_|Restrict], Props) ->
    unbind_q(Queue, Restrict, Props);
unbind_q(_, [], _) -> 'ok'.


routing_key_format() -> <<"#FreeSWITCH,FreeSWITCH-Hostname,Event-Name,Event-Subclass,Unique-ID">>.
    

routing_key(Props) ->
    routing_key(props:get_value('hostname', Props, <<"*">>)
                ,props:get_value('event', Props, <<"*">>)
                ,props:get_value('subclass', Props, <<"*">>)
                ,props:get_value('callid', Props, <<"*">>)
               ).

%% routing_key(Event, Subclass) ->
%%     routing_key(props:get_value('hostname', Props, <<"*">>)
%%                 ,Event
%%                 ,Subclass
%%                 ,props:get_value('callid', Props, <<"*">>)
%%                ).

routing_key(Hostname, Event, _Subclass, CallId) ->
    list_to_binary([<<"FreeSWITCH.">>
                      ,amqp_util:encode(Hostname)
                      ,"."
                      ,amqp_util:encode(Event)
                      ,"."
%                      ,amqp_util:encode(Subclass)
%                      ,"."
                      ,amqp_util:encode(CallId)
                   ]).

%%--------------------------------------------------------------------
%% @doc
%% declare the exchanges used by this API
%% @end
%%--------------------------------------------------------------------
-spec declare_exchanges() -> 'ok'.
declare_exchanges() ->
    amqp_util:new_exchange(?EXCHANGE_FREESWITCH, ?TYPE_FREESWITCH, [{durable, 'true'}]).

