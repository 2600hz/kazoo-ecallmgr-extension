%%%-------------------------------------------------------------------
%%% @copyright (C) 2015, 2600Hz
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(kapi_freeswitch).

-export([bind_q/2
	,unbind_q/2
        ]).

%%-export([declare_exchanges/0, routing_key_format/0]).
-export([declare_exchanges/0]).

-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_amqp/include/kz_api.hrl").

-define(EXCHANGE_FREESWITCH, <<"freeswitch">>).
-define(TYPE_FREESWITCH, <<"topic">>).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec bind_q(ne_binary(), kz_proplist()) -> 'ok'.
bind_q(Queue, Props) ->
    RestrictTo = props:get_value('restrict_to', Props),
    bind_q(Queue, RestrictTo, Props).

-spec bind_q(ne_binary(), api_binaries(), kz_proplist()) -> 'ok'.
bind_q(Queue, 'undefined', _) ->
    amqp_util:bind_q_to_exchange(Queue, <<"#">>, ?EXCHANGE_FREESWITCH);
bind_q(Queue, ['key'|Restrict], Props) ->
    amqp_util:bind_q_to_exchange(Queue, routing_key(Props), ?EXCHANGE_FREESWITCH),
    bind_q(Queue, Restrict, Props);
bind_q(Queue, [_|Restrict], Props) ->
    bind_q(Queue, Restrict, Props);
bind_q(_, [], _) -> 'ok'.

-spec unbind_q(ne_binary(), kz_proplist()) -> 'ok'.
unbind_q(Queue, Props) ->
    RestrictTo = props:get_value('restrict_to', Props),
    unbind_q(Queue, RestrictTo, Props).

-spec unbind_q(ne_binary(), api_binary(), kz_proplist()) -> 'ok'.
unbind_q(Queue, 'undefined', _) ->
    amqp_util:unbind_q_from_exchange(Queue, <<"#">>, ?EXCHANGE_FREESWITCH);
unbind_q(Queue, ['key'|Restrict], Props) ->
    amqp_util:unbind_q_from_exchange(Queue, routing_key(Props), ?EXCHANGE_FREESWITCH),
    unbind_q(Queue, Restrict, Props);
unbind_q(Queue, [_|Restrict], Props) ->
    unbind_q(Queue, Restrict, Props);
unbind_q(_, [], _) -> 'ok'.


%% -spec routing_key_format() -> binary().
%% routing_key_format() -> <<"#FreeSWITCH,FreeSWITCH-Hostname,Event-Subclass|Event-Name,Unique-ID">>.


-spec routing_key(kz_proplist()) -> binary().
routing_key(Props) ->
    routing_key(props:get_value('profile', Props, <<"*">>)
               ,props:get_value('hostname', Props, <<"*">>)
               ,props:get_value('event', Props, <<"*">>)
      	       ,props:get_value('callid', Props, <<"*">>)
               ).

-spec routing_key(binary(), binary(), binary(), binary()) -> binary().
routing_key(Profile, Hostname, Event, CallId) ->
    list_to_binary([<<"FreeSWITCH.">>
           ,amqp_util:encode(Profile)
           ,"."
		   ,amqp_util:encode(Hostname)
		   ,"."
		   ,amqp_util:encode(Event)
		   ,"."
		   ,amqp_util:encode(CallId)
                   ]).

%%--------------------------------------------------------------------
%% @doc
%% declare the exchanges used by this API
%% @end
%%--------------------------------------------------------------------
-spec declare_exchanges() -> 'ok'.
declare_exchanges() ->
    amqp_util:new_exchange(?EXCHANGE_FREESWITCH, ?TYPE_FREESWITCH).

