%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2026, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_amqp_event_stream).

-behaviour(gen_listener).

-export([start_link/1]).

-export([handle_req/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr_extension.hrl").

-define(RESPONDERS, [{?MODULE
                     ,[{<<"*">>, <<"*">>}]
                     }
                    ]).

-define(QUEUE_NAME(Name), <<"ecallmgr_amqp_event_", (kz_term:to_binary(Name))/binary, "_", (binary:replace(kz_term:to_binary(node()), <<"@">>, <<"_">>))/binary>>).
-define(QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(CONSUME_OPTIONS, [{'exclusive', 'false'}]).

-type state() :: map().
-type bindings() :: atom() | [atom(),...] | kz_term:ne_binary() | kz_term:ne_binaries().
-type profile() :: {atom() | kz_term:ne_binary(), bindings()}.

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_link(profile()) -> kz_types:startlink_ret().
start_link({Name, Events}=Profile) ->
    Bindings = amqp_bindings(Events),
    gen_listener:start_link(?MODULE
                           ,[{'responders', ?RESPONDERS}
                            ,{'bindings', Bindings}
                            ,{'queue_name', ?QUEUE_NAME(Name)}
                            ,{'queue_options', ?QUEUE_OPTIONS}
                            ,{'consume_options', ?CONSUME_OPTIONS}
                            ]
                           ,[Profile, Bindings]).


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server
%%
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> {'ok', state()}.
init([{Name, _} = Profile, Bindings]) ->
    lager:info("starting new amqp event stream listener for ~s", [Name]),
    {'ok', #{profile => Profile, bindings => Bindings}}.

-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'gen_listener',{'is_consuming', _IsConsuming}}, State) ->
    {'noreply', State};
handle_cast({'gen_listener',{'created_queue', _Q}}, State) ->
    {'noreply', State};
handle_cast(_Cast, State) ->
    lager:debug("unhandled cast: ~p", [_Cast]),
    {'noreply', State, 'hibernate'}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info(_Info, State) ->
    lager:debug("unhandled message: ~p", [_Info]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), state()) -> gen_listener:handle_event_return().
handle_event(_JObj, _State) ->
    {'reply', []}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, #{profile := {Name, _}}) ->
    lager:debug("amqp event stream for ~s termination: ~p", [ Name, _Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed
%%
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    kz_log:put_callid(JObj),
    Node = kz_term:to_atom(kz_api:node(JObj), 'true'),
    handle_stream(Node, mod_com_kazoo:core_uuid(Node), JObj, Props).

-spec handle_stream(atom(), atom(), kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_stream(_Node, 'undefined', _JObj, _Props) -> 'ok';
handle_stream('undefined', _CoreUUID, _JObj, _Props) -> 'ok';
handle_stream(Node, Node, _JObj, _Props) -> 'ok';
handle_stream(Node, CoreUUID, JObj, Props) ->
    kz_log:put_callid(JObj),
    UUID = kz_api:call_id(JObj),
    Category = kz_api:event_category(JObj),
    Event = kz_api:event_name(JObj),
    Ctx = #{node => Node
           ,core_uuid => CoreUUID
           ,call_id => UUID
           ,category => Category
           ,event => Event
           ,payload => JObj
           ,basic => props:get_value('basic', Props)
           ,deliver => props:get_value('deliver', Props)
           },
    log_event(Ctx),
    run_bindings(Ctx).

log_event(#{event := 'undefined'
           ,payload := JObj
           }) ->
    lager:debug_unsafe("received unknown fs event : ~s", [kz_json:encode(JObj)]);
log_event(#{category := Category
           ,event := Event
           ,payload := JObj
           ,basic := Basic
           ,deliver := Deliver
           }) ->
    NowUs = erlang:system_time('micro_seconds'),
    Created = kz_json:get_integer_value(<<"Event-Timestamp">>, JObj, 0),
    Published = Basic#'P_basic'.timestamp,
    lager:debug("received fs ~s : ~s (~B,~B,~B) => ~s => ~s", [Category, Event
                                                              ,Published - Created
                                                              ,NowUs - Published
                                                              ,NowUs - Created
                                                              ,gen_listener:routing_key_used(Deliver)
                                                              ,log_basic_headers(Basic)
                                                              ]).

log_basic_headers(#'P_basic'{headers=undefined}) -> <<"no-headers">>;
log_basic_headers(#'P_basic'{headers=Headers}) ->
    kz_binary:join([io_lib:format("~s = ~s", [K, kz_term:to_binary(V)]) || {K, _, V} <- Headers]).

run_bindings(Ctx) ->
    Stages = [fun run_event/1
             ,fun run_process/1
             ,fun run_notify/1
             ],
    Fun = fun(StageFun) -> StageFun(Ctx) end,
    lists:foreach(Fun, Stages).

run_event(Ctx) ->
    Routing = create_routing(<<"event">>, Ctx),
    kazoo_bindings:map(Routing, Ctx).

run_process(Ctx) ->
    Routing = create_routing(<<"process">>, Ctx),
    kazoo_bindings:map(Routing, Ctx).

run_notify(Ctx) ->
    Routing = create_routing(<<"registered">>, Ctx),
    kazoo_bindings:map(Routing, Ctx).

create_routing(Name, #{category := Category, event := Event}) ->
    <<"event_stream.", Name/binary, ".", Category/binary, ".", Event/binary>>.



-spec amqp_bindings(bindings()) -> kz_term:proplist().
amqp_bindings(Bindings) ->
    lists:foldl(fun amqp_bindings_fold/2, [], Bindings).

amqp_bindings_fold(Binding, Props) ->
    {Kapi, Event} = true_event(Binding),
    case props:get_value(Kapi, Props) of
        undefined -> [{Kapi, [{'restrict_to', [Event]}]}];
        List ->
            RestrictTo = props:get_value('restrict_to', List, []),
            case props:get_value(Event, RestrictTo) of
                undefined ->
                    NewList = props:set_value('restrict_to', [Event | RestrictTo], List),
                    props:set_value(Kapi, NewList, Props);
                _ -> Props
            end
    end.

true_event(Event)
  when is_atom(Event) ->
    true_event(kz_term:to_binary(Event));
true_event(<<"sofia::transferor">>) -> {call, 'CHANNEL_TRANSFEROR'};
true_event(<<"sofia::transferee">>) -> {call, 'CHANNEL_TRANSFEREE'};
true_event(<<"sofia::replaced">>) -> {call, 'CHANNEL_REPLACED'};
true_event(<<"sofia::intercepted">>) -> {call, 'CHANNEL_INTERCEPTED'};
true_event(<<"spandsp::", _/binary>>) -> {call, 'CHANNEL_FAX_STATUS'};
true_event(<<"conference::maintenance">>) -> {conference, 'event'};
true_event(<<"loopback::bowout">>) -> {call, 'CHANNEL_BOWOUT'};
true_event(<<"loopback::direct">>) -> {call, 'CHANNEL_DIRECT'};
true_event(Event) -> {call, kz_term:to_atom(Event, 'true')}.
