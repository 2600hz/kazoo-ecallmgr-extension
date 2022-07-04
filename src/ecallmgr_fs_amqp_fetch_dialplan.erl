%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2022, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_amqp_fetch_dialplan).

-behaviour(gen_listener).

-export([start_link/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,handle_req/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr_extension.hrl").

-define(RESPONDERS, [{?MODULE
                     ,[{<<"*">>, <<"*">>}]
                     }
                    ]).
-define(BINDINGS(CoreUUID), [{'freeswitch', [{'restrict_to', ['dialplan']}
                                            ,{'core_uuid', CoreUUID}
                                            ]}
                            ]).

-define(QUEUE_NAME(CoreUUID), list_to_binary(["ecallmgr_amqp_fetch_dialplan_", kz_term:to_binary(CoreUUID)])).
-define(QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(CONSUME_OPTIONS, [{'exclusive', 'false'}]).

-type state() :: map().

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_link(atom(), kz_term:proplist()) -> kz_types:startlink_ret().
start_link(Node, Options) ->
    CoreUUID = mod_com_kazoo:core_uuid(Node),
    gen_listener:start_link(?MODULE
                           ,[{'responders', ?RESPONDERS}
                            ,{'bindings', ?BINDINGS(CoreUUID)}
                            ,{'queue_name', ?QUEUE_NAME(CoreUUID)}
                            ,{'queue_options', ?QUEUE_OPTIONS}
                            ,{'consume_options', ?CONSUME_OPTIONS}
                            ]
                           ,[Node, Options]).


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server
%%
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> {'ok', state()}.
init([Node, Options]) ->
    lager:debug("starting new ecallmgr amqp fetch dialplan listener for ~s", [Node]),
    {'ok', #{node => Node, options => Options}}.

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
handle_event(_JObj, _State) -> {'reply', []}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, _State) ->
    lager:debug("ecallmgr amqp fetch dialplan termination: ~p", [ _Reason]).

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
    log_event(JObj, Props),
    Node = kzd_fetch:node(JObj),
    FetchId = kzd_fetch:fetch_uuid(JObj),
    CoreUUID = kzd_fetch:core_uuid(JObj),
    Key = kzd_fetch:fetch_key_value(JObj),
    Section = kzd_fetch:fetch_section(JObj),
    Tag = kzd_fetch:fetch_tag(JObj),
    Version = kzd_fetch:fetch_version(JObj),
    Event = kz_term:to_lower_binary(kz_api:event_name(JObj)),
    RKs = lists:filter(fun kz_term:is_not_empty/1, [<<"fetch">>, Section, Tag, Version, Event, Key]),

    Routing = kz_binary:join(RKs, <<".">>),
    Map = #{node => Node
           ,section => kz_term:to_atom(Section, 'true')
           ,tag => kz_term:to_atom(Tag, 'true')
           ,fetch_id => FetchId
           ,payload => JObj
           ,version => kz_term:to_atom(Version, 'true')
           ,core_uuid => kz_term:to_atom(CoreUUID, 'true')
           ,routing => Routing
           ,call_id => kzd_fetch:call_id(JObj)
           ,server_id => kz_api:server_id(JObj)
           ,basic => props:get_value('basic', Props)
           },
    lager:debug("requesting binding for ~s", [Routing]),
    case kazoo_bindings:map(Routing, Map) of
        [] -> not_found(Map);
        _ -> 'ok'
    end.

not_found(#{node := Node, fetch_id := FetchId, section := Section, routing := Routing}=Ctx) ->
    lager:debug("replying not found to ~s request ~s from node ~s with routing ~s", [Section, FetchId, Node, Routing]),
    {'ok', XmlResp} = ecallmgr_fs_xml:not_found(Routing),
    mod_com_kazoo:fetch_reply(Ctx#{reply => iolist_to_binary(XmlResp)}).

-spec log_event(kz_json:object(), kz_term:proplist()) -> 'ok'.
log_event(JObj, Props) ->
    Key = kzd_fetch:fetch_key_value(JObj),
    Section = kzd_fetch:fetch_section(JObj),
    Event = kz_term:to_lower_binary(kz_api:event_name(JObj)),
    Category = kz_term:to_lower_binary(kz_api:event_category(JObj)),
    Basic = props:get_value('basic', Props),
    Deliver = props:get_value('deliver', Props),
    NowUs = erlang:system_time('micro_seconds'),
    Created = kzd_fetch:fetch_timestamp_micro(JObj),
    Published = Basic#'P_basic'.timestamp,
    lager:debug("received fs fetch request ~s (~s,~s) (~s, ~s) (~B,~B,~B) => ~s => ~s"
               ,[kz_api:msg_id(JObj)
                ,Category
                ,Event
                ,Section
                ,Key
                ,Published - Created
                ,NowUs - Published
                ,NowUs - Created
                ,gen_listener:routing_key_used(Deliver)
                ,log_basic_headers(Basic)
                ]
               ).

log_basic_headers(#'P_basic'{headers=undefined}) -> <<"no-headers">>;
log_basic_headers(#'P_basic'{headers=Headers}) ->
    kz_binary:join([io_lib:format("~s = ~s", [K, kz_term:to_binary(V)]) || {K, _, V} <- Headers]).
