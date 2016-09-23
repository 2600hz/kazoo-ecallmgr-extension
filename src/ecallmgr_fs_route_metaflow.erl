%%%-------------------------------------------------------------------
%%% @copyright (C) 2011-2016, 2600Hz INC
%%% @doc
%%% Receive route(dialplan) requests from FS, request routes and respond
%%% @end
%%% @contributors
%%%   James Aimonetti
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_route_metaflow).

-behaviour(gen_listener).

-export([start_link/1, start_link/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_event/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-import(ecallmgr_fs_xml, [action_el/2
                         ,condition_el/1
                         ,context/2, context_el/2
                         ,extension_el/3
                         ,section_el/3
                         ]).

-include("ecallmgr-extension.hrl").
-include("gen_server_spec.hrl").

-define(SERVER, ?MODULE).
-define(DEFAULT_METAFLOW_CONTEXT, <<"metaflow">>).
-define(FETCH_SECTION, 'dialplan').
-define(BINDINGS_CFG_KEY, <<"metaflow_routing_bindings">>).
-define(DEFAULT_BINDINGS, [?DEFAULT_METAFLOW_CONTEXT]).

-record(state, {node = 'undefined' :: atom()
               ,options = [] :: kz_proplist()
               ,control_q :: api_binary()
               }).

-define(PROP_MATCHING_DIGITS, <<"variable_last_matching_digits">>).

-define(BINDINGS, [{'self', []}
                  ,{'dialplan', []}
                  ,{'metaflow', [{restrict_to, ['action', 'flow']}]}
                  ]).
-define(RESPONDERS, []).
-define(QUEUE_NAME, <<>>).
-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Starts the server
%%--------------------------------------------------------------------
-spec start_link(atom()) -> startlink_ret().
-spec start_link(atom(), kz_proplist()) -> startlink_ret().
start_link(Node) -> start_link(Node, []).
start_link(Node, Options) ->
    gen_listener:start_link(?SERVER, [{'responders', ?RESPONDERS}
                                     ,{'bindings', ?BINDINGS}
                                     ,{'queue_name', ?QUEUE_NAME}
                                     ,{'queue_options', ?QUEUE_OPTIONS}
                                     ,{'consume_options', ?CONSUME_OPTIONS}
                                     ],
                            [Node, Options]).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Node, Options]) ->
    kz_util:put_callid(Node),
    lager:info("starting new fs route metaflow listener for ~s", [Node]),
    gen_server:cast(self(), 'bind_to_metaflow'),
    {'ok', #state{node=Node, options=Options}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast('bind_to_metaflow', #state{node=Node}=State) ->
    Bindings = ecallmgr_config:get(?BINDINGS_CFG_KEY, ?DEFAULT_BINDINGS, Node),
    case ecallmgr_fs_router_util:register_bindings(Node, ?FETCH_SECTION, Bindings) of
        'true' -> {'noreply', State};
        'false' ->
            lager:critical("unable to establish route bindings : ~p", [Bindings]),
            {'stop', 'no_binding', State}
    end;
handle_cast({'gen_listener', {'created_queue', Q}}, State) ->
    {'noreply', State#state{control_q=Q}};
handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Allows listener to pass options to handlers
%%
%% @spec handle_event(JObj, State) -> {reply, Options}
%% @end
%%--------------------------------------------------------------------
handle_event(JObj, State) ->
    handle_event(kz_util:get_event_type(JObj), JObj, State).

handle_event({<<"call">>, <<"command">>}, JObj, #state{node=Node}) ->
    kz_util:spawn(fun ecallmgr_metaflow_control:handle_call_command/2, [JObj, Node]),
    'ignore';
handle_event({<<"metaflow">>, <<"action">>}, JObj, #state{node=Node
                                                         ,control_q=CtrlQ
                                                         }) ->
    kz_util:spawn(fun ecallmgr_metaflow_action:handle_metaflow_action/3, [JObj, CtrlQ, Node]),
    'ignore';
handle_event({<<"metaflow">>, <<"flow">>}, JObj, #state{node=Node
                                                       ,control_q=CtrlQ
                                                       }) ->
    kz_util:spawn(fun ecallmgr_metaflow_flow:handle_metaflow_flow/3, [JObj, CtrlQ, Node]),
    'ignore';
handle_event(_, _, _) ->
    'ignore'.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'route', Section, <<"REQUEST_PARAMS">>, _SubClass, <<"metaflow">>, FSId, CallId, FSData},
            #state{node=Node
                  ,control_q=CtrlQ
                  }=State) ->
    lager:info("processing metaflow fetch request ~s (call ~s) from ~s", [FSId, CallId, Node]),
    _ = kz_util:spawn(fun process_route_req/5, [Section, Node, FSId, CallId, [{<<"Control-Queue">>, CtrlQ} | FSData]]),
    {'noreply', State};
handle_info(_Other, State) ->
    lager:debug("unhandled msg: ~p", [_Other]),
    {'noreply', State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
                                                % terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{node=Node}) ->
    lager:info("route metaflow listener for ~s terminating: ~p", [Node, _Reason]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_metaflow_props(atom(), kz_proplist()) -> kz_proplist().
init_metaflow_props(Node, Props) ->
    Routines = [fun add_metaflow_missing_props/1
               ],
    lists:foldl(fun(F,P) -> F(P) end, [{<<"FreeSWITCH-Node">>, Node} | Props], Routines).

-spec add_metaflow_missing_props(kz_proplist()) -> kz_proplist().
add_metaflow_missing_props(Props) ->
    Number = metaflow_number(props:get_value(?PROP_MATCHING_DIGITS, Props)),
    TargetLeg = props:get_value(?GET_VAR(?METAFLOW_TARGET_VAR), Props),
    Direction = case TargetLeg of
                    <<"self">> -> <<"outbound">>;
                    <<"peer">> -> <<"inbound">>
                end,
    CRHs = [{<<"Metaflow-Request-Type">>, <<"in-call">>}
           ,{<<"Metaflow-Request">>, Number}
           ,{<<"Other-Leg-Call-ID">>, kzd_freeswitch:other_leg_call_id(Props)}
           ],
    AddProps = props:filter_undefined(
                 [{<<"Resource-Type">>,<<"metaflow">>}
                 ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
                 ,{<<"Route-Resp-Xml-Fun">>, fun route_resp_xml/4}
                 ,{<<"Application-Logical-Direction">>, Direction}
                 ,{<<"Hunt-Destination-Number">>, Number}
                 ]),
    %% TODO
    %% override request to simplify things and handling on konami-pro ?
    props:set_values(AddProps, Props).

-spec metaflow_number(binary()) -> binary().
metaflow_number(<<"*", Number/binary>>) -> Number;
metaflow_number(Number) -> Number.

-spec process_route_req(atom(), atom(), ne_binary(), ne_binary(), kz_proplist()) -> 'ok'.
process_route_req(Section, Node, FetchId, UUID, FSProps) ->
    kz_util:put_callid(UUID),
    Props = init_metaflow_props(Node, FSProps),
    case ecallmgr_fs_router_util:search_for_route(Section, Node, FetchId, UUID, Props, 'false') of
        'ok' -> lager:debug("xml fetch metaplan ~s finished without success", [FetchId]);
        {'ok', JObj} -> start_metaflow_handling(Node, FetchId, UUID, JObj, Props)
    end.

-spec start_metaflow_handling(atom(), ne_binary(), ne_binary(), kz_json:object(), kz_proplist()) -> 'ok'.
start_metaflow_handling(_Node, FetchId, CallId, JObj, Props) ->
    ControlQ = props:get_value(<<"Control-Queue">>, Props),
    CCVs = [{[<<"Custom-Channel-Vars">>, <<"Application-Name">>], kz_json:get_value(<<"App-Name">>, JObj)}
           ,{[<<"Custom-Channel-Vars">>, <<"Application-Node">>], kz_json:get_value(<<"Node">>, JObj)}
           ],
    send_metaflow_win(ControlQ, FetchId, CallId, kz_json:set_values(CCVs, JObj)).

-spec send_metaflow_win(ne_binary(), ne_binary(), ne_binary(), kz_json:object()) -> 'ok'.
send_metaflow_win(ControlQ, FetchId, CallId, JObj) ->
    ControllerQ = kz_api:server_id(JObj),
    CCVs = kz_json:get_value(<<"Custom-Channel-Vars">>, JObj),
    Win = [{<<"Msg-ID">>, FetchId}
          ,{<<"Call-ID">>, CallId}
          ,{<<"Control-Queue">>, ControlQ}
          ,{<<"Custom-Channel-Vars">>, CCVs}
           | kz_api:default_headers(ControlQ, <<"dialplan">>, <<"route_win">>, ?APP_NAME, ?APP_VERSION)
          ],
    lager:debug("sending metaflow route_win to ~s", [ControllerQ]),
    Publisher = fun(API) -> kapi_route:publish_win(ControllerQ, API) end,
    kz_amqp_worker:cast(Win, Publisher).

-spec route_resp_xml(ne_binary(), kz_json:objects(), kz_json:object(), kz_proplist()) -> {'ok', iolist()}.
route_resp_xml(<<"application">>, _Routes, JObj, Props) ->
    Node = props:get_value(<<"FreeSWITCH-Node">>, Props),
    UUID = props:get_value(<<"Unique-UUID">>, Props),
    Exten = [route_resp_log_winning_node()
             | route_resp_ccvs(Node, UUID, JObj)
            ],
    ParkExtEl = extension_el(<<"metaflow">>, 'undefined', [condition_el(Exten)]),
    Context = context(JObj, Props),
    ContextEl = context_el(Context, [ParkExtEl]),
    SectionEl = section_el(<<"dialplan">>, <<"Metaflow Application Response">>, ContextEl),
    {'ok', xmerl:export([SectionEl], 'fs_xml')}.

-spec route_resp_log_winning_node() -> xml_el().
route_resp_log_winning_node() ->
    action_el(<<"log">>, [<<"NOTICE log|${uuid}|", (kz_util:to_binary(node()))/binary, " won metaflow control">>]).

-spec route_resp_ccvs(atom(), ne_binary(), kz_json:object()) -> xml_els().
route_resp_ccvs(Node, UUID, JObj) ->
    case kz_json:get_value(<<"Custom-Channel-Vars">>, JObj) of
        'undefined' -> [];
        CCVs -> [action_el(<<"kz_multiset">>, ecallmgr_util:multi_set_args(Node, UUID, kz_json:to_proplist(CCVs)))]
    end.
