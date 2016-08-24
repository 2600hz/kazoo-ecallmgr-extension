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
                  ,{'metaflow', [{restrict_to, ['action']}]}
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
    props:to_log(Props, <<"MISSING PROPS">>),
    Number = metaflow_number(props:get_value(?PROP_MATCHING_DIGITS, Props)),
    TargetLeg = props:get_value(?GET_VAR(?METAFLOW_TARGET_VAR), Props),
    Direction = case TargetLeg of
                    <<"self">> -> <<"outbound">>;
                    <<"peer">> -> <<"inbound">>
                end,
    CRHs = [{<<"Metaflow-Request">>, Number}
           ,{<<"Control-Queue">>, props:get_value(<<"Control-Queue">>, Props)}
           ],
    AddProps = props:filter_undefined(
                 [{<<"Resource-Type">>,<<"metaflow">>}
                 ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
                 ,{<<"Route-Resp-Xml-Fun">>, fun route_resp_xml/4 }
                 ,{<<"Application-Logical-Direction">>, Direction}
                 ,{<<"Hunt-Destination-Number">>, Number}

                 ]),
    props:set_values(AddProps, Props).

-spec metaflow_number(binary()) -> binary().
metaflow_number(<<"*", Number/binary>>) -> Number;
metaflow_number(Number) -> Number.

-spec process_route_req(atom(), atom(), ne_binary(), ne_binary(), kz_proplist()) -> 'ok'.
process_route_req(Section, Node, FetchId, CallId, Props) ->
    kz_util:put_callid(CallId),
    do_process_route_req(Section, Node, FetchId, CallId, init_metaflow_props(Node, Props)).

-spec do_process_route_req(atom(), atom(), ne_binary(), ne_binary(), kz_proplist()) -> 'ok'.
do_process_route_req(Section, Node, FetchId, CallId, Props) ->
    case ecallmgr_fs_router_util:search_for_route(Section, Node, FetchId, CallId, Props, 'false') of
    'ok' ->
            lager:debug("xml fetch metaplan ~s finished without success", [FetchId]);
    {'ok', _JObj} ->
            lager:debug("xml fetch metaplan ~s finished with success", [FetchId])
    end.

-spec route_resp_xml(ne_binary(), kz_json:objects(), kz_json:object(), kz_proplist()) -> {'ok', iolist()}.
route_resp_xml(<<"application">>, _Routes, JObj, Props) ->
    Node = props:get_value(<<"FreeSWITCH-Node">>, Props),
%%    Apps = app_data(JObj),
    Cmd = kz_json:get_value(<<"Application-Data">>, JObj),
    DP = handle_application(Cmd, Node),
    
    Actions = [action_el(App, AppArg) || {App, AppArg} <- DP],
    Exten = [route_resp_log_winning_node()
%              | route_resp_ccvs(JObj) ++ Actions
            ] ++ Actions,
    ParkExtEl = extension_el(<<"metaflow">>, 'undefined', [condition_el(Exten)]),
    Context = context(JObj, Props),
    ContextEl = context_el(Context, [ParkExtEl]),
    SectionEl = section_el(<<"dialplan">>, <<"Metaflow Application Response">>, ContextEl),
    {'ok', xmerl:export([SectionEl], 'fs_xml')}.

-spec route_resp_log_winning_node() -> xml_el().
route_resp_log_winning_node() ->
    action_el(<<"log">>, [<<"NOTICE log|${uuid}|", (kz_util:to_binary(node()))/binary, " won metaflow control">>]).


handle_application('undefined', _Node) -> [];
handle_application(JObj, Node) ->
    case kz_json:get_value(<<"Application-Name">>, JObj) of
        <<"queue">> ->
            'true' = kapi_dialplan:queue_v(JObj),
            Commands = kz_json:get_value(<<"Commands">>, JObj, []),
            DefJObj = kz_json:from_list(kz_api:extract_defaults(JObj)),
            handle_queue_commands(Commands, DefJObj, Node, []);
        _AName -> control_process('fetch_dialplan', JObj, Node)
    end.

handle_queue_commands([], _, _Node, DP) -> DP;
handle_queue_commands([Command|Commands], DefJObj, Node, DP) ->
    case kz_json:is_empty(Command)
        orelse kz_json:get_ne_value(<<"Application-Name">>, Command) =:= 'undefined'
    of
        'true' -> handle_queue_commands(Commands, DefJObj, Node, DP);
        'false' ->
            JObj = kz_json:merge_jobjs(Command, DefJObj),
            'true' = kapi_dialplan:v(JObj),
            Cmd = control_process('fetch_dialplan', JObj, Node),
            handle_queue_commands(Commands, DefJObj, Node, DP ++ Cmd)
    end.

handle_event({<<"call">>, <<"command">>}, JObj, #state{node=Node}) ->
    kz_util:spawn(fun handle_call_command/2, [JObj, Node]),
    'ignore';
handle_event(_, _, _) ->
    'ignore'.

-spec handle_call_command(kz_json:object(), atom()) -> 'ok'.
handle_call_command(JObj, Node) ->
    case kz_json:get_value(<<"Application-Name">>, JObj) of
        <<"queue">> ->
            'true' = kapi_dialplan:queue_v(JObj),
            Commands = kz_json:get_value(<<"Commands">>, JObj, []),
            DefJObj = kz_json:from_list(kz_api:extract_defaults(JObj)),
            DP = handle_queue_commands(Commands, DefJObj, Node, []),
            UUID = kz_json:get_value(<<"Call-ID">>, JObj),
            exec_dialplan(Node, UUID, DP);
        _AName -> control_process('exec_cmd', JObj, Node)
    end.

-spec control_process(atom(), kz_json:object(), atom()) -> 'ok'.
control_process(Fun, Cmd, Node) ->
    kz_util:put_callid(Cmd),
    Category = kz_api:event_category(Cmd),
    Event = kz_api:event_name(Cmd),

    lager:debug("executing ~s ~s '~s' ~s"
               ,[Category
                ,Event
                ,kz_json:get_value(<<"Application-Name">>, Cmd)
                ,kz_json:get_value(<<"Msg-ID">>, Cmd, <<>>)
                ]),
    CallId = kz_json:get_value(<<"Call-ID">>, Cmd),
    Mod = get_module(Category, Event),
    try Mod:Fun(Node, CallId, Cmd, self())
    catch
        _:{'error', 'nosession'} ->
            lager:debug("unable to execute command, no session");
        'error':{'badmatch', {'error', 'nosession'}} ->
            lager:debug("unable to execute command, no session");
        'error':{'badmatch', {'error', ErrMsg}} ->
            ST = erlang:get_stacktrace(),
            lager:debug("invalid command ~s: ~p", [kz_json:get_value(<<"Application-Name">>, Cmd), ErrMsg]),
            kz_util:log_stacktrace(ST);
        'throw':{'msg', ErrMsg} ->
            lager:debug("error while executing command ~s: ~s", [kz_json:get_value(<<"Application-Name">>, Cmd), ErrMsg]);
        'throw':Msg ->
            lager:debug("failed to execute ~s: ~s", [kz_json:get_value(<<"Application-Name">>, Cmd), Msg]);
        _A:_B ->
            ST = erlang:get_stacktrace(),
            lager:debug("exception (~s) while executing ~s: ~p", [_A, kz_json:get_value(<<"Application-Name">>, Cmd), _B]),
            kz_util:log_stacktrace(ST)
    end.

exec_dialplan(Node, UUID, DP) ->
    [ecallmgr_util:send_cmd(Node, UUID, AppName, AppData) || {AppName, AppData} <- DP].

-spec get_module(ne_binary(), ne_binary()) -> atom().
get_module(Category, Name) ->
    ModuleName = <<"ecallmgr_", Category/binary, "_", Name/binary>>,
    try kz_util:to_atom(ModuleName)
    catch
        'error':'badarg' ->
            kz_util:to_atom(ModuleName, 'true')
    end.

%% -spec route_resp_ccvs(kz_json:object()) -> xml_els().
%% route_resp_ccvs(JObj) ->
%%     case kz_json:get_value(<<"Custom-Channel-Vars">>, JObj) of
%%         'undefined' -> [];
%%         CCVs -> [action_el(<<"kz_multiset">>, route_ccvs_list(kz_json:to_proplist(CCVs)) )]
%%     end.
%% 
%% -spec route_ccvs_list(kz_proplist()) -> ne_binary().
%% route_ccvs_list(CCVs) ->
%%     L = [kz_util:to_list(ecallmgr_util:get_fs_kv(K, V))
%%          || {K, V} <- CCVs
%%         ],
%%     <<"^^;", (kz_util:to_binary(string:join(L, ";")))/binary>>.
