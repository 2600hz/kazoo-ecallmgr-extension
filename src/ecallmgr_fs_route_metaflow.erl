%%%-------------------------------------------------------------------
%%% @copyright (C) 2011-2016, 2600Hz INC
%%% @doc
%%% Receive route(dialplan) requests from FS, request routes and respond
%%% @end
%%% @contributors
%%%   James Aimonetti
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_route_metaflow).

-behaviour(gen_server).

-export([start_link/1, start_link/2]).
-export([init/1
        ,handle_call/3
        ,handle_cast/2
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

-define(SERVER, ?MODULE).
-define(DEFAULT_METAFLOW_CONTEXT, <<"metaflow">>).
-define(FETCH_SECTION, 'dialplan').
-define(BINDINGS_CFG_KEY, <<"metaflow_routing_bindings">>).
-define(DEFAULT_BINDINGS, [?DEFAULT_METAFLOW_CONTEXT]).

-record(state, {node = 'undefined' :: atom()
               ,options = [] :: kz_proplist()
               }).

-define(PROP_MATCHING_DIGITS, <<"variable_last_matching_digits">>).

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
    gen_server:start_link(?SERVER, [Node, Options], []).

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
handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

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
handle_info({'route', Section, <<"REQUEST_PARAMS">>, _SubClass, <<"metaflow">>, FSId, CallId, FSData},#state{node=Node}=State) ->
    lager:info("processing metaflow fetch request ~s (call ~s) from ~s", [FSId, CallId, Node]),
    _ = kz_util:spawn(fun process_route_req/5, [Section, Node, FSId, CallId, FSData]),
    {'noreply', State, 'hibernate'};
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
    CRHs = [{<<"Metaflow-Request">>, metaflow_number(props:get_value(?PROP_MATCHING_DIGITS, Props))}],
    AddProps = props:filter_undefined(
                 [{<<"Resource-Type">>,<<"metaflow">>}
                 ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
                 ,{<<"Route-Resp-Xml-Fun">>, fun route_resp_xml/4 }
                 ]),
    props:insert_values(AddProps, Props).

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

route_resp_xml(<<"application">>, _Routes, JObj, Props) ->
    Apps = app_data(JObj),
    Node = props:get_value(<<"FreeSWITCH-Node">>, Props),
    UUID = kzd_freeswitch:call_id(Props),
    FSApps = apps_cmd(Node, UUID, Apps),
    Actions = [action_el(App, AppArg) || {App, AppArg} <- FSApps],
    Exten = [route_resp_log_winning_node()
              | route_resp_ccvs(JObj) ++ Actions
            ],
    ParkExtEl = extension_el(<<"metaflow">>, 'undefined', [condition_el(Exten)]),
    Context = context(JObj, Props),
    ContextEl = context_el(Context, [ParkExtEl]),
    SectionEl = section_el(<<"dialplan">>, <<"Metaflow Application Response">>, ContextEl),
    {'ok', xmerl:export([SectionEl], 'fs_xml')}.

-spec route_resp_log_winning_node() -> xml_el().
route_resp_log_winning_node() ->
    action_el(<<"log">>, [<<"NOTICE log|${uuid}|", (kz_util:to_binary(node()))/binary, " won metaflow control">>]).

common_headers(JObj) ->
    Headers = [?KEY_APP_NAME
              ,?KEY_APP_VERSION
              ,?KEY_MSG_ID
              ],
    [{Header, kz_json:get_value(Header, JObj)}|| Header <- Headers].

app_headers(JObj) ->
    props:filter_undefined(
      [{?KEY_EVENT_CATEGORY, <<"call">>}
      ,{?KEY_EVENT_NAME, <<"command">>}
      ,{<<"Custom-Channel-Vars">>, kz_json:get_value(<<"Custom-Channel-Vars">>, JObj)}
      | common_headers(JObj)
      ]).

app_data(JObj) ->
    Headers = app_headers(JObj),
    [kz_json:set_values(Headers, App) || App <- kz_json:get_value(<<"Application-Data">>, JObj)].

apps_cmd(Node, UUID, Apps) ->
    lists:foldl(fun(App, Acc) -> Acc ++ app_cmd(Node, UUID, App) end, [], Apps).

app_cmd(Node, UUID, JObj) ->
    AppName = kz_json:get_value(<<"Application-Name">>, JObj),
    case ecallmgr_call_command:get_fs_app(Node, UUID, JObj, AppName) of
        {'error', _Msg} -> [];
        {'return', _Result} -> [];
        {_AppName, 'noop'} -> [];
        {_AppName, _AppData, _NewNode} -> [];
        {_AppName, _AppData}=App -> [App];
        [_|_]=Apps -> Apps
    end.

-spec route_resp_ccvs(kz_json:object()) -> xml_els().
route_resp_ccvs(JObj) ->
    case kz_json:get_value(<<"Custom-Channel-Vars">>, JObj) of
        'undefined' -> [];
        CCVs -> [action_el(<<"kz_multiset">>, route_ccvs_list(kz_json:to_proplist(CCVs)) )]
    end.

-spec route_ccvs_list(kz_proplist()) -> ne_binary().
route_ccvs_list(CCVs) ->
    L = [kz_util:to_list(ecallmgr_util:get_fs_kv(K, V))
         || {K, V} <- CCVs
        ],
    <<"^^;", (kz_util:to_binary(string:join(L, ";")))/binary>>.
