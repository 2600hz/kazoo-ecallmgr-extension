%%%-------------------------------------------------------------------
%%% @copyright (C) 2011-2016, 2600Hz INC
%%% @doc
%%% handles CHANNEL_BRIDGE
%%% requests metaflow bindings
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_metaflow).

-behaviour(gen_server).

-export([start_link/1, start_link/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr-extension.hrl").
-include("gen_server_spec.hrl").

-define(METAFLOW_PLAN_ACTION, <<"exec:execute_extension,METAFLOW_ROUTE_REQ XML metaflow">>).
-define(SERVER, ?MODULE).

-record(state, {node = 'undefined' :: atom()
               ,options = [] :: kz_proplist()
               }).

-type dialplan_action() :: {binary(), iolist() | binary() | list()}.
-type dialplan() :: [dialplan_action()].

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
    gen_server:cast(self(), 'bind_to_events'),
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
handle_cast('bind_to_events', #state{node=Node}=State) ->
    gproc:reg({'p', 'l', ?FS_EVENT_REG_MSG(Node, <<"CHANNEL_BRIDGE">>)}),
    {'noreply', State};
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
handle_info({'event', [UUID | Props]}, #state{node=Node}=State) ->
    _ = kz_util:spawn(fun process_event/3, [UUID, Props, Node]),
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
    lager:info("metaflow handler for ~s terminating: ~p", [Node, _Reason]).

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

-spec process_event(api_binary(), kz_proplist(), atom()) -> any().
process_event(UUID, Props, Node) ->
    kz_util:put_callid(UUID),
    EventName = props:get_value(<<"Event-Subclass">>, Props, props:get_value(<<"Event-Name">>, Props)),
    process_specific_event(EventName, UUID, Props, Node).

-spec process_specific_event(ne_binary(), api_binary(), kz_proplist(), atom()) -> any().
process_specific_event(<<"CHANNEL_BRIDGE">>, _UUID, Props, Node) ->
    request_metaflows(Node, Props);
process_specific_event(_Event, _UUID, _Props, _Node) ->
    lager:debug("event ~s for callid ~s not handled in metaflow (~s)", [_Event, _UUID, _Node]).

-spec request_metaflows(atom(), kz_proplist()) -> any().
request_metaflows(Node, Props) ->
    ALeg = props:get_value(<<"Bridge-A-Unique-ID">>, Props),
    AChannel = ecallmgr_fs_channel:fetch(ALeg, 'record'),
    kz_util:spawn(fun request_metaflow/3, [Node, <<"A">>, AChannel]),
    BLeg = props:get_value(<<"Bridge-B-Unique-ID">>, Props),
    BChannel = ecallmgr_fs_channel:fetch(BLeg, 'record'),
    kz_util:spawn(fun request_metaflow/3, [Node, <<"B">>, BChannel]).

-spec request_metaflow(atom(), ne_binary(), channel()) -> any().
request_metaflow(Node, Leg, {'ok', #channel{handling_locally='true'
                                           ,account_id=?NE_BINARY=AccountId
                                           ,uuid=UUID
                                           ,authorizing_id=AuthorizingId
                                           ,resource_id=ResourceId
                                           ,callflow_id=CallFlowId
                                           ,node=Node
                                           }=Channel
                            }) ->
    kz_util:put_callid(UUID),
    API = [{<<"Account-ID">>, AccountId}
          ,{<<"Binding-Leg">>, Leg}
          ,{<<"Call-ID">>, UUID}
          ,{<<"Authorizing-ID">>, AuthorizingId}
          ,{<<"Resource-ID">>, ResourceId}
          ,{<<"CallFlow-ID">>, CallFlowId}
          | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
          ],
    case kz_amqp_worker:call(API, fun kapi_metaflow:publish_bind_req/1, fun kapi_metaflow:binding_v/1) of
        {'ok', JObj} -> process_metaflow(Node, UUID, JObj, Channel);
        _Else -> lager:debug("error requesting metaflow binding : ~p", [_Else])
    end;       
request_metaflow(_Node, _Leg, _Channel) -> 'ok'.

-spec process_metaflow(atom(), api_binary(), kz_json:object(), channel()) -> no_return().
process_metaflow(Node, UUID, JObj, _Channel) ->
    lager:debug("metaflow binding received"),
    EndpointId = kz_json:get_value(<<"Endpoint-ID">>, JObj, kz_util:rand_hex_binary(16)),
    ListenOn = kz_json:get_value(<<"Listen-On">>, JObj, <<"self">>),
    BindingDigit = kz_json:get_value(<<"Binding-Digit">>, JObj, <<"*">>),
    CollectTimeout = kz_json:get_integer_value(<<"Collect-Timeout">>, JObj, 15000),
    Patterns = kz_json:get_value(<<"Patterns">>, JObj, kz_json:new()),
    Numbers = kz_json:get_value(<<"Numbers">>, JObj, kz_json:new()),
    TargetLeg = kz_json:get_value(<<"Target-Leg">>, JObj, ListenOn),
    EventLeg = kz_json:get_value(<<"Event-Leg">>, JObj, ListenOn),

    API = [clear_bind_digit_action(EndpointId)
           ,bind_digit_input_timeout(CollectTimeout)
          ]
    ++ [bind_digit_action(EndpointId
                 ,encode_number(BindingDigit, Number)
                 ,?METAFLOW_PLAN_ACTION
                 ,TargetLeg
                 ,EventLeg
                 ) || Number <- kz_json:get_keys(Numbers)
       ]
    ++ [bind_digit_action(EndpointId
                 ,encode_pattern(BindingDigit, remove_start_anchor(Pattern))
                 ,?METAFLOW_PLAN_ACTION
                 ,TargetLeg
                 ,EventLeg
                 ) || Pattern <- kz_json:get_keys(Patterns)
       ]
    ++ [bind_digit_action_dummy_regex(EndpointId, TargetLeg, EventLeg)
       ,digit_action_set_realm(EndpointId)
       ,metaflow_target(TargetLeg)
       ],

    case kz_json:get_keys(Patterns) =/= []
        orelse kz_json:get_keys(Numbers) =/= []
    of
        'true' -> send_api(Node, UUID, API);
        'false' -> 'ok'
    end.

-spec send_api(atom(), ne_binary(), dialplan()) -> no_return().
send_api(Node, UUID, API) ->
    [ecallmgr_util:send_cmd(Node, UUID, AppName, kz_util:to_binary(AppData)) || {AppName, AppData} <- API].

-spec metaflow_target(ne_binary()) -> dialplan_action().
metaflow_target(TargetLeg) ->
    {<<"set">>, [?METAFLOW_TARGET_VAR, "=", TargetLeg]}.

-spec clear_bind_digit_action(ne_binary()) -> dialplan_action().
clear_bind_digit_action(EndpointId) ->
    {<<"clear_digit_action">>, EndpointId}.

-spec bind_digit_input_timeout(integer()) -> dialplan_action().
bind_digit_input_timeout(Timeout) ->
    {<<"set">>, ["bind_digit_input_timeout=", kz_util:to_binary(Timeout)]}.

-spec digit_action_set_realm(binary()) -> dialplan_action().
digit_action_set_realm(Realm) ->
    {<<"digit_action_set_realm">>, Realm}.

-spec bind_digit_action(binary(), binary(), binary(), binary(), binary()) -> dialplan_action().
bind_digit_action(Realm, Action, Plan, TargetLeg, EventLeg) ->
    {<<"bind_digit_action">>, [Realm, ",", Action, ",", Plan, ",", TargetLeg, ",", EventLeg]}.

-spec bind_digit_action_dummy_regex(binary(), binary(), binary()) -> dialplan_action().
bind_digit_action_dummy_regex(Realm, TargetLeg, EventLeg) ->
    {<<"bind_digit_action">>, [Realm, ",'~^######$',exec:execute_extension,METAFLOW_DUMMY_REQ XML metaflow,", TargetLeg, ",", EventLeg]}.

-spec remove_start_anchor(binary()) -> binary().
remove_start_anchor(<<"^", Pattern/binary>>) -> Pattern;
remove_start_anchor(Pattern) -> Pattern.

-spec encode_pattern(binary(), binary()) -> binary().
encode_pattern(<<"*", _/binary>> = BindingDigit, Pattern) ->
    encode_pattern(<<"\\", BindingDigit/binary>>, Pattern);
encode_pattern(BindingDigit, Pattern) ->
    <<"'~^", BindingDigit/binary, Pattern/binary, "'">>.

-spec encode_number(binary(), binary()) -> binary().
encode_number(BindingDigit, Number) ->
    <<"'~^\\", BindingDigit/binary, Number/binary, "$'">>.
