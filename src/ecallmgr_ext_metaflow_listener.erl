%%%-------------------------------------------------------------------
%%% @copyright (C) 2014, 2600Hz
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_ext_metaflow_listener).

-behaviour(gen_listener).

-export([start_link/0
        ]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr-extension.hrl").
-include("gen_listener_spec.hrl").

-define(SERVER, ?MODULE).

-record(state, {}).

%% By convention, we put the options here in macros, but not required.
-define(BINDINGS, [{'dialplan', ['metaflow']}
                  ]).
-define(RESPONDERS, [{fun handle_metaflow/2
                     ,[{<<"call">>, <<"command">>}]
                     }
                    ]).
-define(QUEUE_NAME, <<>>).
-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).

-type fetch_resp() :: kz_json:object() |
                      kz_proplist() |
                      channel().

-type channel_fetch_reply() ::  {'ok', fetch_resp()} |          
                                {'error', 'not_found'}.

-type dialplan_action() :: {binary(), iolist() | binary() | list()}.
-type dialplan() :: [dialplan_action()].

-define(METAFLOW_PLAN_ACTION, <<"exec:execute_extension,METAFLOW_ROUTE_REQ XML metaflow">>).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Starts the server
%%--------------------------------------------------------------------
-spec start_link() -> startlink_ret().
start_link() ->
    gen_listener:start_link(?SERVER, [{'bindings', ?BINDINGS}
                                     ,{'responders', ?RESPONDERS}
                                     ,{'queue_name', ?QUEUE_NAME}       % optional to include
                                     ,{'queue_options', ?QUEUE_OPTIONS} % optional to include
                                     ,{'consume_options', ?CONSUME_OPTIONS} % optional to include
                                     ], []).

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
init([]) ->
    {'ok', #state{}}.

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
handle_cast({'gen_listener', {'created_queue', _QueueNAme}}, State) ->
    {'noreply', State};
handle_cast({'gen_listener', {'is_consuming', _IsConsuming}}, State) ->
    {'noreply', State};
handle_cast(_Msg, State) ->
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
handle_info(_Info, State) ->
    {'noreply', State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Allows listener to pass options to handlers
%%
%% @spec handle_event(JObj, State) -> {reply, Options}
%% @end
%%--------------------------------------------------------------------
handle_event(_JObj, _State) ->
    {'reply', []}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    lager:debug("listener terminating: ~p", [_Reason]).

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
-spec handle_metaflow(kz_json:object(), kz_proplist()) -> no_return().
handle_metaflow(JObj, _Props) ->
    'true' = kapi_dialplan:metaflow_v(JObj),
    UUID = metaflow_callid(JObj),
    kz_util:put_callid(UUID),
    maybe_process_metaflow(UUID, JObj, ecallmgr_fs_channel:fetch(UUID)).

-spec maybe_process_metaflow(api_binary(), kz_json:object(), channel_fetch_reply()) -> no_return().
maybe_process_metaflow(_UUID, JObj, {'error', Error}) ->
    lager:debug("channel for metaflow metaflow not found (~p): ~p", [Error, JObj]);
maybe_process_metaflow(UUID, JObj, {'ok', Channel}) ->
    lager:debug("channel for metaflow metaflow found : ~p : ~p", [Channel, JObj]),
    maybe_local_channel(UUID, JObj, Channel, kz_json:is_true(<<"handling_locally">>, Channel, 'false')).

-spec maybe_local_channel(api_binary(), kz_json:object(), kz_json:object(), boolean()) -> no_return().
maybe_local_channel(_UUID, _JObj, _Channel, 'false') ->
    lager:debug("channel for metaflow not handled locally");
maybe_local_channel(UUID, JObj, Channel, 'true') ->
    lager:debug("channel for metaflow handled locally, processing"),
    process_metaflow(UUID, JObj, Channel).


-spec process_metaflow(api_binary(), kz_json:object(), kz_json:object()) -> no_return().
process_metaflow(UUID, JObj, Channel) ->
    Node = kz_json:get_atom_value(<<"switch_nodename">>, Channel),
    EndpointId = kz_json:get_value(<<"Endpoint-ID">>, JObj, kz_util:rand_hex_binary(16)),
    _ListenOn = kz_json:get_value(<<"Listen-On">>, JObj, <<"self">>),
    BindingDigit = kz_json:get_value(<<"Binding-Digit">>, JObj, <<"*">>),
    CollectTimeout = kz_json:get_integer_value(<<"Collect-Timeout">>, JObj, 15000),
    Patterns = kz_json:get_value(<<"Patterns">>, JObj, kz_json:new()),
    Numbers = kz_json:get_value(<<"Numbers">>, JObj, kz_json:new()),
    TargetLeg = kz_json:get_value(<<"Target-Leg">>, JObj, <<"self">>),
    EventLeg = kz_json:get_value(<<"Event-Leg">>, JObj, <<"self">>),

    API = [clear_bind_digit_action()
	  ,bind_digit_input_timeout(CollectTimeout)
          ]
	++ [bind_digit_action(EndpointId
			     ,encode_pattern(BindingDigit, remove_start_anchor(Pattern))
			     ,?METAFLOW_PLAN_ACTION
			     ,TargetLeg
			     ,EventLeg
			     ) || Pattern <- kz_json:get_keys(Patterns)
	   ]
    ++ [bind_digit_action(EndpointId
                 ,encode_number(BindingDigit, Number)
                 ,?METAFLOW_PLAN_ACTION
                 ,TargetLeg
                 ,EventLeg
                 ) || Number <- kz_json:get_keys(Numbers)
       ]
	++ [bind_digit_action_dummy_regex(EndpointId)
	   ,digit_action_set_realm(EndpointId)
	   ],
    send_api(Node, UUID, API).

-spec send_api(atom(), ne_binary(), dialplan()) -> no_return().
send_api(Node, UUID, API) ->
    [ecallmgr_util:send_cmd(Node, UUID, AppName, kz_util:to_binary(AppData)) || {AppName, AppData} <- API].
    
   
-spec clear_bind_digit_action() -> dialplan_action().
clear_bind_digit_action() ->
    {<<"clear_digit_action">>, <<>>}.

-spec bind_digit_input_timeout(integer()) -> dialplan_action().
bind_digit_input_timeout(Timeout) ->
    {<<"set">>, ["bind_digit_input_timeout=", kz_util:to_binary(Timeout)]}.

-spec digit_action_set_realm(binary()) -> dialplan_action().
digit_action_set_realm(Realm) ->
    {<<"digit_action_set_realm">>, Realm}.

-spec bind_digit_action(binary(), binary(), binary(), binary(), binary()) -> dialplan_action().
bind_digit_action(Realm, Action, Plan, TargetLeg, EventLeg) ->
    {<<"bind_digit_action">>, [Realm, ",", Action, ",", Plan, ",", TargetLeg, ",", EventLeg]}.

-spec bind_digit_action_dummy_regex(binary()) -> dialplan_action().
bind_digit_action_dummy_regex(Realm) ->
    {<<"bind_digit_action">>, [Realm, ",'~^######$',exec:execute_extension,METAFLOW_DUMMY_REQ XML metaflow,self,self"]}.

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
    <<"'^", BindingDigit/binary, Number/binary, "$'">>.

-spec metaflow_callid(api_terms()) -> ne_binary().
metaflow_callid([_|_]=Props) ->
    case props:get_value(<<"Call-ID">>, Props) of
        'undefined' -> metaflow_callid(props:get_value(<<"Call">>, Props));
        CallId -> CallId
    end;
metaflow_callid(JObj) ->
    kz_json:get_first_defined([<<"Call-ID">>
                              ,[<<"Call">>, <<"Call-ID">>]
                              ], JObj).
