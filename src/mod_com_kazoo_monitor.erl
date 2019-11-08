%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_monitor).

-behaviour(gen_listener).

-export([start_link/0]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr_extension.hrl").

-define(CHECK_INTERVAL, 2000).
-define(NODE_EXPIRATION, 25000).

-define(RESPONDERS, []).
-define(BINDINGS, [{'freeswitch', [{'restrict_to', ['events']}
                                  ,{'events', ['HEARTBEAT']}
                                  ]
                   }
                  ]).
-define(QUEUE_NAME, <<>>).
-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).

-define(SERVER, ?MODULE).

-type state() :: map().

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    gen_listener:start_link({'local', ?SERVER}, ?MODULE
                           , [{'responders', ?RESPONDERS}
                             ,{'bindings', ?BINDINGS}
                             ,{'queue_name', ?QUEUE_NAME}
                             ,{'queue_options', ?QUEUE_OPTIONS}
                             ,{'consume_options', ?CONSUME_OPTIONS}
                             ], []).


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server
%%
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> {'ok', state()}.
init([]) ->
    kz_log:put_callid(?MODULE),
    lager:info("starting new fs amqp monitor"),
    {'ok', #{nodes => #{}}, ?CHECK_INTERVAL}.

-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'gen_listener',{'is_consuming', 'false'}}, State) ->
    {'noreply', State#{active => 'false'}, ?CHECK_INTERVAL};
handle_cast({'gen_listener',{'is_consuming', 'true'}}, State) ->
    {'noreply', State#{active => 'true'}, ?CHECK_INTERVAL};
handle_cast({'gen_listener',{'created_queue', _Q}}, State) ->
    {'noreply', State, ?CHECK_INTERVAL};
handle_cast(_Cast, State) ->
    lager:debug("unhandled cast: ~p", [_Cast]),
    {'noreply', State, ?CHECK_INTERVAL}.

-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info(timeout, #{active := 'true'} = State) ->
    {'noreply', handle_timeout(State), ?CHECK_INTERVAL};
handle_info(timeout, State) ->
    {'noreply', State, ?CHECK_INTERVAL};
handle_info(_Info, State) ->
    lager:debug("unhandled message: ~p", [_Info]),
    {'noreply', State, ?CHECK_INTERVAL}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), state()) -> gen_listener:handle_event_return().
handle_event(JObj, State) ->
    {'ignore', handle_hearbeat(JObj, State)}.

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
    lager:debug("fs amqp monitor termination: ~p", [ _Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed
%%
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

-spec node_name(kz_json:object()) -> atom().
node_name(JObj) ->
    kz_term:to_atom(kz_json:get_value(<<"Node">>, JObj), 'true').

-spec core_uuid(kz_json:object()) -> atom().
core_uuid(JObj) ->
    kz_term:to_atom(kz_json:get_value(<<"Core-UUID">>, JObj), 'true').

-spec handle_hearbeat(kz_json:object(), state()) -> state().
handle_hearbeat(JObj, #{nodes := Nodes} = Map) ->
    CoreUUID = core_uuid(JObj),
    Nodename = node_name(JObj),
    INU = ecallmgr_fs_nodes:is_node_up(Nodename),
    case Map of
        #{nodes := #{CoreUUID := #{state := up}=Node}} when INU ->
            maybe_update_code(Nodes),
            Map#{nodes => Nodes#{CoreUUID => Node#{time => kz_time:now_ms()}}};
        #{nodes := #{CoreUUID := #{state := up}=Node}} ->
            handle_nodeup(CoreUUID, Node, Map);
        #{nodes := #{CoreUUID := #{state := expired}}=Node} ->
            handle_nodeup(CoreUUID, Node, Map);
        #{nodes := #{CoreUUID := #{state := down}}=Node} ->
            handle_nodeup(CoreUUID, Node, Map);
        Map ->
            handle_nodeup(CoreUUID, #{name => Nodename}, Map)
    end.

-spec handle_nodeup(atom(), map(), state()) -> state().
handle_nodeup(CoreUUID, #{name := Name} = Node, #{nodes := Nodes}=State) ->
    NewNodes = Nodes#{CoreUUID => Node#{state => up, time => kz_time:now_ms()}},
    _ = update_code(NewNodes),
    _ = case ecallmgr_fs_nodes:is_node(Name) of
            'true' -> ecallmgr_fs_nodes:nodeup(Name, 'heartbeat');
            'false' -> ecallmgr_fs_nodes:add(Name, no_cookie, [{connect_strategy, 'heartbeat'}])
        end,
    State#{nodes => NewNodes}.


-spec handle_nodedown(atom(), map()) -> any().
handle_nodedown(CoreUUID, #{name := Name}) ->
    lager:critical("received node down notice for ~s", [CoreUUID]),
    _ = ecallmgr_fs_nodes:nodedown(Name).

check_node_expiration(_Key, #{time := Time}) ->
    kz_time:elapsed_ms(Time) > ?NODE_EXPIRATION;
check_node_expiration(_Key, _Val) ->
    false.

-spec handle_timeout(state()) -> state().
handle_timeout(#{nodes := Nodes} = State) ->
    Remove = maps:filter(fun check_node_expiration/2, Nodes),
    case maps:without(maps:keys(Remove), Nodes) of
        Nodes -> State;
        NewNodes ->
            _ = update_code(NewNodes),
            _ = [handle_nodedown(CoreUUID, Node) || {CoreUUID, Node} <- maps:to_list(Remove)],
            State#{nodes => NewNodes}
    end.

is_code_handled({CoreUUID, NodeName}) ->
    freeswitch:mod(NodeName) =/= 'mod_com_kazoo'
        orelse mod_com_kazoo:core_uuid(NodeName) =/= CoreUUID.

maybe_update_code(Nodes) ->
    Props = [{K, maps:get(name, V)} || {K,V} <- maps:to_list(Nodes)],
    case lists:any(fun is_code_handled/1, Props) of
        'true' -> lager:info("mod_com_kazoo code out of sync, updating."),
                  update_code(Nodes);
        'false' -> 'ok'
    end.

update_code(Nodes) ->
    Props = [{K, maps:get(name, V)} || {K,V} <- maps:to_list(Nodes)],
    FSCode = freeswitch_code(Props),
    meta:replace_function(mod, 1, FSCode, freeswitch),
    meta:replace_function(core_uuid, 1, mod_com_kazoo_code(Props), mod_com_kazoo).

freeswitch_code(Props) ->
    {function,1,mod,1,
     [{clause,1, [{atom,1, V}], [], [{atom,1,mod_com_kazoo}]} || {_K, V} <- Props]
     ++ [{clause,1, [{var,1,'_'}], [], [{atom,1,mod_kazoo}]}]
    }.

mod_com_kazoo_code(Props) ->
    {function,1,core_uuid,1,
     [{clause,1,[{atom,1,V}], [], [{atom,1,K}]} || {K,V} <- Props]
     ++ [{clause,1, [{var,1,'X'}], [[{call,1,{atom,1,is_atom},[{var,1,'X'}]}]], [{var,1,'X'}]}]
     ++ [{clause,1, [{var,1,'_'}], [], [{atom,1,'error_not_found'}]}]
    }.
