%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2015, 2600Hz INC
%%% @doc
%%%
%%%
%%% @end
%%% @contributors
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_amqp_listener).

-behaviour(gen_listener).

-export([start_link/3]).

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

-define(SYSTEM_ALERT, "AMQP Profile ~s ~s in node ~s from ~s").

-define(RESPONDERS, [{'ecallmgr_fs_amqp_handler'
                     ,[{<<"*">>, <<"*">>}]
                     }
                    ]).
-define(BINDINGS(HN, P), [{'freeswitch', [{'restrict_to', ['key']}
                                         ,{'hostname', HN}
                                         ,{'profile', P}
                                         ]}
                      ]).
-define(QUEUE_NAME(HN, P), <<"fs_amqp_", P/binary, "_shared_listener_", HN/binary>>).
-define(QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(CONSUME_OPTIONS, [{'exclusive', 'false'}]).

-define(SERVER, ?MODULE).

-define(FREESWITCH_HEARTBEAT, 30).
-define(FUDGE_HEARTBEAT, 15).
-define(HEARTBEAT_TIMER_MS, 15 * ?MILLISECONDS_IN_SECOND).
-define(AMQP_HEARTBEAT, ?FUDGE_HEARTBEAT + ?FREESWITCH_HEARTBEAT).
-define(HEARTBEAT_MAX_ELAPSED_MS, ?AMQP_HEARTBEAT * ?MILLISECONDS_IN_SECOND).

-record(state, {node :: atom()
               ,switch_url :: api_binary()
               ,switch_uri :: api_binary()
               ,switch_info = 'false' :: boolean()
               ,options :: kz_proplist()
               ,profile :: ne_binary()
               ,events :: ne_binaries()
               ,timer = 'undefined' :: timer:tref() | 'undefined'
               ,heartbeat = 0 :: integer()
               ,active = 'false' :: boolean()
               ,configuration :: kz_json:object()
               }).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(atom(), kz_proplist(), ne_binary()) -> startlink_ret().
start_link(Node, Options, Profile) ->
    gen_listener:start_link(?MODULE, [], [Node, Options, Profile]).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {'ok', State} |
%%                     {'ok', State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Node, Options, Profile]) ->
    put('callid', ?LOG_SYSTEM_ID),
    lager:debug("starting new fs amqp handler"),
    Producers = ecallmgr_fs_amqp:amqp_producers(),
    Configuration = kz_json:get_value([<<"producers">>, Profile], Producers),
    Events = kz_json:get_list_value(<<"events">>, Configuration, []),
    gen_server:cast(self(), 'check_sip_url'),
    {'ok', #state{node=Node
                 ,options=Options
                 ,profile=Profile
                 ,configuration=Configuration
                 ,events=Events
                 }}.

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
handle_cast('check_sip_url', #state{node=Node
                                   ,profile=Profile
                                   ,switch_info='false'
                                   ,configuration=Configuration
                                   }=State) ->
    try ecallmgr_fs_nodes:sip_url(Node) of
        'undefined' ->
            lager:debug("no sip url available yet for ~s", [Node]),
            timer:sleep(2000),
            gen_server:cast(self(), 'check_sip_url'),
            {'noreply', State};
        SwitchURL ->
            [_, SwitchURIHost] = binary:split(SwitchURL, <<"@">>),
            SwitchURI = <<"sip:", SwitchURIHost/binary>>,
            Nodename = ecallmgr_fs_node:hostname(Node),
            Params = [{'responders', ?RESPONDERS}
                     ,{'bindings', ?BINDINGS(Nodename, Profile)}
                     ,{'queue_name', kz_json:get_binary_value(<<"queue_name">>, Configuration, ?QUEUE_NAME(Nodename, Profile))}
                     ,{'queue_options', kz_json:get_list_value(<<"queue_options">>, Configuration, ?QUEUE_OPTIONS)}
                     ,{'consume_options', kz_json:get_list_value(<<"consume_options">>, Configuration, ?CONSUME_OPTIONS)}
                     ],
            gen_listener:start_listener(self(), Params),
            {'noreply', State#state{switch_uri=SwitchURI
                                   ,switch_url=SwitchURL
                                   ,switch_info='true'
                                   }}
    catch
        _E:_R ->
            lager:warning("failed to check sip_url for node ~s : ~p : ~p", [Node, _E, _R]),
            timer:sleep(2000),
            gen_server:cast(self(), 'check_sip_url')
    end;
handle_cast({'gen_listener',{'is_consuming', 'false'}}, #state{}=State) ->
    _ = notify_procs('false', State),
    {'noreply', State#state{active='false'}};
handle_cast({'gen_listener',{'is_consuming', 'true'}}, #state{}=State) ->
    {'noreply', State};
handle_cast({'gen_listener',{'created_queue', _Q}}, #state{}=State) ->
    _ = timer:send_interval(?HEARTBEAT_TIMER_MS, 'check_heartbeat'),
    {'noreply', State};
handle_cast(_Cast, State) ->
    lager:debug("unhandled cast: ~p", [_Cast]),
    {'noreply', State, 'hibernate'}.

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
handle_info('check_heartbeat', State) ->
    {'noreply', check_elapsed(State)};
handle_info(_Info, State) ->
    lager:debug("unhandled message: ~p", [_Info]),
    {'noreply', State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Allows listener to pass options to handlers
%%
%% @spec handle_event(JObj, State) -> {reply, Options}
%% @end
%%--------------------------------------------------------------------
handle_event(JObj, #state{node=FSNode
                         ,switch_url=SwitchURL
                         ,switch_uri=SwitchURI
                         }=State) ->
    case kz_api:event_name(JObj) =:= <<"HEARTBEAT">> of
        'true' -> {'ignore', handle_heartbeat(State)};
        'false' -> {'reply', [{'FSNode', FSNode}
                             ,{'Switch-URL', SwitchURL}
                             ,{'Switch-URI', SwitchURI}
                             ]}
    end.

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
    lager:debug("fs amqp listener termination: ~p", [ _Reason]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {'ok', NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

-spec notify_procs(boolean(), state()) -> any().
notify_procs(IsConsuming, #state{node=Node}) ->
    [notify_proc(IsConsuming, Pid) || Pid <- gproc:lookup_pids({'p', 'l', ?FS_OPTION_MSG(Node)})].

-spec notify_proc(boolean(), pid()) -> any().
notify_proc(IsConsuming, Pid) ->
    Pid ! {'option', <<"Publish-Channel-State">>, not IsConsuming}.

-spec handle_heartbeat(state()) -> state().
handle_heartbeat(#state{active='true', profile=_Profile}=State) ->
    State#state{heartbeat=kz_util:current_tstamp()};
handle_heartbeat(#state{active='false', profile=Profile, node=Node, heartbeat=Heartbeat}=State) ->
    lager:debug("heartbeat for inactive profile ~s, activating", [Profile]),
    _ = notify_procs('true', State),
    _ = Heartbeat =/= 0 andalso
            kz_notify:system_alert(?SYSTEM_ALERT, [Profile, "activated", node(), Node]),
    State#state{active='true', heartbeat=kz_util:current_tstamp()}.

-spec check_elapsed(state()) -> state().
check_elapsed(#state{heartbeat=0} = State) -> State;
check_elapsed(#state{active='true', heartbeat=Heartbeat, node=Node, profile=Profile} = State) ->
    Nodes = kz_nodes:whapp_count('ecallmgr'),
    case kz_util:elapsed_ms(Heartbeat) > Nodes *  (?FREESWITCH_HEARTBEAT + ?HEARTBEAT_MAX_ELAPSED_MS) of
        'true' ->
            lager:debug(?SYSTEM_ALERT, [Profile, "deactivated", node(), Node]),
            kz_notify:system_alert(?SYSTEM_ALERT, [Profile, "deactivated", node(), Node]),
            _ = notify_procs('false', State),
            State#state{active='false'};
        'false' -> State
    end;
check_elapsed(#state{active='false'} = State) ->
    State.
