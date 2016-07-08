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

-export([start_link/1, start_link/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("../../ecallmgr/src/ecallmgr.hrl").

-define(RESPONDERS, [{'ecallmgr_fs_amqp_handler'
                     ,[{<<"*">>, <<"*">>}]
                     }
                    ]).
-define(BINDINGS(HN), [{'freeswitch', [{'restrict_to', ['key']}
                                      ,{'hostname', HN}
                                      ]}
                      ]).
-define(QUEUE_NAME(HN), <<"fs_amqp_shared_listener_", HN/binary>>).
-define(QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(CONSUME_OPTIONS, [{'exclusive', 'false'}]).

-define(SERVER, ?MODULE).


-record(state, {node :: atom()
               ,switch_url :: api_binary()
               ,switch_uri :: api_binary()
               ,switch_info = 'false' :: boolean()
               }).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(atom()) -> startlink_ret().
-spec start_link(atom(), kz_proplist()) -> startlink_ret().

start_link(Node) ->
    start_link(Node, []).

start_link(Node, Options) ->
    gen_listener:start_link(?MODULE, [], [Node, Options]).


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
init([Node, _Options]) ->
    put('callid', ?LOG_SYSTEM_ID),
    lager:debug("starting new fs amqp handler"),
    gen_server:cast(self(), 'check_sip_url'),
    {'ok', #state{node=Node}}.

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
                                   ,switch_info='false'
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
                     ,{'bindings', ?BINDINGS(Nodename)}
                     ,{'queue_name', ?QUEUE_NAME(Nodename)}
                     ,{'queue_options', ?QUEUE_OPTIONS}
                     ,{'consume_options', ?CONSUME_OPTIONS}
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
handle_cast({'gen_listener',{'is_consuming',IsConsuming}}, #state{node=Node}=State) ->
    case channel_pid(Node) of
        'undefined' -> lager:warning("channel server for node ~s not found", [Node]);
        Pid -> Pid ! {'option', <<"Publish-Channel-State">>, not IsConsuming}
    end,
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
handle_event(_JObj, #state{node=FSNode
                          ,switch_url=SwitchURL
                          ,switch_uri=SwitchURI
                          }) ->
    {'reply', [{'FSNode', FSNode}
              ,{'Switch-URL', SwitchURL}
              ,{'Switch-URI', SwitchURI}
              ]}.

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

-spec channel_pid(atom()) -> api_pid().
channel_pid(Node) ->
    ecallmgr_fs_node_sup:channel_srv(ecallmgr_fs_sup:find_node(Node)).
