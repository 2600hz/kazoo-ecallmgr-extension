%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2018, 2600Hz
%%% @doc Receive route(dialplan) requests from FS, request routes and respond
%%% @author James Aimonetti
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_metaflow).

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

-include("metaflow.hrl").

-define(SERVER, ?MODULE).
-define(DEFAULT_METAFLOW_CONTEXT, <<"metaflow">>).
-define(FETCH_SECTION, 'dialplan').
-define(BINDINGS_CFG_KEY, <<"metaflow_routing_bindings">>).
-define(DEFAULT_BINDINGS, [?DEFAULT_METAFLOW_CONTEXT]).

-record(state, {node = 'undefined' :: atom()
               ,options = [] :: kz_term:proplist()
               ,control_q :: kz_term:api_binary()
               }).

-type state() :: #state{}.

-define(PROP_MATCHING_DIGITS, <<"variable_last_matching_digits">>).

-define(BINDINGS, [{'self', []}
                  ,{'dialplan', []}
                  ,{'metaflow', [{restrict_to, ['action', 'flow']}, 'federate']}
                  ]).

-define(RESPONDERS, [{'metaflow_control'
                     ,[{<<"call">>, <<"command">>}]
                     }
                    ,{'metaflow_action'
                     ,[{<<"metaflow">>, <<"action">>}]
                     }
                    ,{'metaflow_flow'
                     ,[{<<"metaflow">>, <<"flow">>}]
                     }
                    ]).

-define(QUEUE_NAME, <<>>).
-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the server
%% @end
%%------------------------------------------------------------------------------

-spec start_link(atom()) -> kz_types:startlink_ret().
start_link(Node) -> start_link(Node, []).

-spec start_link(atom(), kz_term:proplist()) -> kz_types:startlink_ret().
start_link(Node, Options) ->
    gen_listener:start_link(?SERVER, [{'responders', ?RESPONDERS}
                                     ,{'bindings', ?BINDINGS}
                                     ,{'queue_name', ?QUEUE_NAME}
                                     ,{'queue_options', ?QUEUE_OPTIONS}
                                     ,{'consume_options', ?CONSUME_OPTIONS}
                                     ],
                            [Node, Options]).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server.
%% @end
%%------------------------------------------------------------------------------
-spec init([kz_term:api_atom() | kz_term:proplist()]) -> {'ok', state()}.
init([Node, Options]) ->
    kz_util:put_callid(Node),
    lager:info("starting new fs metaflow listener for ~s", [Node]),
    gen_server:cast(self(), 'bind_to_events'),
    gen_server:cast(self(), 'bind_to_metaflow'),
    {'ok', #state{node=Node, options=Options}}.

%%------------------------------------------------------------------------------
%% @doc Handling call messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast('bind_to_metaflow', #state{node=Node}=State) ->
    Bindings = ecallmgr_config:get(?BINDINGS_CFG_KEY, ?DEFAULT_BINDINGS, Node),
    case ecallmgr_fs_router_util:register_bindings(Node, ?FETCH_SECTION, Bindings) of
        'true' -> {'noreply', State};
        'false' ->
            lager:critical("unable to establish route bindings : ~p", [Bindings]),
            {'stop', 'no_binding', State}
    end;
handle_cast('bind_to_events', #state{node=Node}=State) ->
    case gproc:reg({'p', 'l', ?FS_EVENT_REG_MSG(Node, <<"CHANNEL_BRIDGE">>)}) =:= 'true' of
        'true' -> {'noreply', State};
        'false' -> {'stop', 'gproc_badarg', State}
    end;
handle_cast({'gen_listener', {'created_queue', Q}}, State) ->
    {'noreply', State#state{control_q=Q}};
handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers.
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), state()) -> gen_listener:handle_event_return().
handle_event(_JObj, #state{node=Node, control_q=CtrlQ}) ->
    {'reply', [{'FSNode', Node}
              ,{'Control-Q', CtrlQ}
              ]}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info({'event', [UUID | Props]}, #state{node=Node}=State) ->
    _ = kz_util:spawn(fun metaflow_bind:handle_bridge/3, [Node, UUID, Props]),
    {'noreply', State};
handle_info({'route', Section, <<"REQUEST_PARAMS">>, _SubClass, <<"metaflow">>, FSId, CallId, FSData},
            #state{node=Node
                  ,control_q=CtrlQ
                  }=State) ->
    lager:info("processing metaflow fetch request ~s (call ~s) from ~s", [FSId, CallId, Node]),
    _ = kz_util:spawn(fun metaflow_route:handle_metaflow_route/5, [Section, Node, FSId, CallId, [{<<"Control-Queue">>, CtrlQ} | FSData]]),
    {'noreply', State};
handle_info(_Other, State) ->
    lager:debug("unhandled msg: ~p", [_Other]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
                                                % terminate. It should be the opposite of `Module:init/1' and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, #state{node=Node}) ->
    lager:info("route metaflow listener for ~s terminating: ~p", [Node, _Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed.
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.
