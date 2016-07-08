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

-include("ecallmgr.hrl").

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
handle_info({'route', Section, <<"REQUEST_PARAMS">>, _SubClass, <<"metaflow">>, FSId, CallId, FSData}, State) ->
    lager:info("processing metaflow fetch request ~s (call ~s) from ~s", [FSId, CallId, Node]),
    props:to_log(FSData, <<"METAFLOW_REQ">>),
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

-spec init_metaflow_props(kz_proplist()) -> kz_proplist().
init_metaflow_props(Props) ->
    Routines = [fun add_metaflow_missing_props/1
               ],
    lists:foldl(fun(F,P) -> F(P) end, Props, Routines).

-spec add_metaflow_missing_props(kz_proplist()) -> kz_proplist().
add_metaflow_missing_props(Props) ->
    CRHs = [{<<"Metaflow-Request">>, props:get_value(?PROP_MATCHING_DIGITS, Props)],
    AddProps = props:filter_undefined(
                 [{<<"Resource-Type">>,<<"metaflow">>}
                 ,{<<"Custom-Routing-Headers">>, kz_json:from_list(CRHs)}
                 ]),
    props:insert_values(AddProps, Props).

-spec process_route_req(atom(), atom(), ne_binary(), ne_binary(), kz_proplist()) -> 'ok'.
process_route_req(Section, Node, FetchId, CallId, Props) ->
    kz_util:put_callid(CallId),
    do_process_route_req(Section, Node, FetchId, CallId, init_metaflow_props(Props)).

-spec do_process_route_req(atom(), atom(), ne_binary(), ne_binary(), kz_proplist()) -> 'ok'.
do_process_route_req(Section, Node, FetchId, CallId, Props) ->
    case ecallmgr_fs_router_util:search_for_route(Section, Node, FetchId, MsgId, Props, 'false') of
    'ok' ->
            lager:debug("xml fetch metaplan ~s finished without success", [FetchId]);
    {'ok', _JObj} ->
            lager:debug("xml fetch metaplan ~s finished with success", [FetchId]);
    end.
