%%%-------------------------------------------------------------------
%%% @copyright (C) 2011-2016, 2600Hz INC
%%% @doc
%%% handles bowout
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_bowout).

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

-define(SERVER, ?MODULE).

-record(state, {node = 'undefined' :: atom()
               ,options = [] :: kz_proplist()
               }).

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
    gproc:reg({'p', 'l', ?FS_EVENT_REG_MSG(Node, <<"CHANNEL_DESTROY">>)}),
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
    lager:info("bowout handler for ~s terminating: ~p", [Node, _Reason]).

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
process_specific_event(<<"CHANNEL_DESTROY">>, _UUID, Props, Node) ->
    maybe_manual_bowout(Node, Props);
process_specific_event(_Event, _UUID, _Props, _Node) ->
    lager:debug("event ~s for callid ~s not handled in bowout (~s)", [_Event, _UUID, _Node]).

-spec maybe_manual_bowout(atom(), kz_proplist()) -> 'ok'.
maybe_manual_bowout(Node, Props) ->
    App = props:get_value(<<"variable_last_app">>, Props),
    Role = props:get_value(<<"variable_last_bridge_role">>, Props),
    BridgeTo = props:get_value(<<"variable_last_bridge_to">>, Props),
    maybe_manual_bowout(Node, App, Role, BridgeTo).

-spec maybe_manual_bowout(atom(), ne_binary(), ne_binary(), ne_binary()) -> 'ok'.
maybe_manual_bowout(Node, <<"att_xfer">>, <<"originator">>, UUID) ->
    case ecallmgr_fs_channel:fetch(UUID,  'record') of
        {'ok', #channel{loopback_other_leg=OtherLeg
                       ,is_loopback='true'
                       ,account_id=AccountId
                       }
        } ->
            {'ok', #channel{account_id=OtherAccountId}} = ecallmgr_fs_channel:fetch(OtherLeg,  'record'),
            case OtherAccountId =:= AccountId of
                'true' ->
%%                     lager:debug("MANUAL BOWOUT FOR ~s , ~s", [UUID, OtherLeg]),
%%                     lager:debug("MANUAL BOWOUT C1  FOR ~p", [C1]),
%%                     lager:debug("MANUAL BOWOUT C2  FOR ~p", [C2]),
                    freeswitch:api(Node, 'uuid_setvar', <<UUID/binary, " ", "loopback_bowout true">>),
                    freeswitch:api(Node, 'uuid_setvar', <<OtherLeg/binary, " ", "loopback_bowout true">>);
                %%gproc:send({'p', 'l', ?LOOPBACK_BOWOUT_REG(ResigningUUID)}, ?LOOPBACK_BOWOUT_MSG(Node, Props));
                'false' ->
                    lager:debug("not bowing out different accounts ~s  / ~s", [AccountId, OtherAccountId])
            end,
            'ok';
        _Else -> 'ok'
    end;
maybe_manual_bowout(_Node, _App, _Role, _UUID) ->
%    lager:debug("MANUAL BOWOUT ~p, ~p, ~p, ~p", [_Node, _App, _Role, _UUID]),
    'ok'.

