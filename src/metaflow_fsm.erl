%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2014-2018, 2600Hz
%%% @doc
%%% @author James Aimonetti
%%% @author Luis Azedo
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/4]).

%% gen_fsm callbacks
-export([init/1

        ,unarmed/2, unarmed/3
        ,armed/2, armed/3

        ,handle_event/3
        ,handle_sync_event/4
        ,handle_info/3
        ,terminate/3
        ,code_change/4
        ]).

-include("metaflow.hrl").

-type state() :: map().

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_link(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object()) -> any().
start_link(Node, UUID, OtherUUID, JObj) ->
    gen_fsm:start_link(?MODULE, {Node, UUID, OtherUUID, JObj}, []).

%%%=============================================================================
%%% gen_fsm callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec init({atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object()}) -> {'ok', 'unarmed', state()}.
init({Node, UUID, OtherUUID, JObj}) ->
    kz_util:put_callid(UUID),

    lager:debug("starting metaflow fsm for call-id ~s", [UUID]),
    EndpointId = kz_json:get_ne_binary_value(<<"Endpoint-ID">>, JObj, kz_binary:rand_hex(16)),
    ListenOn = kz_json:get_ne_binary_value(<<"Listen-On">>, JObj, <<"self">>),
    BindingDigit = kz_json:get_ne_binary_value(<<"Binding-Digit">>, JObj, <<"*">>),
    CollectTimeout = kz_json:get_integer_value(<<"Collect-Timeout">>, JObj, 1000),
    Patterns = kz_json:get_json_value(<<"Patterns">>, JObj, kz_json:new()),
    Numbers = kz_json:get_json_value(<<"Numbers">>, JObj, kz_json:new()),
    TargetLeg = kz_json:get_ne_binary_value(<<"Target-Leg">>, JObj, ListenOn),
    EventLeg = kz_json:get_ne_binary_value(<<"Event-Leg">>, JObj, ListenOn),

    gen_fsm:send_all_state_event(self(), 'bind'),

    {'ok', 'unarmed', #{metaflow => JObj
                       ,node => Node
                       ,numbers => Numbers
                       ,patterns => Patterns
                       ,binding_digit => BindingDigit
                       ,digit_timeout => CollectTimeout
                       ,uuid => UUID
                       ,other_uuid => OtherUUID
                       ,target_leg => TargetLeg
                       ,event_leg => EventLeg
                       ,endpoint_id => EndpointId
                       ,listen_on => ListenOn
                       }}.

-spec unarmed(any(), state()) -> kz_types:handle_fsm_ret(state()).
unarmed('stop', State) ->
    {'stop', 'normal', State};
unarmed({'dtmf', BindingDigit}, #{binding_digit := BindingDigit}=State) ->
    lager:debug("recv binding digit ~s, arming", [BindingDigit]),
    {'next_state', 'armed', arm(State)};
unarmed({'dtmf', _BindingDigit}, State) ->
    lager:debug("ignoring dtmf '~s' while unarmed", [_BindingDigit]),
    {'next_state', 'unarmed', State};
unarmed(_Event, State) ->
    lager:debug("unhandled unarmed/2: ~p", [_Event]),
    {'next_state', 'unarmed', State, 'hibernate'}.

-spec unarmed(any(), atom(), state()) -> kz_types:handle_sync_event_ret(state()).
unarmed(_Event, _From, State) ->
    lager:debug("unhandled unarmed/3: ~p", [_Event]),
    {'reply', {'error', 'not_implemented'}, 'unarmed', State}.

-spec armed(any(), state()) -> kz_types:handle_fsm_ret(state()).
armed('stop', State) ->
    {'stop', 'normal', State};
armed({'dtmf', Digit}, State) ->
    {'next_state', 'armed', add_dtmf(State, Digit)};
armed({'timeout', _Ref, 'digit_timeout'}, State) ->
    _ = maybe_handle_code(State),
    {'next_state', 'unarmed', disarm(State), 'hibernate'};
armed(_Event, State) ->
    lager:debug("unhandled armed/2: ~p", [_Event]),
    {'next_state', 'armed', State}.

-spec armed(any(), atom(), state()) -> kz_types:handle_sync_event_ret(state()).
armed(_Event, _From, State) ->
    lager:debug("unhandled armed/3: ~p", [_Event]),
    {'reply', {'error', 'not_implemented'}, 'armed', State}.

-spec handle_event(any(), atom(), state()) -> kz_types:handle_fsm_ret(state()).
handle_event('bind', _StateName, #{node := Node
                                  ,uuid := UUID
                                  } = State) ->
    case gproc:reg({'p', 'l', ?FS_CALL_EVENT_MSG(Node, <<"CHANNEL_DESTROY">>, UUID)}) =:= 'true'
        andalso gproc:reg({'p', 'l', ?FS_CALL_EVENT_MSG(Node, <<"DTMF">>, UUID)}) =:= 'true'
        andalso gproc:reg({'p', 'l', ?METAFLOW_REG_MSG(UUID)}) =:= 'true'
    of
        'true' -> {'next_state', 'unarmed', State};
        'false' -> {'stop', 'normal', State}
    end;
handle_event(_Event, StateName, State) ->
    lager:debug("unhandled event in ~s: ~p", [StateName, _Event]),
    {'next_state', StateName, State}.

-spec handle_sync_event(any(), {pid(),any()}, atom(), state()) -> kz_types:handle_sync_event_ret(state()).
handle_sync_event(_Event, _From, StateName, State) ->
    lager:debug("unhandled sync_event in ~s: ~p", [StateName, _Event]),
    {'reply', {'error', 'not_implemented'}, StateName, State}.

-spec handle_info(any(), atom(), state()) -> kz_types:handle_fsm_ret(state()).
handle_info({'event', Event, [UUID | Props]}, StateName, #{node := Node} = State) ->
    process_specific_event(Event, UUID, Props, Node),
    {'next_state', StateName, State};
handle_info(_Info, StateName, State) ->
    lager:debug("unhandled msg in ~s: ~p", [StateName, _Info]),
    {'next_state', StateName, State}.

-spec process_specific_event(kz_term:ne_binary(), kz_term:api_binary(), kz_term:proplist(), atom()) -> any().
process_specific_event(<<"CHANNEL_DESTROY">>, _UUID, _Props, _Node) ->
    gen_fsm:send_event(self(), 'stop');
process_specific_event(<<"DTMF">>, _UUID, Props, _Node) ->
    Digit = props:get_value(<<"DTMF-Digit">>, Props),
    gen_fsm:send_event(self(), {dtmf, Digit});
process_specific_event(_Event, _UUID, _Props, _Node) ->
    lager:debug("event ~s for callid ~s not handled in metaflow fsm (~s)", [_Event, _UUID, _Node]).

-spec terminate(any(), atom(), state()) -> 'ok'.
terminate(_Reason, _StateName, _State) ->
    lager:debug("fsm terminating while in ~s: ~p", [_StateName, _Reason]).

-spec code_change(any(), atom(), state(), any()) -> {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {'ok', StateName, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec has_metaflow(kz_term:ne_binary(), kz_json:object(), kz_json:object()) ->
                          'false' |
                          {'number', kz_json:object()} |
                          {'pattern', kz_json:object()}.
has_metaflow(Collected, Numbers, Patterns) ->
    case has_number(Collected, Numbers) of
        'false' -> has_pattern(Collected, Patterns);
        N -> N
    end.

-spec has_number(kz_term:ne_binary(), kz_json:object()) ->
                        'false' |
                        {'number', kz_json:object()}.
has_number(Collected, Numbers) ->
    case kz_json:get_value(Collected, Numbers) of
        'undefined' -> 'false';
        Number -> {'number', Number}
    end.

-spec has_pattern(kz_term:ne_binary(), kz_json:object()) ->
                         'false' |
                         {'pattern', kz_json:object()}.
has_pattern(Collected, Patterns) ->
    Regexes = kz_json:get_keys(Patterns),
    has_pattern(Collected, Patterns, Regexes).

has_pattern(_Collected, _Patterns, []) -> 'false';
has_pattern(Collected, Patterns, [Regex|Regexes]) ->
    case re:run(Collected, Regex, [{'capture', 'all_but_first', 'binary'}]) of
        'nomatch' -> has_pattern(Collected, Patterns, Regexes);
        {'match', _Captured} -> {'pattern', Collected}
    end.

-spec disarm(state()) -> state().
disarm(#{digit_timeout_ref := Ref}=State) ->
    lager:debug("disarming state"),
    maybe_cancel_timer(Ref),

    State#{digit_timeout_ref => 'undefined'
          ,collected_dtmf => <<>>
          }.

-spec maybe_cancel_timer(any()) -> 'ok'.
maybe_cancel_timer(Ref) when is_reference(Ref) ->
    catch erlang:cancel_timer(Ref),
    'ok';
maybe_cancel_timer(_) -> 'ok'.

-spec maybe_handle_code(state()) -> 'ok'.
maybe_handle_code(#{numbers := Ns
                   ,patterns := Ps
                   ,collected_dtmf := Collected
                   ,uuid := UUID
                   ,node := Node
                   }) ->
    lager:debug("a DTMF timeout, let's check '~s'", [Collected]),
    case has_metaflow(Collected, Ns, Ps) of
        'false' -> lager:debug("no handler for '~s', unarming", [Collected]);
        {'number', _N} -> kz_util:spawn(fun fire_metaflow/3, [Node, UUID, Collected]);
        {'pattern', _P} -> fire_metaflow(Node, UUID, Collected)
    end.

-spec fire_metaflow(atom(), kz_term:ne_binary(), kz_term:ne_binary()) -> 'ok'.
fire_metaflow(Node, UUID, Collected) ->
    lager:debug("firing metaflow ~s : ~s", [UUID, Collected]),
    ecallmgr_util:send_cmd(Node, UUID, "execute_extension", <<Collected/binary, " XML metaflow">>).

-spec arm(state()) -> state().
arm(#{digit_timeout := Timeout}=State) ->
    State#{digit_timeout_ref => gen_fsm:start_timer(Timeout, 'digit_timeout')
          ,collected_dtmf => <<>>
          }.

-spec add_dtmf(state(), kz_term:ne_binary()) -> state().
add_dtmf(#{collected_dtmf := Collected
          ,digit_timeout_ref := OldRef
          ,digit_timeout := Timeout
          }=State, DTMF) ->
    lager:debug("recv dtmf '~s' while armed, adding to '~s'", [DTMF, Collected]),
    maybe_cancel_timer(OldRef),
    State#{digit_timeout_ref => gen_fsm:start_timer(Timeout, 'digit_timeout')
          ,collected_dtmf => <<Collected/binary, DTMF/binary>>
          }.
