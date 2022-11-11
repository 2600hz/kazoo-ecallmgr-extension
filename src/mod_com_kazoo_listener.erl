%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2022, 2600Hz
%%% @doc handle communication with freeswitch thru amqp
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_listener).
-behaviour(gen_listener).

-export([start_link/2]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_event/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr_extension.hrl").

-define(RESPONDERS, []).
-define(BINDINGS, [{'self', []}]).

-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).
-define(SHARED_QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(SHARED_CONSUME_OPTIONS, [{'exclusive', 'false'}]).

-define(QOS, 50).

-type state() :: map().

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the server
%% @end
%%------------------------------------------------------------------------------
-spec start_link(pid(), kz_term:api_ne_binary()) -> kz_types:startlink_ret().
start_link(Parent, Queue) ->
    gen_listener:start_link(?MODULE
                           ,[{'responders', ?RESPONDERS}
                            ,{'bindings', ?BINDINGS}
                            ,{'basic_qos', ?QOS}
                            | queue_settings(Queue)
                            ]
                           ,[Parent]).


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server
%%
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> {'ok', state()}.
init([Pid]) ->
    {'ok', #{manager => Pid}}.

%%------------------------------------------------------------------------------
%% @doc Handling call messages
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'gen_listener',{'is_consuming', Active}}, #{manager := Pid, queue := Queue} = State) ->
    lager:info("com kazoo api listener is active, notifying manager"),
    gen_server:cast(Pid, {'com_kazoo_api_listener_is_ready', self(), kz_amqp_channel:consumer_channel(), Queue, Active}),
    {'noreply', State};
handle_cast({'gen_listener',{'created_queue', QueueName}}, State) ->
    {'noreply', State#{queue => QueueName}};
handle_cast({'gen_listener', _}, State) ->
    {'noreply', State};
handle_cast(_Msg, State) ->
    lager:warning("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info(_Other, State) ->
    lager:debug("unhandled msg: ~p", [_Other]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), state()) -> gen_listener:handle_event_return().
handle_event(JObj, #{}) ->
    handle_req(JObj),
    'ignore'.

%%------------------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, #{}) ->
    lager:info("api listener terminating: ~p", [_Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed
%%
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

-spec handle_req(kz_json:object()) -> 'ok'.
handle_req(JObj) ->
    %% lager:debug_unsafe("REPLY : ~s", [kz_json:encode(JObj)]),
    Pid = kz_term:to_pid(kz_api:reply_to(JObj)),
    Event = kz_api:event_name(JObj),
    handle_req(Pid, Event, JObj).

handle_req('undefined', _Event, _JObj) ->
    lager:warning("NO REPLY-TO-PID ~s : ~p", [_Event, _JObj]);

handle_req(Pid, <<"API">>, JObj) ->
    MsgId = kz_api:msg_id(JObj),
    case kz_json:get_atom_value(<<"Result">>, JObj) of
        'ok' ->
            case kz_json:get_ne_binary_value(<<"Return">>, JObj) of
                <<"-ERR", Error/binary>> -> api_result(Pid, MsgId, 'error', Error);
                <<"+OK", Msg/binary>> -> api_result(Pid, MsgId, 'ok', Msg);
                Msg -> api_result(Pid, MsgId, 'ok', Msg)
            end;
        'error' -> api_result(Pid, MsgId, 'error', kz_json:get_ne_binary_value(<<"Return">>, JObj))
    end;

handle_req(Pid, <<"BACKGROUND_JOB">>, JObj) ->
    JobId = kz_json:get_ne_binary_value(<<"Job-UUID">>, JObj),
    Data = kz_json:to_proplist(JObj),
    Reply = kz_json:get_ne_binary_value(<<"Job-Return">>, JObj),
    case kz_json:get_atom_value(<<"Job-Result">>, JObj) of
        'bgok' ->
            case Reply of
                <<"-ERR", Error/binary>> -> bgapi_result(Pid, 'bgerror', Error, JobId, Data);
                <<"+OK", Msg/binary>> -> bgapi_result(Pid, 'bgok', Msg, JobId, Data);
                _ -> bgapi_result(Pid, 'bgok', Reply, JobId, Data)
            end;
        'bgerror' -> bgapi_result(Pid, 'bgerror', Reply, JobId, Data)
    end;

handle_req(Pid, <<"json_api.reply">>, JObj) ->
    MsgId = kz_api:msg_id(JObj),
    case kz_json:get_atom_value(<<"status">>, JObj) of
        'success' -> Pid ! {'switch_reply', MsgId, {'ok', kz_json:get_json_value(<<"response">>, JObj)}};
        'error' -> Pid ! {'switch_reply', MsgId, {'error', kz_json:get_first_defined([<<"error">>, <<"message">>], JObj)}}
    end;

handle_req(_Pid, _Event, _JObj) ->
    lager:warning("REPLY-TO-PID ~p ,  ~s : ~p", [_Pid, _Event, _JObj]).

bgapi_result(Pid, Result, Bin, JobId, Data) ->
    Pid ! {'switch_job_reply', bgapi_result(Result, Bin, JobId, Data)}.

-spec bgapi_result('bgerror' | 'bgok', kz_term:api_ne_binary(), JobId, Data) ->
          {'bgok' | 'bgerror', JobId, boolean() | kz_term:ne_binary(), Data} |
          {'bgok', JobId, Data}.
bgapi_result(Result, 'undefined', JobId, Data) -> {Result, JobId, Data};
bgapi_result(Result, <<"-ERR", Error/binary>>, JobId, Data) ->
    bgapi_result(Result, Error, JobId, Data);
bgapi_result(Result, <<"+OK", Msg/binary>>, JobId, Data) ->
    bgapi_result(Result, Msg, JobId, Data);
bgapi_result(Result, Bin, JobId, Data) ->
    case kz_binary:strip_left(kz_binary:strip_right(Bin, <<"\n">>), $\s) of
        <<>> -> {'bgok', JobId, Data};
        <<"true">> -> {Result, JobId, 'true', Data};
        <<"false">> -> {Result, JobId, 'false', Data};
        Msg -> {Result, JobId, Msg, Data}
    end.

api_result(Pid, MsgId, Result, Bin) ->
    Pid ! {'switch_reply', MsgId, api_result(Result, Bin)}.

api_result(Result, 'undefined') -> Result;
api_result(Result, <<"-ERR", Error/binary>>) ->
    api_result(Result, Error);
api_result(Result, <<"+OK", Msg/binary>>) ->
    api_result(Result, Msg);
api_result(Result, Bin) ->
    case kz_binary:strip_left(kz_binary:strip_right(Bin, <<"\n">>), $\s) of
        <<>> when Result =:= 'error' -> {'error', 'failed'};
        <<>> -> 'ok';
        <<"true">> -> {Result, 'true'};
        <<"false">> -> {Result, 'false'};
        Msg -> result(Result, Msg)
    end.

result('error', <<"no such channel" , _/binary>>) -> {'error', 'nosession'};
result('error', <<"No such channel" , _/binary>>) -> {'error', 'nosession'};
result('error', <<"invalid session" , _/binary>>) -> {'error', 'nosession'};
result(Result, Msg) -> {Result, Msg}.

queue_settings('undefined') ->
    [{'queue_name', list_to_binary([<<"com-kazoo-api-">>, kz_binary:rand_uuid()])}
    ,{'queue_options', ?QUEUE_OPTIONS}
    ,{'consume_options', ?CONSUME_OPTIONS}
    ];
queue_settings(Queue) ->
    [{'queue_name', Queue}
    ,{'queue_options', ?SHARED_QUEUE_OPTIONS}
    ,{'consume_options', ?SHARED_CONSUME_OPTIONS}
    ].
