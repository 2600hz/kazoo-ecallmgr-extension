%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2019, 2600Hz
%%% @doc handle communication with freeswitch thru amqp
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_listener).
-behaviour(gen_listener).

-export([start_link/1]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_req/2
        ,handle_event/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr_extension.hrl").
-include("gen_server_spec.hrl").

-define(RESPONDERS, [{?MODULE, [{<<"*">>, <<"*">>}]}]).

-define(BINDINGS, [{'self', []}]).
-define(QUEUE_OPTIONS, [{'exclusive', 'false'}]).
-define(CONSUME_OPTIONS, [{'exclusive', 'false'}]).


-define(SERVER, ?MODULE).

-type state() :: map().

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the server
%% @end
%%------------------------------------------------------------------------------
-spec start_link(kz_term:ne_binary()) -> kz_types:startlink_ret().
start_link(Queue) ->
    gen_listener:start_link(?MODULE
                           ,[{'responders', ?RESPONDERS}
                            ,{'bindings', ?BINDINGS}
                            ,{'queue_name', Queue}
                            ,{'queue_options', ?QUEUE_OPTIONS}
                            ,{'consume_options', ?CONSUME_OPTIONS}
                            ]
                           ,[Queue]).


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server
%%
%% @end
%%------------------------------------------------------------------------------
init([Queue]) ->
    {'ok', #{queue => Queue}}.

%%------------------------------------------------------------------------------
%% @doc Handling call messages
%%
%% @end
%%------------------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages
%%
%% @end
%%------------------------------------------------------------------------------
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
handle_info({'amqp_send', Payload, PublisherFun}, #{queue := Q} = State) ->
    PublisherFun([{<<"Server-ID">>, Q} | Payload]),
    {'noreply', State};
handle_info(_Other, State) ->
    lager:debug("unhandled msg: ~p", [_Other]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers
%%
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), state()) -> {'reply', kz_term:proplist()}.
handle_event(_JObj, #{}) ->
    {'reply', []}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
terminate(_Reason, #{}) ->
    lager:info("api listener terminating: ~p", [_Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed
%%
%% @end
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, Props) ->
%%    lager:debug_unsafe("REPLY : ~s", [kz_json:encode(JObj, ['pretty'])]),
    Pid = kz_term:to_pid(kz_api:reply_to(JObj)),
    Event = kz_api:event_name(JObj),
    handle_req(Pid, Event, JObj, Props).

handle_req('undefined', _Event, _JObj, _Props) ->
    lager:warning("NO REPLY-TO-PID ~s : ~p", [_Event, _JObj]);

handle_req(Pid, <<"API">>, JObj, _Props) ->
    case kz_json:get_atom_value(<<"Result">>, JObj) of
        'ok' ->
            case kz_json:get_ne_binary_value(<<"Return">>, JObj) of
                <<"-ERR", Error/binary>> -> api_result(Pid, error, Error);
                <<"+OK", Msg/binary>> -> api_result(Pid, ok, Msg);
                Msg -> api_result(Pid, ok, Msg)
            end;
        'error' -> api_result(Pid, error, kz_json:get_ne_binary_value(<<"Return">>, JObj))
    end;

handle_req(Pid, <<"BACKGROUND_JOB">>, JObj, _Props) ->
    JobId = kz_json:get_ne_binary_value(<<"Job-UUID">>, JObj),
    Data = kz_json:to_proplist(JObj),
    Reply = kz_json:get_ne_binary_value(<<"Job-Return">>, JObj),
    case kz_json:get_atom_value(<<"Job-Result">>, JObj) of
        'bgok' ->
            case Reply of
                <<"-ERR", Error/binary>> -> bgapi_result(Pid, bgerror, Error, JobId, Data);
                <<"+OK", Msg/binary>> -> bgapi_result(Pid, bgok, Msg, JobId, Data);
                _ -> bgapi_result(Pid, bgok, Reply, JobId, Data)
            end;
        'bgerror' -> bgapi_result(Pid, bgerror, Reply, JobId, Data)
    end;

handle_req(Pid, <<"json_api.reply">>, JObj, _Props) ->
    case kz_json:get_atom_value(<<"status">>, JObj) of
        'success' -> Pid ! {switch_reply, {'ok', kz_json:get_json_value(<<"response">>, JObj)}};
        'error' -> Pid ! {switch_reply, {'error', kz_json:get_first_defined([<<"error">>, <<"message">>], JObj)}}
    end;

handle_req(_Pid, _Event, _JObj, _Props) ->
    lager:warning("REPLY-TO-PID ~p ,  ~s : ~p", [_Pid, _Event, _JObj]).

bgapi_result(Pid, Result, Bin, JobId, Data) ->
    Pid ! {switch_reply, bgapi_result(Result, Bin, JobId, Data)}.

bgapi_result(Result, 'undefined', JobId, Data) -> {Result, JobId, Data};
bgapi_result(Result, <<"-ERR", Error/binary>>, JobId, Data) ->
    bgapi_result(Result, Error, JobId, Data);
bgapi_result(Result, <<"+OK", Msg/binary>>, JobId, Data) ->
    bgapi_result(Result, Msg, JobId, Data);
bgapi_result(Result, Bin, JobId, Data) ->
    case kz_binary:strip_left(kz_binary:strip_right(Bin, <<"\n">>), $\s) of
        <<>> when Result =:= 'error' -> {error, JobId, 'failed', Data};
        <<>> -> {bgok, JobId, Data};
        <<"true">> -> {Result, JobId, true, Data};
        <<"false">> -> {Result, JobId, false, Data};
        Msg -> {Result, JobId, Msg, Data}
    end.

api_result(Pid, Result, Bin) ->
    Pid ! {switch_reply, api_result(Result, Bin)}.

api_result(Result, 'undefined') -> Result;
api_result(Result, <<"-ERR", Error/binary>>) ->
    api_result(Result, Error);
api_result(Result, <<"+OK", Msg/binary>>) ->
    api_result(Result, Msg);
api_result(Result, Bin) ->
    case kz_binary:strip_left(kz_binary:strip_right(Bin, <<"\n">>), $\s) of
        <<>> when Result =:= 'error' -> {error, 'failed'};
        <<>> -> ok;
        <<"true">> -> {Result, true};
        <<"false">> -> {Result, false};
        Msg -> {Result, Msg}
    end.
