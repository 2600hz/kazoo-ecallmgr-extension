%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2023, 2600Hz
%%% @doc Receive call command and executes
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_control).


-export([handle_req/2]).
-export([exec_payload/2]).

-include("ecallmgr_extension.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, _Props) ->
    lager:debug_unsafe("~s", [kz_json:encode(JObj)]),
    UUID = kz_api:call_id(JObj),
    case ecallmgr_fs_channel:fetch(UUID, 'record') of
        {'error', 'not_found'} -> lager:debug("channel ~s not found locally, exiting", [UUID]);
        {'ok', #channel{handling_locally='true', node=Node}} ->
            exec_payload(Node, JObj);
        {'ok', #channel{}} -> lager:debug("channel ~s not handled on this node, exiting", [UUID])
    end.

-spec exec_payload(atom(), kz_json:object()) -> 'ok'.
exec_payload(Node, JObj) ->
    freeswitch:call_cmd_sync(true),
    case kz_json:get_value(<<"Application-Name">>, JObj) of
        <<"queue">> ->
            'true' = kapi_dialplan:queue_v(JObj),
            Commands = kz_json:get_value(<<"Commands">>, JObj, []),
            DefJObj = kz_json:from_list(kz_api:extract_defaults(JObj)),
            DP = handle_queue_commands(Commands, DefJObj, Node, []),
            UUID = kz_json:get_value(<<"Call-ID">>, JObj),
            exec_dialplan(Node, UUID, DP);
        _AName -> control_process('exec_cmd', JObj, Node)
    end.


handle_queue_commands([], _, _Node, DP) -> DP;
handle_queue_commands([Command|Commands], DefJObj, Node, DP) ->
    case kz_json:is_empty(Command)
        orelse kz_json:get_ne_value(<<"Application-Name">>, Command) =:= 'undefined'
    of
        'true' -> handle_queue_commands(Commands, DefJObj, Node, DP);
        'false' ->
            JObj = kz_json:merge_jobjs(Command, DefJObj),
            'true' = kapi_dialplan:v(JObj),
            Cmd = control_process('fetch_dialplan', JObj, Node),
            handle_queue_commands(Commands, DefJObj, Node, DP ++ Cmd)
    end.

-spec control_process(atom(), kz_json:object(), atom()) -> 'ok' | fs_apps().
control_process(Fun, Cmd, Node) ->
    kz_log:put_callid(Cmd),
    Category = kz_api:event_category(Cmd),
    Event = kz_api:event_name(Cmd),

    DialplanApp = kapi_dialplan:application_name(Cmd),
    lager:debug("executing ~s ~s '~s' ~s"
               ,[Category
                ,Event
                ,DialplanApp
                ,kz_api:msg_id(Cmd, <<>>)
                ]),

    CallId = kz_api:call_id(Cmd),

    M = get_mfa(Category, Event, Fun),

    try apply(M, Fun, [Node, CallId, Cmd])
    catch
        _:{'error', 'nosession'}:_ ->
            lager:debug("unable to execute command, no session");
        'error':{'badmatch', {'error', 'nosession'}}:_ ->
            lager:debug("unable to execute command, no session");
        'error':{'badmatch', {'error', ErrMsg}}:ST ->
            lager:debug("invalid command ~s: ~p", [DialplanApp, ErrMsg]),
            kz_log:log_stacktrace(ST);
        'throw':{'msg', ErrMsg} ->
            lager:debug("error while executing command ~s: ~s", [DialplanApp, ErrMsg]);
        'throw':Msg ->
            lager:debug("failed to execute ~s: ~s", [DialplanApp, Msg]);
        _A:_B:ST ->
            lager:debug("exception (~s) while executing ~s: ~p", [_A, DialplanApp, _B]),
            kz_log:log_stacktrace(ST)
    end.

exec_dialplan(Node, UUID, DP) ->
    [send_cmd(Node, UUID, App) || App <- DP].

send_cmd(Node, UUID, {AppName, AppData}) ->
    ecallmgr_util:send_cmd(Node, UUID, AppName, AppData);
send_cmd(_Node, UUID, {AppName, AppData, NewNode, ExtraHeaders}) ->
    ecallmgr_util:send_cmd(NewNode, UUID, AppName, AppName, AppData, ExtraHeaders).


-spec get_mfa(kz_term:ne_binary(), kz_term:ne_binary(), atom()) -> module().
get_mfa(Category, Name, Fun) ->
    ModuleName = <<"ecallmgr_", Category/binary, "_", Name/binary>>,
    case kz_module:is_exported(ModuleName, Fun, 3) of
        'true' ->
            kz_term:to_atom(ModuleName);
        'false' ->
            lager:error("module ~s does not export ~s/3", [ModuleName, Fun]),
            throw({'error', 'no_function'})
    end.
