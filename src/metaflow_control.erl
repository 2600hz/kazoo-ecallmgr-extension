%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2011-2018, 2600Hz
%%% @doc Receive call command and executes
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_control).


-export([handle_req/2]).

-include("metaflow.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec handle_req(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_req(JObj, Props) ->
    Node = props:get_value('FSNode', Props),
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
    kz_util:put_callid(Cmd),
    Category = kz_api:event_category(Cmd),
    Event = kz_api:event_name(Cmd),

    lager:debug("executing ~s ~s '~s' ~s"
               ,[Category
                ,Event
                ,kz_json:get_value(<<"Application-Name">>, Cmd)
                ,kz_json:get_value(<<"Msg-ID">>, Cmd, <<>>)
                ]),
    CallId = kz_json:get_value(<<"Call-ID">>, Cmd),
    Mod = get_module(Category, Event),
    try Mod:Fun(Node, CallId, Cmd, self())
    catch
        _:{'error', 'nosession'} ->
            lager:debug("unable to execute command, no session");
        'error':{'badmatch', {'error', 'nosession'}} ->
            lager:debug("unable to execute command, no session");
        'error':{'badmatch', {'error', ErrMsg}} ->
            ST = erlang:get_stacktrace(),
            lager:debug("invalid command ~s: ~p", [kz_json:get_value(<<"Application-Name">>, Cmd), ErrMsg]),
            kz_util:log_stacktrace(ST);
        'throw':{'msg', ErrMsg} ->
            lager:debug("error while executing command ~s: ~s", [kz_json:get_value(<<"Application-Name">>, Cmd), ErrMsg]);
        'throw':Msg ->
            lager:debug("failed to execute ~s: ~s", [kz_json:get_value(<<"Application-Name">>, Cmd), Msg]);
        _A:_B ->
            ST = erlang:get_stacktrace(),
            lager:debug("exception (~s) while executing ~s: ~p", [_A, kz_json:get_value(<<"Application-Name">>, Cmd), _B]),
            kz_util:log_stacktrace(ST)
    end.

exec_dialplan(Node, UUID, DP) ->
    [ecallmgr_util:send_cmd(Node, UUID, AppName, AppData) || {AppName, AppData} <- DP].

-spec get_module(kz_term:ne_binary(), kz_term:ne_binary()) -> atom().
get_module(Category, Name) ->
    ModuleName = <<"ecallmgr_", Category/binary, "_", Name/binary>>,
    try kz_term:to_atom(ModuleName)
    catch
        'error':'badarg' ->
            kz_term:to_atom(ModuleName, 'true')
    end.
