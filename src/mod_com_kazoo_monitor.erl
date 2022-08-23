%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2022, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_monitor).

-behaviour(gen_server).

-export([start_link/0]).

-export([monitor/0]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-include("ecallmgr_extension.hrl").

-type state() :: map().

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    gen_server:start_link({'local', ?MODULE}, ?MODULE, [], []).


%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server
%%
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> {'ok', state()}.
init([]) ->
    kz_log:put_callid(?MODULE),
    lager:info("starting new fs amqp monitor"),
    _ = kz_nodes:notify_new(),
    _ = kz_nodes:notify_expire(),
    _ = kz_nodes:notify_heartbeat(),
    {'ok', #{zone => kz_nodes:local_zone(), nodes => #{}}}.

-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'kz_nodes', {'new', #kz_node{zone = Zone} = Node}}, #{zone := Zone} = State) ->
    {'noreply', handle_heartbeat(Node, State)};
handle_cast({'kz_nodes',{'heartbeat', #kz_node{zone = Zone} = Node}}, #{zone := Zone} = State) ->
    {'noreply', handle_heartbeat(Node, State)};
handle_cast({'kz_nodes',{'expire', #kz_node{zone = Zone} = Node}}, #{zone := Zone} = State) ->
    {'noreply', handle_expired(Node, State)};
handle_cast({'kz_nodes', _Node}, State) ->
    {'noreply', State};
handle_cast(monitor_kz_nodes, State) ->
    _ = kz_nodes:notify_new(),
    _ = kz_nodes:notify_expire(),
    _ = kz_nodes:notify_heartbeat(),
    {'noreply', State};
handle_cast(_Cast, State) ->
    lager:debug("unhandled cast: ~p", [_Cast]),
    {'noreply', State}.

-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info(_Info, State) ->
    lager:debug("unhandled message: ~p", [_Info]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, _State) ->
    lager:debug("fs amqp monitor termination: ~p", [ _Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed
%%
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

-spec node_name(kz_types:kz_node()) -> atom().
node_name(#kz_node{node = Node}) -> Node.

-spec core_uuid(kz_types:kz_node()) -> atom().
core_uuid(#kz_node{runtime=Runtime}) ->
    kz_term:to_atom(kz_json:get_value(<<"Core-UUID">>, Runtime), 'true').

-spec is_media_server(kz_types:kz_node()) -> boolean().
is_media_server(#kz_node{kapps=Apps}) ->
    props:is_defined(<<"freeswitch">>, Apps).

-spec is_local_zone(kz_types:kz_node()) -> boolean().
is_local_zone(#kz_node{zone=Zone}) ->
    Zone =:= kz_nodes:local_zone().

-spec should_handle_heartbeat(kz_types:kz_node()) -> boolean().
should_handle_heartbeat(Node) ->
    Funs = [fun is_media_server/1
           ,fun is_local_zone/1
           ],
    lists:all(fun(F) -> F(Node) end, Funs).

-spec handle_heartbeat(kz_types:kz_node(), state()) -> state().
handle_heartbeat(Node, State) ->
    handle_heartbeat(should_handle_heartbeat(Node), Node, State).

-spec handle_heartbeat(boolean(), kz_types:kz_node(), state()) -> state().
handle_heartbeat(false, _Node, State) -> State;
handle_heartbeat(true, Node, State) ->
    CoreUUID = core_uuid(Node),
    Nodename = node_name(Node),
    handle_heartbeat(Node, CoreUUID, Nodename, State).

-spec handle_heartbeat(kz_types:kz_node(), atom(), atom(), state()) -> state().
handle_heartbeat(Node, CoreUUID, Nodename, #{nodes := Nodes} = State) ->
    Context = #{core_uuid => CoreUUID
               ,nodes => Nodes
               ,node => Node
               ,nodename => Nodename
               },
    Routines = [fun instance_id/1
               ,fun is_node_up/1
               ,fun is_node/1
               ,fun node_supervisor/1
               ,fun node_server/1
               ,fun update_nodes/1
               ,fun maybe_update_code/1
               ,fun maybe_node_up/1
               ,fun maybe_node_sync/1
               ,fun maybe_node_run/1
               ],
    try
        #{nodes := NewNodes} = kz_maps:exec(Routines, Context),
        State#{nodes => NewNodes}
    catch
        _Exception:_Error:_Stack ->
            State
    end.

instance_id(#{nodename := Nodename} = Context) ->
    Context#{node_uuid => ecallmgr_fs_nodes:instance_uuid(Nodename)}.

is_node_up(#{nodename := Nodename} = Context) ->
    Context#{node_up => ecallmgr_fs_nodes:is_node_up(Nodename)}.

is_node(#{nodename := Nodename} = Context) ->
    Context#{is_node => ecallmgr_fs_nodes:is_node(Nodename)}.

node_supervisor(#{node := Node} = Context) ->
    Context#{node_supervisor => ecallmgr_fs_sup:find_node(Node)}.

node_server(#{node_supervisor := Supervisor} = Context) ->
    Context#{node_server => ecallmgr_fs_node_sup:node_srv(Supervisor)}.

update_nodes(#{core_uuid := CoreUUID
              ,nodename := Nodename
              ,nodes := CurrentNodes
              } = Context) ->
    Nodes = maybe_remove_older_uuid(CoreUUID, Nodename, CurrentNodes),
    Context#{nodes => Nodes#{CoreUUID => #{name => Nodename}
                            ,Nodename => #{uuid => CoreUUID}
                            }}.

maybe_remove_older_uuid(CoreUUID, Nodename, Nodes) ->
    case maps:get(Nodename, Nodes, 'undefined') of
        'undefined' -> Nodes;
        #{uuid := CoreUUID} -> Nodes;
        #{uuid := OtherUUID} -> maps:without([OtherUUID], Nodes)
    end.

maybe_update_code(#{nodes := Nodes} = Context) ->
    Props = [{K, Name} || {K, #{name := Name}} <- maps:to_list(Nodes)],
    _ = maybe_update_code(Nodes, lists:any(fun is_code_not_handled/1, Props)),
    Context.

maybe_update_code(Nodes, 'true') -> update_code(Nodes);
maybe_update_code(_Nodes, 'false') -> 'ok'.

maybe_node_up(#{is_node := 'true'
               ,node_up := 'true'
               } = Context) ->
    Context;
maybe_node_up(#{is_node := 'true'
               ,node_up := 'false'
               ,nodename := Nodename
               } = Context) ->
    lager:info("notifying node ~s is up", [Nodename]),
    ecallmgr_fs_nodes:nodeup(Nodename, 'heartbeat'),
    Context;
maybe_node_up(#{is_node := 'false'
               ,nodename := Nodename
               } = Context) ->
    lager:info("adding new node ~s", [Nodename]),
    'ok' = ecallmgr_fs_nodes:add(Nodename, erlang:get_cookie(), [{'connect_strategy', 'heartbeat'}]),
    Context.

maybe_node_sync(#{node_server := 'undefined'} = Context) -> Context;
maybe_node_sync(#{is_node := 'true'
                 ,core_uuid := CoreUUID
                 ,node_server := Server
                 } = Context) ->
    case node_instance_uuid(Server) of
        CoreUUID -> Context;
        OtherUUID -> node_sync(Context#{node_core_uuid => OtherUUID})
    end;
maybe_node_sync(Context) -> Context.

node_sync(#{core_uuid := CoreUUID
           ,node_core_uuid := NodeCoreUUID
           ,nodename := Nodename
           } = Context) ->
    lager:info("running node sync routines for node ~s uuid from ~s to ~s", [Nodename, NodeCoreUUID, CoreUUID]),
    Routines = [fun sync_info/1
               ,fun sync_fetch/1
               ],
    kz_maps:exec(Routines, Context).

sync_info(#{core_uuid := CoreUUID
           ,node_core_uuid := NodeCoreUUID
           ,nodename := Nodename
           } = Context) ->
    lager:info("synchronizing node ~s uuid from ~s to ~s", [Nodename, NodeCoreUUID, CoreUUID]),
    ecallmgr_fs_node:sync_info(Nodename),
    Context.

sync_fetch(#{core_uuid := CoreUUID
            ,node_core_uuid := NodeCoreUUID
            ,nodename := Nodename
            ,node_supervisor := Supervisor
            } = Context) ->
    lager:info("synchronizing node ~s fetch bindings from ~s to ~s", [Nodename, NodeCoreUUID, CoreUUID]),
    case ecallmgr_fs_node_sup:node_server(Supervisor, <<"amqp_fetch_dialplan_sup">>) of
        undefined ->
            lager:warning("failed to get amqp fetch dialplan sup from ~s", [Nodename]),
            Context;
        Pid ->
            erlang:exit(Pid, 'core_uuid_changed'),
            Context
    end.

node_instance_uuid(Nodename) ->
    kz_term:to_atom(ecallmgr_fs_node:instance_uuid(Nodename), 'true').

maybe_node_run(#{node_server := 'undefined'} = Context) -> Context;
maybe_node_run(#{is_node := 'true'
                ,nodename := Nodename
                ,node := #kz_node{modules=Modules}
                ,node_server := Server
                } = Context) ->
    Loaded = kz_json:get_list_value(<<"loaded">>, Modules),
    case not lists:member(<<"mod_sofia">>, Loaded)
        andalso should_run_cmds()
    of
        'false' ->
            Context;
        'true' ->
            lager:info("starting node commands for ~s", [Nodename]),
            run_node(Server, Nodename),
            Context
    end.

run_node(Server, Nodename) ->
    handle_run_node_result(ecallmgr_fs_node:node_restart(Server), Nodename).

handle_run_node_result({'error', 'invalid_node'}, Nodename) ->
    lager:error("how did this happened? node ~s disapeared from fs nodes", [Nodename]);
handle_run_node_result('ok', _Nodename) -> 'ok'.

should_run_cmds() ->
    kz_nodes:whapp_oldest_node(?APP) =:= node().

-spec handle_expired(kz_types:kz_node(), state()) -> state().
handle_expired(Node, State) ->
    CoreUUID = core_uuid(Node),
    Nodename = node_name(Node),
    handle_expired(is_media_server(Node), CoreUUID, Nodename, State).

-spec handle_expired(boolean(), atom(), atom(), state()) -> state().
handle_expired('false', _CoreUUID, _Nodename, State) -> State;
handle_expired('true', CoreUUID, Nodename, #{nodes := Nodes} = State) ->
    lager:critical("received node down notice for ~s / ~s", [CoreUUID, Nodename]),
    NewNodes = maps:without([CoreUUID, Nodename], Nodes),
    _ = update_code(NewNodes),
    _ = ecallmgr_fs_nodes:nodedown(Nodename),
    State#{nodes => NewNodes}.

is_code_not_handled({CoreUUID, NodeName}) ->
    freeswitch:mod(NodeName) =/= 'mod_com_kazoo'
        orelse mod_com_kazoo:core_uuid(NodeName) =/= CoreUUID.

update_code(Nodes) ->
    lager:info("mod_com_kazoo code out of sync, updating."),
    Props = [{K, Name} || {K, #{name := Name}} <- maps:to_list(Nodes)],
    FSCode = freeswitch_code(Props),
    MODCode = mod_com_kazoo_code(Props),
    meta:replace_function('mod', 1, FSCode, 'freeswitch'),
    meta:replace_function('core_uuid', 1, MODCode, 'mod_com_kazoo').

freeswitch_code(Props) ->
    {'function',1,'mod',1,
     [{'clause',1, [{'atom',1, V}], [], [{'atom',1,'mod_com_kazoo'}]} || {_K, V} <- Props]
     ++ [{'clause',1, [{'var',1,'_'}], [], [{'atom',1,'mod_kazoo'}]}]
    }.

mod_com_kazoo_code(Props) ->
    {'function',1,'core_uuid',1,
     [{'clause',1,[{'atom',1,V}], [], [{'atom',1,K}]} || {K,V} <- Props]
     ++ [{'clause',1, [{'var',1,'X'}], [[{'call',1,{'atom',1,'is_atom'},[{'var',1,'X'}]}]], [{'var',1,'X'}]}]
     ++ [{'clause',1, [{'var',1,'_'}], [], [{'atom',1,'error_not_found'}]}]
    }.

-spec monitor() -> ok.
monitor() ->
    gen_server:cast(?MODULE, monitor_kz_nodes).
