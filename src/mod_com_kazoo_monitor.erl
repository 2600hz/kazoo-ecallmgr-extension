%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
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

-spec handle_heartbeat(kz_types:kz_node(), state()) -> state().
handle_heartbeat(Node, State) ->
    CoreUUID = core_uuid(Node),
    Nodename = node_name(Node),
    handle_heartbeat(is_media_server(Node), CoreUUID, Nodename, State).

-spec handle_heartbeat(boolean(), atom(), atom(), state()) -> state().
handle_heartbeat(false, _CoreUUID, _Nodename, State) -> State;
handle_heartbeat(true, CoreUUID, Nodename, #{nodes := Nodes} = State) ->
    INU = ecallmgr_fs_nodes:is_node_up(Nodename),
    case heartbeat_match(CoreUUID, Nodename, Nodes) of
        {#{name := _}, _} when INU ->
            _ = maybe_update_code(Nodes),
            State;
        {#{name := _}, _} ->
            _ = notify_nodeup(Nodename),
            State;
        {_, #{uuid := CoreUUID}} when INU ->
            _ = maybe_update_code(Nodes),
            State;
        {_, #{uuid := CoreUUID}} ->
            _ = notify_nodeup(Nodename),
            State;
        {_, #{uuid := OtherUUID}} when INU ->
            RemovedNodes = maps:without([OtherUUID, Nodename], Nodes),
            NewNodes = RemovedNodes#{CoreUUID => #{name => Nodename}
                                    ,Nodename => #{uuid => CoreUUID}
                                    },
            _ = update_code(NewNodes),
            State#{nodes => NewNodes};
        {_, #{uuid := OtherUUID}} ->
            RemovedNodes = maps:without([OtherUUID, Nodename], Nodes),
            NewNodes = RemovedNodes#{CoreUUID => #{name => Nodename}
                                    ,Nodename => #{uuid => CoreUUID}
                                    },
            _ = update_code(NewNodes),
            _ = notify_nodeup(Nodename),
            State#{nodes => NewNodes};
        {undefined, undefined}
          when INU ->
            NewNodes = Nodes#{CoreUUID => #{name => Nodename}
                             ,Nodename => #{uuid => CoreUUID}
                             },
            _ = update_code(NewNodes),
            State#{nodes => NewNodes};
        {undefined, undefined} ->
            handle_nodeup(CoreUUID, Nodename, State);
        _Other ->
            lager:info("unhandled heartbeat match => ~p", [_Other]),
            State
    end.

-type node_map_result() :: {undefined | map()}.
-type heartbeat_match_result() :: {node_map_result(), node_map_result()}.

-spec heartbeat_match(atom(), atom(), map()) -> heartbeat_match_result().
heartbeat_match(CoreUUID, Nodename, Nodes) ->
    {maps:get(CoreUUID, Nodes, undefined)
    ,maps:get(Nodename, Nodes, undefined)
    }.

-spec handle_nodeup(atom(), atom(), state()) -> state().
handle_nodeup(CoreUUID, Nodename, #{nodes := Nodes}=State) ->
    NewNodes = Nodes#{CoreUUID => #{name => Nodename}
                     ,Nodename => #{uuid => CoreUUID}
                     },
    _ = update_code(NewNodes),
    _ = notify_nodeup(Nodename),
    State#{nodes => NewNodes}.

-spec notify_nodeup(atom()) -> 'ok' | {'error', 'no_connection'}.
notify_nodeup(Nodename) ->
    case ecallmgr_fs_nodes:is_node(Nodename) of
        'true' -> ecallmgr_fs_nodes:nodeup(Nodename, 'heartbeat');
        'false' -> ecallmgr_fs_nodes:add(Nodename, 'no_cookie', [{'connect_strategy', 'heartbeat'}])
    end.

-spec handle_expired(kz_types:kz_node(), state()) -> state().
handle_expired(Node, State) ->
    CoreUUID = core_uuid(Node),
    Nodename = node_name(Node),
    handle_expired(is_media_server(Node), CoreUUID, Nodename, State).

-spec handle_expired(boolean(), atom(), atom(), state()) -> state().
handle_expired(false, _CoreUUID, _Nodename, State) -> State;
handle_expired(true, CoreUUID, Nodename, #{nodes := Nodes} = State) ->
    lager:critical("received node down notice for ~s / ~s", [CoreUUID, Nodename]),
    NewNodes = maps:without([CoreUUID, Nodename], Nodes),
    _ = update_code(NewNodes),
    _ = ecallmgr_fs_nodes:nodedown(Nodename),
    State#{nodes => NewNodes}.

is_code_handled({CoreUUID, NodeName}) ->
    freeswitch:mod(NodeName) =/= 'mod_com_kazoo'
        orelse mod_com_kazoo:core_uuid(NodeName) =/= CoreUUID.

maybe_update_code(Nodes) ->
    Props = [{K, Name} || {K, #{name := Name}} <- maps:to_list(Nodes)],
    case lists:any(fun is_code_handled/1, Props) of
        'true' -> update_code(Nodes);
        'false' -> 'ok'
    end.

update_code(Nodes) ->
    lager:info("mod_com_kazoo code out of sync, updating."),
    Props = [{K, Name} || {K, #{name := Name}} <- maps:to_list(Nodes)],
    FSCode = freeswitch_code(Props),
    meta:replace_function('mod', 1, FSCode, 'freeswitch'),
    meta:replace_function('core_uuid', 1, mod_com_kazoo_code(Props), 'mod_com_kazoo').

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
