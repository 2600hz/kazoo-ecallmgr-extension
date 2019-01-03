%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_extension_node_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-export([start_link/2]).

-export([init/1]).

-define(NODE_CHILD_TYPE(Type), kz_json:from_list([{<<"type">>, Type}])).
-define(NODE_WORKER, ?NODE_CHILD_TYPE(<<"worker">>)).
-define(NODE_SUPERVISOR, ?NODE_CHILD_TYPE(<<"supervisor">>)).

-define(CHILDREN, kz_json:from_list(
                    [{<<"event_amqp_sup">>, ?NODE_SUPERVISOR}
                    ,{<<"route_metaflow">>, ?NODE_WORKER}
                    ,{<<"bowout">>, ?NODE_WORKER}
                    ,{<<"metaflow">>, ?NODE_WORKER}
                    ])).

%%==============================================================================
%% API functions
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor.
%% @end
%%------------------------------------------------------------------------------
-spec start_link(atom(), kz_term:proplist()) -> kz_types:startlink_ret().
start_link(Node, Options) ->
    lager:debug("starting ecallmgr extension supervisor for node ~p", [Node]),
    supervisor:start_link(?MODULE, [Node, Options]).

%%==============================================================================
%% Supervisor callbacks
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Whenever a supervisor is started using `supervisor:start_link/[2,3]',
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> kz_types:sup_init_ret().
init([Node, Options]) ->
    RestartStrategy = 'one_for_one',
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 6,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    NodeB = kz_term:to_binary(Node),
    Args = [Node, Options],
    Modules = kz_json:merge_jobjs(kapps_config:get(?CONFIG_CAT, <<"extension_modules">>, ?CHILDREN), ?CHILDREN),
    Children = kz_json:foldl(fun(Module, V, Acc) ->
                                     Type = kz_json:get_ne_binary_value(<<"type">>, V),
                                     [child_name(NodeB, Args, Module, Type) | Acc]
                             end
                            ,[]
                            ,Modules
                            ),
    {'ok', {SupFlags, Children}}.

-spec child_name(binary(), list(), binary(), binary()) -> any().
child_name(NodeB, Args, Module, <<"supervisor">>) ->
    Name = kz_term:to_atom(<<NodeB/binary, "_", Module/binary>>, 'true'),
    Mod = kz_term:to_atom(<<"ecallmgr_fs_", Module/binary>>, 'true'),
    ?SUPER_NAME_ARGS(Mod, Name, Args);
child_name(NodeB, Args, Module, <<"worker">>) ->
    Name = kz_term:to_atom(<<NodeB/binary, "_", Module/binary>>, 'true'),
    Mod = kz_term:to_atom(<<"ecallmgr_fs_", Module/binary>>, 'true'),
    ?WORKER_NAME_ARGS(Mod, Name, Args).
