%%%-------------------------------------------------------------------
%%% @copyright (C) 2012-2016, 2600Hz INC
%%% @doc
%%%
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_extension_node_sup).

-behaviour(supervisor).

-include("ecallmgr-extension.hrl").

-export([start_link/2]).

-export([init/1]).

-define(CHILDREN, [<<"event_amqp_sup">>
                  ,<<"route_metaflow">>
                  ]).

%% ===================================================================
%% API functions
%% ===================================================================

%%--------------------------------------------------------------------
%% @public
%% @doc Starts the supervisor
%%--------------------------------------------------------------------
-spec start_link(atom(), kz_proplist()) -> startlink_ret().
start_link(Node, Options) ->
    lager:debug("starting ecallmgr extension supervisor for node ~p", [Node]),
    supervisor:start_link(?MODULE, [Node, Options]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(list()) -> sup_init_ret().
init([Node, Options]) ->
    RestartStrategy = 'one_for_one',
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 6,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    NodeB = kz_util:to_binary(Node),
    Args = [Node, Options],
    Children = [ child_name(NodeB, Args, H) || H <- ecallmgr_config:get(<<"extension_modules">>, ?CHILDREN)],

    {'ok', {SupFlags, Children}}.

-spec child_name(binary(), list(), binary() | tuple()) -> any().
child_name(NodeB, Args, {<<"supervisor">>, Module}) ->
    Name = kz_util:to_atom(<<NodeB/binary, "_", Module/binary>>, 'true'),
    Mod = kz_util:to_atom(<<"ecallmgr_fs_", Module/binary>>, 'true'),
    ?SUPER_NAME_ARGS(Mod, Name, Args);
child_name(NodeB, Args, {<<"worker">>, Module}) ->
    Name = kz_util:to_atom(<<NodeB/binary, "_", Module/binary>>, 'true'),
    Mod = kz_util:to_atom(<<"ecallmgr_fs_", Module/binary>>, 'true'),
    ?WORKER_NAME_ARGS(Mod, Name, Args);
child_name(NodeB, Args, <<"event_amqp_sup">>=Module) ->
    Name = kz_util:to_atom(<<NodeB/binary, "_", Module/binary>>, 'true'),
    Mod = kz_util:to_atom(<<"ecallmgr_fs_", Module/binary>>, 'true'),
    ?SUPER_NAME_ARGS(Mod, Name, Args);
child_name(NodeB, Args, <<_/binary>>=Module) ->
    Name = kz_util:to_atom(<<NodeB/binary, "_", Module/binary>>, 'true'),
    Mod = kz_util:to_atom(<<"ecallmgr_fs_", Module/binary>>, 'true'),
    ?WORKER_NAME_ARGS(Mod, Name, Args).

