%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2022, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_fs_amqp_fetch_dialplan_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-export([start_link/2]).
-export([init/1]).

-define(CHILDREN(A), [?WORKER_ARGS_TYPE('ecallmgr_fs_amqp_fetch_dialplan', [A], 'temporary')]).

%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor
%% @end
%%------------------------------------------------------------------------------
-spec start_link(atom(), kz_term:proplist()) -> kz_types:startlink_ret().
start_link(Node, Options) ->
    lager:debug("starting dialplan node sup  ~s", [Node]),
    {'ok', Pid} = supervisor:start_link({local, sup_name(Node)} ,?MODULE, [Node, Options]),
    _ = kz_process:spawn(fun start_workers/1, [Pid]),
    {'ok', Pid}.

sup_name(Node) ->
    Name = iolist_to_binary([kz_term:to_binary(?MODULE)
                            ,"_"
                            ,kz_term:to_binary(Node)
                            ]),
    kz_term:to_atom(Name, 'true').

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> kz_types:sup_init_ret().
init([Node, Options]) ->
    RestartStrategy = 'simple_one_for_one',
    MaxRestarts = 0,
    MaxSecondsBetweenRestarts = 1,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {'ok', {SupFlags, ?CHILDREN(dialplan_args(Node, Options))}}.

dialplan_args(Node, Options) ->
    #{node => Node
     ,options => Options
     ,share_type => cfg_share_type()
     ,core_uuid => mod_com_kazoo:core_uuid(Node)
     }.

cfg_share_type() ->
    kz_app_config:get_atom(?APP_CONFIG_CAT, [<<"amqp">>, <<"dialplan">>, <<"share_type">>], cfg_default_share_type()).

cfg_default_share_type() ->
    case cfg_listeners() of
        1 -> queue;
        _ -> hashed
    end.

cfg_listeners() ->
    lists:max([1, kz_app_config:get_integer(?APP_CONFIG_CAT, [<<"amqp">>, <<"dialplan">>, <<"listeners">>], 5)]).

start_workers(Pid) ->
    Workers = cfg_listeners(),
    [start_worker(Pid, N) || N <- lists:seq(1, Workers)].

start_worker(Pid, N) ->
    supervisor:start_child(Pid, [N]).
