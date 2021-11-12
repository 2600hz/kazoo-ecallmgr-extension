%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2021, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_listener_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-export([start_link/0]).
-export([init/1]).
-export([start_listener/2]).

%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    {'ok', Pid} = supervisor:start_link({'local', ?MODULE}, ?MODULE, []),
    Workers = kz_app_config:get_integer(?APP_CONFIG_CAT, [<<"amqp">>, <<"api">>, <<"listeners">>], 5),
    QueueId = kz_binary:rand_hex(16),
    kz_process:spawn(fun() -> [begin
                                   _ = supervisor:start_child(Pid, [QueueId]),
                                   timer:sleep(1000)
                               end || _N <- lists:seq(1, Workers)
                              ]
                     end),
    {'ok', Pid}.

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
init([]) ->
    RestartStrategy = 'simple_one_for_one',
    MaxRestarts = 0,
    MaxSecondsBetweenRestarts = 1,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {'ok', {SupFlags, [?WORKER_ARGS_TYPE('mod_com_kazoo_listener', [], 'temporary')]}}.

-spec start_listener(pid(), kz_term:ne_binary()) -> kz_types:startlink_ret().
start_listener(Pid, Queue) ->
    supervisor:start_child(?MODULE, [Pid, Queue]).
