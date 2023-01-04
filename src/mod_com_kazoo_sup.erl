%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2023, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-define(SERVER, ?MODULE).

-export([start_link/0]).
-export([init/1]).

-export([start_monitor/0]).

-define(CHILDREN, [?SUPER('mod_com_kazoo_listener_sup')
                  ,?WORKER('mod_com_kazoo_api')
                  ]).

%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    supervisor:start_link({'local', ?SERVER}, ?MODULE, []).

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
-spec init(any()) -> kz_types:sup_init_ret().
init([]) ->
    _ = kz_process:set_startup(),
    RestartStrategy = 'one_for_one',
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {'ok', {SupFlags, ?CHILDREN}}.

-spec start_monitor() -> kz_types:sup_startchild_ret() | ok.
start_monitor() ->
    start_monitor(whereis(mod_com_kazoo_monitor)).

-spec start_monitor(kz_term:api_pid()) -> kz_types:sup_startchild_ret() | ok.
start_monitor(undefined) -> supervisor:start_child(?MODULE, ?WORKER('mod_com_kazoo_monitor'));
start_monitor(_) -> ok.

