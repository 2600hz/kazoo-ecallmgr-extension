%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2026, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_amqp_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-define(SERVER, ?MODULE).

-export([start_link/0]).
-export([init/1]).

-define(CHILDREN, [?SUPER('ecallmgr_amqp_event_streams_sup')
                  ,?SUPER('ecallmgr_amqp_fetch_sup')
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
