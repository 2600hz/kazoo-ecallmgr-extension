%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_amqp_event_stream_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-export([start_link/1]).
-export([init/1]).

-define(SERVER(N), kz_term:to_atom(list_to_binary(["amqp_event_stream_", kz_term:to_binary(N)]), 'true')).

-type bindings() :: atom() | [atom(),...] | kz_term:ne_binary() | kz_term:ne_binaries().
-type profile() :: {atom() | kz_term:ne_binary(), bindings()}.

%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor
%% @end
%%------------------------------------------------------------------------------
-spec start_link(profile()) -> kz_types:startlink_ret().
start_link({Name, _}=Profile) ->
    lager:debug("starting amqp event stream supervisor ~s", [Name]),
    Server = ?SERVER(Name),
    lager:debug("starting amqp event stream supervisor ~s : ~p", [Name, Server]),
    {'ok', Pid} = supervisor:start_link({'local', Server}
                                       ,?MODULE
                                       ,[Profile]
                                       ),
    lager:debug("started amqp event stream supervisor ~s : ~p", [Name, Pid]),
    Workers = kapps_config:get_integer(?CONFIG_CAT, [<<"amqp_event_stream">>, kz_term:to_binary(Name)], 1),
    kz_process:spawn(fun() -> [begin
                                timer:sleep(500),
                                supervisor:start_child(Pid, [Profile])
                            end
                            || _N <- lists:seq(1, Workers)
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
init([_Profile]) ->
    RestartStrategy = 'simple_one_for_one',
    MaxRestarts = 0,
    MaxSecondsBetweenRestarts = 1,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {'ok', {SupFlags, [?WORKER_ARGS_TYPE('ecallmgr_amqp_event_stream', [], 'temporary')]}}.
