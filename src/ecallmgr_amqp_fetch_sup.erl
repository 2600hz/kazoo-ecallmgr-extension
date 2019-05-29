%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_amqp_fetch_sup).

-behaviour(supervisor).

-include("ecallmgr_extension.hrl").

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    {'ok', Pid} = supervisor:start_link({local, ?SERVER} ,?MODULE, []),
    Workers = kapps_config:get_integer(?CONFIG_CAT, <<"amqp_fetch_listeners">>, 5),
    kz_util:spawn(fun() -> [begin
                                timer:sleep(500),
                                supervisor:start_child(Pid, [])
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
init([]) ->
    RestartStrategy = 'simple_one_for_one',
    MaxRestarts = 0,
    MaxSecondsBetweenRestarts = 1,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {'ok', {SupFlags, [?WORKER_ARGS_TYPE('ecallmgr_amqp_fetch', [], 'temporary')]}}.
