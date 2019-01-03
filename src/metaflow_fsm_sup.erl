%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(metaflow_fsm_sup).

-behaviour(supervisor).

-include("metaflow.hrl").

-define(SERVER, ?MODULE).

-export([start_link/0]).
-export([init/1]).
-export([new/4]).

-define(CHILDREN, [?WORKER_TYPE('metaflow_fsm', 'temporary')]).

%%==============================================================================
%% API functions
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor.
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    supervisor:start_link({'local', ?SERVER}, ?MODULE, []).

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
-spec init(any()) -> kz_types:sup_init_ret().
init([]) ->
    RestartStrategy = 'simple_one_for_one',
    MaxRestarts = 0,
    MaxSecondsBetweenRestarts = 1,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {'ok', {SupFlags, ?CHILDREN}}.

-spec new(atom(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object()) -> kz_types:sup_startchild_ret() | 'ok'.
new(Node, UUID, OtherUUID, JObj) ->
    case gproc:lookup_values({'p', 'l', ?METAFLOW_REG_MSG(UUID)}) of
        [] -> supervisor:start_child(?SERVER, [Node, UUID, OtherUUID, JObj]);
        _ -> lager:debug("metaflow for ~s already in place", [UUID])
    end.
