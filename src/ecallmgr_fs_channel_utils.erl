%% @author root
%% @doc @todo Add description to ecallmgr_channel_utils.


-module(ecallmgr_fs_channel_utils).

%% ====================================================================
%% API functions
%% ====================================================================
-export([control_queue/1]).


-include("ecallmgr-extension.hrl").

%% ====================================================================
%% Internal functions
%% ====================================================================

control_queue(CallId) ->
    gproc:lookup_pids({'p', 'l', {'call_control', CallId}}).
