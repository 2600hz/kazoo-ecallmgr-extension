%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2021, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_api).

-behaviour(gen_server).

-include("ecallmgr_extension.hrl").

-export([start_link/0]).
-export([send/2]).

%% gen_server callbacks
-export([handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ,init/1
        ]).

-type state() :: map().

%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Starts Server
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    gen_server:start_link({'local', ?MODULE}, ?MODULE, [], []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server.
%% @end
%%------------------------------------------------------------------------------
-spec init(list()) -> {'ok', state()}.
init(_) ->
    Workers = kz_app_config:get_integer(?APP,[<<"amqp">>, <<"api">>, <<"listeners">>], 5),
    gen_server:cast(self(), init),
    {'ok', #{workers => Workers
            ,queue => set_queue()
            ,listeners => #{}
            ,pids => #{}
            ,channels => #{}
            ,refs => #{}
            }}.

%%------------------------------------------------------------------------------
%% @doc Handling call messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    lager:debug("unhandled call: ~p from ~p", [_Request, _From]),
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast('init', State) ->
    kz_amqp_channel:requisition(),
    {'noreply', init_queues(State)};

handle_cast('init_queues', State) ->
    {'noreply', init_queues(State)};

handle_cast({'com_kazoo_api_listener_is_ready', Pid, Channel}, State) ->
    {'noreply', add_listener(Pid, Channel, State)};

handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info({'kz_amqp_assignment', {'new_channel', Reconnect, Channel}}, State) ->
    {'noreply', handle_canary(Reconnect, Channel, State)};
handle_info({'DOWN', Ref, 'process', Pid, Reason}, State) ->
    {noreply, handle_down(Pid, Ref, Reason, State)};

handle_info(_Msg, State) ->
    lager:debug("unhandled message: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a `gen_server' when it is about to
%% terminate. It should be the opposite of `Module:init/1' and do any
%% necessary cleaning up. When it returns, the `gen_server' terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, #{canary := Channel}) ->
    kz_amqp_channel:release(Channel),
    lager:debug("releasing channel ~p and terminating call control manager : ~p ", [Channel, _Reason]);
terminate(_Reason, _) ->
    lager:debug("terminating call control manager : ~p ", [_Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed.
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.


-spec remove_listener(pid(), pid(), state()) -> state().
remove_listener(Pid, Channel, State) ->
    #{listeners := Listeners, channels := Channels, refs := Refs} = State,
    case maps:get(Pid, Listeners, undefined) of
        undefined -> State;
        #{channel := Channel} -> State;
        #{channel := OldChannel, monitor := ListenerMonitor} ->
            erlang:demonitor(ListenerMonitor),
            #{monitor := ChannelMonitor} = maps:get(OldChannel, Channels),
            erlang:demonitor(ChannelMonitor),
            NewRefs = maps:without([ListenerMonitor, ChannelMonitor], Refs),
            NewListeners = maps:without([Pid], Listeners),
            NewChannels = maps:without([OldChannel], Channels),
            State#{refs => NewRefs, listeners => NewListeners, channels => NewChannels}
    end.

-spec add_listener(pid(), pid(), state()) -> state().
add_listener(Pid, Channel, State0) ->
    State = remove_listener(Pid, Channel, State0),
    #{listeners := Listeners, channels := Channels, refs := Refs} = State,

    PidRef = erlang:monitor(process, Pid),
    ChannelRef = erlang:monitor(process, Channel),
    NewRefs0 = maps:put(PidRef, #{listener => Pid}, Refs),
    NewRefs = maps:put(ChannelRef, #{channel => Channel}, NewRefs0),

    NewListeners = maps:put(Pid, #{channel => Channel, monitor => PidRef}, Listeners),
    NewChannels = maps:put(Channel, #{pid => Pid, monitor => ChannelRef}, Channels),

    set_channels(maps:keys(NewChannels)),
    maybe_start_monitor(),
    State#{listeners => NewListeners, channels => NewChannels, refs => NewRefs}.

maybe_start_monitor() ->
    maybe_start_monitor(whereis(mod_com_kazoo_monitor)).
maybe_start_monitor(undefined) ->
    _ = mod_com_kazoo_sup:start_monitor(),
    ok;
maybe_start_monitor(_) -> ok.

-spec init_queues(state()) -> state().
init_queues(#{workers := Workers, queue := Queue} = State) ->
    start_listeners(Workers, Queue),
    State.

-spec start_listeners(pos_integer(), kz_term:ne_binary()) -> ok.
start_listeners(Workers, Queue) ->
    start_listeners(Workers, Queue, self()).


-spec start_listeners(pos_integer(), kz_term:ne_binary(), pid()) -> ok.
start_listeners(Workers, Queue, Self) ->
    lists:foreach(fun(_) -> start_listener(Self, Queue) end, lists:seq(1, Workers)).

-spec start_listener(pid(), kz_term:ne_binary()) -> kz_types:startlink_ret().
start_listener(Self, Queue) ->
    mod_com_kazoo_listener_sup:start_listener(Self, Queue).



set_queue() ->
    Queue = list_to_binary([<<"com-kazoo-api-">>, kz_binary:rand_uuid()]),
    persistent_term:put(mod_com_kazoo_api_amqp_queue, Queue),
    Queue.

-spec queue() -> kz_term:ne_binary().
queue() -> persistent_term:get(mod_com_kazoo_api_amqp_queue).

-type api_publish_fun() :: fun((kz_term:api_terms()) -> any()).

-spec send(kz_term:api_terms(), api_publish_fun()) -> 'ok'.
send(Payload, PublisherFun) ->
    Channel = consumer_channel(),
    kz_amqp_channel:consumer_channel(channel()),
    PublisherFun([{<<"Server-ID">>, queue()} | Payload]),
    kz_amqp_channel:consumer_channel(Channel),
    'ok'.

consumer_channel() ->
    consumer_channel(kz_amqp_channel:has_channel()).

consumer_channel(false) -> undefined;
consumer_channel(true) -> kz_amqp_channel:consumer_channel().

counter() ->
    case persistent_term:get(mod_com_kazoo_api_counter, undefined) of
        undefined ->
            Ref = counters:new(1, ['write_concurrency']),
            persistent_term:put(mod_com_kazoo_api_counter, Ref),
            Ref;
        Ref ->
            Ref
    end.

next() ->
    counters:add(counter(), 1, 1),
    counters:get(counter(), 1).

set_channels(Channels) ->
    persistent_term:put(mod_com_kazoo_api_channels, Channels).

channels() ->
    persistent_term:get(mod_com_kazoo_api_channels).

channel() ->
    Channels = channels(),
    Index = next() rem length(Channels),
    lists:nth(Index + 1, Channels).

handle_canary(false, Channel, State) ->
    gen_server:cast(self(), 'init_queues'),
    handle_canary(Channel, State);
handle_canary(true, Channel, #{channels := Channels} = State) ->
    set_channels(maps:keys(Channels)),
    handle_canary(Channel, State).

handle_canary(Channel, #{refs := Refs} = State) ->
    lager:debug("canary channel is active"),
    ChannelRef = erlang:monitor(process, Channel),
    NewRefs = maps:put(ChannelRef, #{canary => Channel}, Refs),
    State#{canary => Channel, refs => NewRefs}.

handle_down(Pid, Ref, Reason, #{refs := Refs} = State) ->
    case maps:get(Ref, Refs, undefined) of
        undefined ->
            lager:warning("received down (~p/~p/~p) => unmanaged", [Pid, Ref, Reason]),
            State;
        Managed ->
            handle_down(Pid, Ref, Reason, Managed, State)
    end.

handle_down(Pid, Ref, Reason, #{canary := Pid}, #{refs := Refs, canary := Pid} = State) ->
    lager:debug("received down (~p/~p/~p) for canary channel, we're closing until we get it back", [Pid, Ref, Reason]),
    set_channels([]),
    NewRefs = maps:without([Ref], Refs),
    State#{refs => NewRefs};
handle_down(Pid, Ref, Reason, #{listener := Pid}, #{refs := Refs, listeners := Listeners, channels := Channels} = State) ->
    lager:warning("received down (~p/~p/~p) for listener, this is bad.", [Pid, Ref, Reason]),
    #{channel := Channel} = maps:get(Pid, Listeners),
    NewRefs = maps:without([Ref], Refs),
    NewListeners = maps:without([Pid], Listeners),
    NewChannels = maps:without([Channel], Channels),
    set_channels(NewChannels),
    State#{refs => NewRefs, listeners => NewListeners, channels => NewChannels};
handle_down(Pid, Ref, Reason, #{channel := Pid}, #{refs := Refs, channels := Channels} = State) ->
    lager:warning("received down (~p/~p/~p) for channel, this is bad.", [Pid, Ref, Reason]),
    NewRefs = maps:without([Ref], Refs),
    NewChannels = maps:without([Pid], Channels),
    set_channels(NewChannels),
    State#{refs => NewRefs, channels => NewChannels}.
