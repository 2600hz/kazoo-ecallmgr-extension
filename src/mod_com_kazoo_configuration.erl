%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2022, 2600Hz
%%% @doc Send config commands to FS
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_configuration).

%% API
-export([init/0]).

-export([kazoo/1]).

-export([build_kazoo_config/0]).
-export([kazoo_config/0]).

-import(ecallmgr_fs_xml
       ,[section_el/2
        ,param_el/2
        ,xml_attrib/2
        ,variable_el/2
        ,variables_el/1
        ]).

-include("ecallmgr_extension.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================


%%------------------------------------------------------------------------------
%% @doc Initializes the bindings
%% @end
%%------------------------------------------------------------------------------
-spec init() -> 'ok'.
init() ->
    _ = kazoo_bindings:bind(<<"fetch.configuration.configuration.commercial.*.com.kazoo.conf">>, ?MODULE, 'kazoo'),
    'ok'.

-spec kazoo(map()) -> fs_sendmsg_ret().
kazoo(#{fetch_id := Id, payload := JObj} = Ctx) ->
    kz_log:put_callid(Id),
    fetch_mod_kazoo_config(kz_api:event_name(JObj), Ctx).

-spec fetch_mod_kazoo_config(kz_term:ne_binary(), map()) -> fs_sendmsg_ret().
fetch_mod_kazoo_config(<<"COMMAND">>, #{payload := _JObj} = Ctx) ->
    lager:debug_unsafe("kazoo conf request : ~s", [kz_json:encode(_JObj, ['pretty'])]),
    config_req_not_handled(Ctx);
fetch_mod_kazoo_config(<<"REQUEST_PARAMS">>, #{payload := JObj} = Ctx) ->
    Action = kz_json:get_ne_binary_value(<<"Action">>, JObj),
    fetch_mod_kazoo_config_action(Action, Ctx);
fetch_mod_kazoo_config(<<"GENERAL">>, #{payload := _JObj} = Ctx) ->
    try kazoo_config() of
        {'ok', Xml} ->
            mod_com_kazoo:fetch_reply(Ctx#{reply => Xml})
    catch
        _Ex:_Er:ST ->
            kz_log:log_stacktrace(ST),
            config_req_not_handled(Ctx)
    end;
fetch_mod_kazoo_config(Event, #{core_uuid := Node} = Ctx) ->
    lager:debug("unhandled mod kazoo config event : ~p : ~p", [Node, Event]),
    config_req_not_handled(Ctx).

-spec config_req_not_handled(map()) -> fs_sendmsg_ret().
config_req_not_handled(Ctx) ->
    {'ok', NotHandled} = ecallmgr_fs_xml:not_found(),
    mod_com_kazoo:fetch_reply(Ctx#{reply => iolist_to_binary(NotHandled)}).

-spec fetch_mod_kazoo_config_action(kz_term:api_ne_binary(), map()) -> fs_sendmsg_ret().
fetch_mod_kazoo_config_action('undefined', Ctx) ->
    config_req_not_handled(Ctx);
fetch_mod_kazoo_config_action(Action, #{core_uuid := Node} = Ctx) ->
    lager:debug("unhandled mod kazoo config action : ~p : ~p", [Node, Action]),
    config_req_not_handled(Ctx).

-spec kazoo_config() -> {'ok', binary()}.
kazoo_config() ->
    {ok, persistent_term:get(mod_com_kazoo_xml_config, <<>>)}.

-type build_content_acc() :: {[file:filename_all()], map()}.
-type build_content_fun_acc() :: {[file:filename_all()], kz_types:xml_els()}.
-type build_content_fun() :: fun((kz_types:xml_el(), build_content_fun_acc()) -> build_content_fun_acc()).
-type build_content_arg() :: {atom, build_content_fun(), string()}.

-spec build_content() -> map().
build_content() ->
    Funs = [{event_handlers_el, fun fs_profile_handler/2, "event_profiles"}
           ,{fetch_handlers_el, fun fs_handler/2, "fetch"}
           ,{command_handlers_el, fun fs_handler/2, "api"}
           ,{amqp_profiles_el, fun fs_handler/2, "amqp_profiles"}
           ,{others_el, fun fs_handler/2, "others"}
           ],
    {DefFiles, Map} = lists:foldl(fun build_content/2, {[], maps:new()}, Funs),
    Map#{definitions_el => definitions(DefFiles)}.

-spec build_content(build_content_arg(), build_content_acc()) -> build_content_acc().
build_content({Key, Fun, Directory}, {DefFiles, Map}) ->
    Files = filelib:wildcard(code:priv_dir(?APP) ++ "/mod_kazoo/" ++ Directory ++ "/*.xml"),
    {NewDefFiles, Value} = lists:foldr(Fun, {DefFiles, []}, Files),
    {NewDefFiles,  Map#{Key => Value}}.

definitions(DefFiles) ->
    Defs = lists:foldl(fun one_def/2, [], DefFiles),
    lists:map(fun fs_xml/1, Defs).

-spec build_content_fun(map()) -> fun().
build_content_fun(Map) ->
    fun(Fun, Acc) ->
            build_content_fun(Fun, Acc, Map)
    end.

-type build_content_fun_arg() :: fun() | atom() | {fun(), atom()}.

-spec build_content_fun(build_content_fun_arg(), kz_types:xml_els(), map()) -> kz_types:xml_els().
build_content_fun(Fun, Acc, _Map)
  when is_function(Fun, 0) ->
    build_content_fun_result(Fun(), Acc);
build_content_fun(Fun, Acc, Map)
  when is_function(Fun, 1) ->
    build_content_fun_result(Fun(Map), Acc).

build_content_fun_result(undefined, Acc) -> Acc;
build_content_fun_result(#xmlElement{} = Val, Acc) -> [Val | Acc];
build_content_fun_result([#xmlElement{} | _] = Val, Acc) -> Val ++ Acc.


-spec build_kazoo_config() -> {'ok', binary()}.
build_kazoo_config() ->
    Content = build_content(),

    Funs = [fun pre_process/0
           ,fun definitions_el/1
           ,fun event_handlers_el/1
           ,fun fetch_handlers_el/1
           ,fun command_handlers_el/1
           ,fun amqp_profiles_el/1
           ,fun connections_el/0
           ,fun others_el/1
           ,fun variables_el/0
           ,fun caches_el/0
           ],
    ConfigurationContent = lists:foldr(build_content_fun(Content) , [], Funs),
    ConfigurationEl = mod_kazoo_config_el(ConfigurationContent),
    SectionEl = section_el(<<"configuration">>, ConfigurationEl),
    Xml = xmerl:export([SectionEl], 'fs_xml'),
    Config = iolist_to_binary(Xml),
    persistent_term:put(mod_com_kazoo_xml_config, Config),
    {ok, Config}.

fs_profile_handler(ProfileFile, {DefFiles0, ProfileXmls}) ->
    {DefFiles, ProfileXml} = fs_profile_events(fs_xml(ProfileFile), DefFiles0),
    {DefFiles, [ProfileXml | ProfileXmls]}.

fs_profile_events(XmlEl, DefFiles0) ->
    RefFileList = xmerl_xpath:string("/profile/events/event", XmlEl),
    {DefFiles, EventXmls} = lists:foldl(fun fs_profile_event/2, {DefFiles0, []}, RefFileList),
    #xmlElement{name='profile', content=Xmls} = XmlEl,
    Fun = fun(#xmlElement{name='events'}) -> #xmlElement{name='events', content=EventXmls};
             (Xml) -> Xml
          end,
    {DefFiles, XmlEl#xmlElement{content=lists:map(Fun, Xmls)}}.

-spec fs_profile_event(kz_types:xml_el(), {[file:filename_all()], kz_types:xml_els()}) -> {[file:filename_all()], kz_types:xml_els()}.
fs_profile_event(ProfileEventXml, {DefFiles, EventXmls}) ->
    [NameAttr] = xmerl_xpath:string("@name", ProfileEventXml),
    RoutingKey = xmerl_xpath:string("routing-key", ProfileEventXml),
    Logging = xmerl_xpath:string("logging", ProfileEventXml),
    Flags = xmerl_xpath:string("flags", ProfileEventXml),
    EventFile = fs_evt_filename(NameAttr),
    Tmp = #xmlElement{content = Content} = fs_xml(EventFile),
    EventXml = Tmp#xmlElement{content = Content ++ RoutingKey ++ Logging ++ Flags},
    {fs_defs(EventXml, DefFiles), [EventXml | EventXmls]}.

-spec fs_handler(file:filename_all(), {[file:filename_all()], kz_types:xml_els()}) -> {[file:filename_all()], kz_types:xml_els()}.
fs_handler(EventFile, {DefFiles, EventXmls}) ->
    EventXml = fs_xml(EventFile),
    {fs_defs(EventXml, DefFiles), [EventXml | EventXmls]}.

-spec fs_defs(kz_types:xml_el(), kz_types:xml_els()) -> [file:filename_all()].
fs_defs(XmlEl, Acc) ->
    RefFileList = lists:map(fun fs_def_filename/1, xmerl_xpath:string("//field[@type='reference']/@name", XmlEl)),
    RefXmls = lists:map(fun fs_xml/1, RefFileList),
    lists:foldl(fun fs_defs/2, [], RefXmls) ++ RefFileList ++ Acc.

-spec fs_xml(file:filename_all()) -> kz_types:xml_el().
fs_xml(File) ->
    case xmerl_scan:file(re:replace(File, "::", "-", ['global'])) of
        {error, _Err} -> throw({invalid_configuration, lists:flatten(io_lib:format("error reading file : ~s : ~p", [File, _Err]))});
        {Xml, _} -> Xml
    end.

-spec fs_def_filename(kz_types:xml_attrib() | string()) -> file:filename_all().
fs_def_filename(Name) ->
    fs_filename("/mod_kazoo/definitions/", Name).

-spec fs_evt_filename(kz_types:xml_attrib() | string()) -> file:filename_all().
fs_evt_filename(Name) ->
    fs_filename("/mod_kazoo/events/", Name).

-spec fs_filename(string(), kz_types:xml_attrib() | string()) -> file:filename_all().
fs_filename(Path, #xmlAttribute{name='name', value=Name}) ->
    fs_filename(Path, Name);
fs_filename(Path, Name0) ->
    Name = kz_term:to_list(iolist_to_binary(re:replace(Name0, "::", "-", ['global']))),
    AppFile = code:priv_dir(?APP) ++ Path ++ Name ++ ".xml",
    case filelib:is_regular(AppFile) of
        'true' -> AppFile;
        'false' -> code:priv_dir('ecallmgr') ++ Path ++ Name ++ ".xml"
    end.

-spec one_def(file:filename_all(), [file:filename_all()]) -> [file:filename_all()].
one_def(File, Acc) ->
    case lists:member(File, Acc) of
        'true' -> Acc;
        'false' -> Acc ++ [File]
    end.

-spec definitions_el(map()) -> kz_types:xml_el().
definitions_el(#{definitions_el := Content}) ->
    #xmlElement{name='definitions'
               ,content=Content
               }.

-spec event_handlers_el(map()) -> kz_types:xml_el().
event_handlers_el(#{event_handlers_el := Content}) ->
    #xmlElement{name='event-handlers'
               ,content=Content
               }.

-spec fetch_handlers_el(map()) -> kz_types:xml_el().
fetch_handlers_el(#{fetch_handlers_el := Content}) ->
    #xmlElement{name='fetch-handlers'
               ,content=Content
               }.

-spec command_handlers_el(map()) -> kz_types:xml_el().
command_handlers_el(#{command_handlers_el := Content}) ->
    #xmlElement{name='command-handlers'
               ,content=Content
               }.

-spec amqp_profiles_el(map()) -> kz_types:xml_el().
amqp_profiles_el(#{amqp_profiles_el := Content}) ->
    #xmlElement{name='amqp-profiles'
               ,content=Content
               }.

-spec connections_el() -> kz_types:xml_el() | undefined.
connections_el() ->
    connections_el(kz_app_config:is_true(?APP_CONFIG_CAT, <<"disable_media_amqp_connections">>, false)).

-spec connections_el(Disabled :: boolean()) -> kz_types:xml_el() | undefined.
connections_el(false) ->
    LocalZone = kz_nodes:local_zone(),
    Connections = lists:filtermap(fun connection_filtermap/1, kz_amqp_connections:connections()),
    connections_el(LocalZone, Connections);
connections_el(true) -> undefined.


-spec connections_el(atom() | binary(), kz_types:xml_els()) -> kz_types:xml_el().
connections_el(LocalZone, Content) ->
    #xmlElement{name='connections'
               ,attributes=[xml_attrib('local-zone', kz_term:to_binary(LocalZone))]
               ,content=Content
               }.

-spec connection_el(atom() | binary(), kz_types:xml_els()) -> kz_types:xml_el().
connection_el(Name, Content) ->
    #xmlElement{name='connection'
               ,attributes=[xml_attrib('name', kz_term:to_binary(Name))]
               ,content=Content
               }.

connection_param({K, V}) ->
    param_el(K, kz_term:to_binary(V)).

connection_params(_, {error, _}) -> false;
connection_params(Zone, {ok, #amqp_params_network{host = Hostname
                                                 ,port = Port
                                                 ,virtual_host = VirtualHost
                                                 ,username = Username
                                                 ,password = Password
                                                 }}) ->
    Props = [{hostname, Hostname}
            ,{port, Port}
            ,{virtualhost, VirtualHost}
            ,{username, Username}
            ,{password, Password}
            ,{zone, Zone}
            ],
    ConnectionParams = lists:map(fun connection_param/1, props:filter_empty(Props)),
    ConnectionProperties = connection_properties(Zone),
    Name = list_to_binary([Hostname, "-", kz_term:to_binary(Zone)]),
    {true, connection_el(Name, ConnectionParams ++ ConnectionProperties)}.

connection_filtermap(#kz_amqp_connections{broker=Broker
                                         ,zone=Zone
                                         ,hidden = false
                                         ,tags = []
                                         }) ->
    connection_params(connection_zone(Zone), amqp_uri:parse(Broker));
connection_filtermap(_) -> false.

-spec property_el(kz_types:xml_attrib_value(), kz_types:xml_attrib_value()) -> kz_types:xml_el().
property_el(Name, Value) ->
    #xmlElement{name='property'
               ,attributes=[xml_attrib('name', Name)
                           ,xml_attrib('value', Value)
                           ]
               }.

connection_properties(Zone) ->
    connection_properties(Zone, kz_nodes:local_zone()).

connection_properties(Zone, Zone) ->
    [property_el("zone", Zone)
    ,property_el("primary", true)
    ,property_el("is-federated", false)
    ,property_el("is-local", true)
    ];
connection_properties(Zone, _LocalZone) ->
    [property_el("zone", Zone)
    ,property_el("primary", false)
    ,property_el("is-federated", true)
    ,property_el("is-local", false)
    ].

connection_zone(local) -> kz_nodes:local_zone();
connection_zone(Zone) -> Zone.

-spec others_el(map()) -> kz_types:xml_els().
others_el(#{others_el := Content}) ->
    Content.

pre_process() ->
    Static = kz_json:from_list(pre_process_static_overrides()),
    Configured = pre_process_configured_overrides(),
    Variables = kz_json:set_values(kz_json:to_proplist(Configured), Static),
    lists:map(fun pre_process_override/1, kz_json:to_proplist(Variables)).

pre_process_override({K, V}) ->
    pre_process_el(K, V).

pre_process_configured_overrides() ->
    kz_app_config:get_json(?APP_CONFIG_CAT, <<"config_pre_process_overrides">>, kz_json:new()).

pre_process_static_overrides() ->
    [{<<"stun-set">>, <<"external_rtp_ip=stun:stun.l.google.com:19302">>}
    ].

pre_process_el(Cmd, Data) ->
    #xmlElement{name='X-PRE-PROCESS'
               ,attributes=[xml_attrib('cmd', Cmd)
                           ,xml_attrib('data', Data)
                           ]
               }.

variables_el() ->
    Funs = [fun global_overrides/0
           ,fun sofia_overrides/0
           ],
    Variables = lists:foldl(fun variables_fold/2, [], Funs),
    variables_el(Variables).

variables_fold(Fun, Acc) when is_function(Fun, 0) ->
    Acc ++ Fun().

global_overrides() ->
    Static = kz_json:from_list(global_static_overrides()),
    Configured = global_configured_overrides(),
    Variables = kz_json:set_values(kz_json:to_proplist(Configured), Static),
    lists:map(fun global_profile_override/1, kz_json:to_proplist(Variables)).

global_profile_override({K, V}) ->
    variable_el(K, V).

global_configured_overrides() ->
    kz_app_config:get_json(?APP_CONFIG_CAT, <<"global_variables_overrides">>, kz_json:new()).

global_static_overrides() ->
    [{<<"origination_nested_vars">>, <<"true">>}
    ].

sofia_overrides() ->
    Static = kz_json:from_list(sofia_static_overrides()),
    Configured = sofia_configured_overrides(),
    Variables = kz_json:set_values(kz_json:to_proplist(Configured), Static),
    lists:map(fun sofia_profile_override/1, kz_json:to_proplist(Variables)).

sofia_profile_override({K, V}) ->
    variable_el(<<"sofia-profile-override-", K/binary>>, V).

sofia_configured_overrides() ->
    kz_app_config:get_json(?APP_CONFIG_CAT, <<"sofia_profile_overrides">>, kz_json:new()).

sofia_static_overrides() ->
    [{<<"apply-inbound-acl-x-token">>, <<"X-FS-Auth-Token">>}
    ,{<<"apply-proxy-acl-x-token">>, <<"X-AUTH-Token">>}
    ,{<<"user-x-token-jwt-header">>, <<"X-AUTH-JWT-Token">>}
    ,{<<"enable-uuid-acl-check">>, <<"true">>}
    ,{<<"apply-proxy-acl-uuid-x-header">>, <<"X-Proxy-Core-UUID">>}
    ,{<<"apply-inbound-acl-uuid-x-header">>, <<"X-FS-Core-UUID">>}
    ,{<<"enable-core-uuid-header">>, <<"true">>}
    ,{<<"auth-calls">>, <<"true">>}
    ,{<<"auth-calls-acl-only">>, <<"true">>}
    ,{<<"auth-require-user">>, <<"true">>}
    ,{<<"disable-register">>, <<"true">>}
    ,{<<"accept-blind-auth">>, <<"false">>}
    ,{<<"accept-blind-reg">>, <<"false">>}
    ,{<<"manage-presence">>, <<"false">>}
    ,{<<"manage-shared-appearance">>, <<"false">>}
    ,{<<"channel-xml-fetch-on-nightmare-transfer">>, <<"true">>}
    ,{<<"fire-transfer-events">>, <<"true">>}
    ,{<<"keep-auth-caller-id">>, <<"true">>}
    ,{<<"enable-dynamic-outbound-proxy">>, <<"false">>}
    ].

-spec caches_el() -> kz_types:xml_el().
caches_el() ->
    #xmlElement{name='caches'
               ,content=caches()
               }.

-spec caches() -> kz_types:xml_els().
caches() ->
    Caches = kz_app_config:get_json(?APP_CONFIG_CAT, <<"caches">>, kz_json:new()),
    lists:map(fun cache/1, kz_json:to_proplist(Caches)).

-spec cache(tuple()) -> kz_types:xml_el().
cache({Name, Values}) ->
    Content = lists:map(fun cache_entry/1, kz_json:to_proplist(Values)),
    cache_el(Name, Content).

-spec cache_el(binary(), kz_types:xml_els()) -> kz_types:xml_el().
cache_el(Name, Content) ->
    #xmlElement{name='cache'
               ,attributes=[xml_attrib('name', Name)]
               ,content=Content
               }.

%% TODO
%% handle types other them string
-spec cache_entry(tuple()) -> kz_types:xml_el().
cache_entry({Key, Value})
  when is_binary(Value) ->
    cache_entry_el(Key, 'string', Value).


-spec cache_entry_el(binary(), atom(), binary()) -> kz_types:xml_el().
cache_entry_el(Key, Type, Value) ->
    #xmlElement{name='entry'
               ,attributes=[xml_attrib('key', Key)
                           ,xml_attrib('type', Type)
                           ,xml_attrib('value', Value)
                           ]
               }.

-spec mod_kazoo_config_el(kz_types:xml_els()) -> kz_types:xml_el().
mod_kazoo_config_el(Content) ->
    #xmlElement{name='configuration'
               ,attributes=[xml_attrib('name', "com.kazoo.conf")
                           ,xml_attrib('description', "Built by Kazoo")
                           ,xml_attrib('version', kz_application:version(?APP))
                           ,xml_attrib('timestamp', kz_term:to_binary(kz_time:current_unix_tstamp()))
                           ]
               ,content=Content
               }.
