%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2020, 2600Hz
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
        ,config_el/3
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
    lager:debug_unsafe("kazoo conf request: ~s", [kz_json:encode(_JObj)]),
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

-spec build_kazoo_config() -> {'ok', binary()}.
build_kazoo_config() ->
    ProfileFiles = filelib:wildcard(code:priv_dir(?APP) ++ "/mod_kazoo/event_profiles/*.xml"),
    {DefFiles0, Profiles} = lists:foldr(fun fs_profile_handler/2, {[], []}, ProfileFiles),

    FetchFiles = filelib:wildcard(code:priv_dir(?APP) ++ "/mod_kazoo/fetch/*.xml"),
    {DefFiles1, FetchProfiles} = lists:foldr(fun fs_handler/2, {DefFiles0, []}, FetchFiles),

    APIFiles = filelib:wildcard(code:priv_dir(?APP) ++ "/mod_kazoo/api/*.xml"),
    {DefFiles2, APIProfiles} = lists:foldr(fun fs_handler/2, {DefFiles1, []}, APIFiles),

    AMQPFiles = filelib:wildcard(code:priv_dir(?APP) ++ "/mod_kazoo/amqp_profiles/*.xml"),
    {DefFiles, AMQPProfiles} = lists:foldr(fun fs_handler/2, {DefFiles2, []}, AMQPFiles),

    Defs0 = lists:foldl(fun one_def/2, [], DefFiles),
    Defs = lists:map(fun fs_xml/1, Defs0),

    ConfigurationEl = config_el(<<"com.kazoo.conf">>, <<"Built by Kazoo">>
                               ,[definitions_el(Defs)
                                ,event_handlers_el(Profiles)
                                ,fetch_handlers_el(FetchProfiles)
                                ,command_handlers_el(APIProfiles)
                                ,amqp_profiles_el(AMQPProfiles)
                                ]
                               ),
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

-spec fs_profile_event(kz_types:xml_el(), {kz_types:xml_els(), kz_types:xml_els()}) -> {kz_types:xml_els(), kz_types:xml_els()}.
fs_profile_event(ProfileEventXml, {DefFiles, EventXmls}) ->
    [NameAttr] = xmerl_xpath:string("@name", ProfileEventXml),
    RoutingKey = xmerl_xpath:string("routing-key", ProfileEventXml),
    EventFile = fs_evt_filename(NameAttr),
    Tmp = #xmlElement{content = Content} = fs_xml(EventFile),
    EventXml = Tmp#xmlElement{content = Content ++ RoutingKey},
    {fs_defs(EventXml, DefFiles), [EventXml | EventXmls]}.

-spec fs_handler(file:filename_all(), {kz_types:xml_els(), kz_types:xml_els()}) -> {kz_types:xml_els(), kz_types:xml_els()}.
fs_handler(EventFile, {DefFiles, EventXmls}) ->
    EventXml = fs_xml(EventFile),
    {fs_defs(EventXml, DefFiles), [EventXml | EventXmls]}.

-spec fs_defs(kz_types:xml_el(), kz_types:xml_els()) -> kz_types:xml_els().
fs_defs(XmlEl, Acc) ->
    RefFileList = lists:map(fun fs_def_filename/1, xmerl_xpath:string("//field[@type='reference']/@name", XmlEl)),
    RefXmls = lists:map(fun fs_xml/1, RefFileList),
    lists:foldl(fun fs_defs/2, [], RefXmls) ++ RefFileList ++ Acc.

-spec fs_xml(file:filename_all()) -> kz_types:xml_el().
fs_xml(File) ->
    {Xml, _} = xmerl_scan:file(File),
    Xml.

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

-spec definitions_el(kz_types:xml_els()) -> kz_types:xml_el().
definitions_el(Content) ->
    #xmlElement{name='definitions'
               ,content=Content
               }.

-spec event_handlers_el(kz_types:xml_els()) -> kz_types:xml_el().
event_handlers_el(Content) ->
    #xmlElement{name='event-handlers'
               ,content=Content
               }.

-spec fetch_handlers_el(kz_types:xml_els()) -> kz_types:xml_el().
fetch_handlers_el(Content) ->
    #xmlElement{name='fetch-handlers'
               ,content=Content
               }.

-spec command_handlers_el(kz_types:xml_els()) -> kz_types:xml_el().
command_handlers_el(Content) ->
    #xmlElement{name='command-handlers'
               ,content=Content
               }.

-spec amqp_profiles_el(kz_types:xml_els()) -> kz_types:xml_el().
amqp_profiles_el(Content) ->
    #xmlElement{name='amqp-profiles'
               ,content=Content
               }.
