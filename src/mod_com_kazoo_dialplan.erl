%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2026, 2600Hz
%%% @doc Send config commands to FS
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_dialplan).

%% API
-export([init/0]).

-export([dialplan/1]).


-import(ecallmgr_fs_xml
       ,[section_el/2
        ,context_el/2
        ,extension_el/2
        ,xml_attrib/2
        ]).

-include("ecallmgr_extension.hrl").

-define(VIEW, <<"media.dialplan/context.dialplan">>).

%%%=============================================================================
%%% API
%%%=============================================================================


%%------------------------------------------------------------------------------
%% @doc Initializes the bindings
%% @end
%%------------------------------------------------------------------------------
-spec init() -> 'ok'.
init() ->
    _ = kazoo_bindings:bind(<<"fetch.configuration.dialplan.commercial.request_params.#">>, ?MODULE, 'dialplan'),
    'ok'.

-spec dialplan(map()) -> fs_sendmsg_ret().
dialplan(#{fetch_id := Id, payload := _JObj} = Ctx) ->
    kz_log:put_callid(Id),
    Funs = [fun dialplan_keys/1
           ,fun dialplan_document/1
           ,fun dialplan_build/1
           ,fun freeswitch:fetch_reply/1
           ],
    kz_maps:exec(Funs, Ctx).

dialplan_keys(#{payload := JObj} = Ctx) ->
    Context = kzd_fetch:hunt_context(JObj),
    Extension = kzd_fetch:hunt_extension(JObj),
    Ctx#{context_key => Context, extension_key => Extension}.

dialplan_document(#{context_key := Context, extension_key := Extension} = Ctx) ->
    case kz_datamgr:get_result_doc(?KZ_CONFIG_DB, ?VIEW, [Context, Extension]) of
        {ok, JObj} -> Ctx#{dialplan => kz_maps:keys_to_atoms(kz_json:to_map(JObj))};
        {error, Error} -> Ctx#{error => Error}
    end.

dialplan_build(#{error := _Err} = Ctx) ->
    {'ok', NotHandled} = ecallmgr_fs_xml:not_found(),
    Ctx#{reply => iolist_to_binary(NotHandled)};
dialplan_build(#{context_key := Context} = Ctx) ->
    Content = build_extensions(Ctx),
    ContextEl = context_el(Context, Content),
    SectionEl = section_el(<<"configuration">>, ContextEl),
    Xml = xmerl:export([SectionEl], 'fs_xml'),
    Ctx#{reply => iolist_to_binary(Xml)}.

build_extensions(#{dialplan := #{extensions := Extensions}}) ->
    maps:fold(fun build_extension/3, [], Extensions);
build_extensions(#{extensions := Extensions}) ->
    maps:fold(fun build_extension/3, [], Extensions);
build_extensions(_Ctx) -> [].

build_extension(Extension, #{dialplan := Dialplan} = _V, Acc) ->
    Content = lists:foldr(fun dialplan/2, [], Dialplan),
    [extension_el(Extension, Content) | Acc].

attributes(Map) ->
    attributes(Map, fun attribute/2).

attributes(Map, MapFun) -> attributes(Map, MapFun, ['children']).

attributes(Map, MapFun, WithoutKeys) ->
    maps:values(maps:map(MapFun, maps:without(WithoutKeys, Map))).

attribute(Name, Value) -> xml_attrib(Name, Value).

dialplan(#{condition := Condition}, Acc) ->
    Content = lists:foldr(fun dialplan/2, [], maps:get(children, Condition, [])),
    [condition_el(attributes(Condition), Content) | Acc];
dialplan(#{action := Condition}, Acc) ->
    [action_el(attributes(Condition)) | Acc];
dialplan(#{'anti-action' := Condition}, Acc) ->
    [anti_action_el(attributes(Condition)) | Acc];
dialplan(_, Acc) -> Acc.

-spec condition_el(kz_types:xml_attribs(), kz_types:xml_els()) -> kz_types:xml_el().
condition_el(Attributes, Children) ->
    #xmlElement{name = 'condition'
               ,attributes = Attributes
               ,content = Children
               }.

-spec action_el(kz_types:xml_attribs()) -> kz_types:xml_el().
action_el(Attributes) ->
    #xmlElement{name = 'action'
               ,attributes = Attributes
               }.

-spec anti_action_el(kz_types:xml_attribs()) -> kz_types:xml_el().
anti_action_el(Attributes) ->
    #xmlElement{name = 'anti-action'
               ,attributes = Attributes
               }.
