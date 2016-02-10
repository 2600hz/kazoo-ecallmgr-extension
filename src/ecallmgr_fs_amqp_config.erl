%%%-------------------------------------------------------------------
%%% @copyright (C) 2012-2015, 2600Hz INC
%%% @doc
%%% Send amqp config to freeswitch
%%% @end
%%% @contributors
%%%-------------------------------------------------------------------
-module(ecallmgr_fs_amqp_config).

-export([handle_config_req/4]).

-include("../../ecallmgr/src/ecallmgr.hrl").
-include_lib("whistle/include/wh_amqp.hrl").


-define(AMQP_CAPABILITY, wh_json:from_list([{<<"module">>, <<"mod_amqp">>}
                                            ,{<<"is_loaded">>, 'true'}
                                            ,{<<"capability">>, <<"amqp">>}
                                           ])).
%%%===================================================================
%%% API
%%%===================================================================

-spec handle_config_req(atom(), ne_binary(), ne_binary(), wh_proplist() | 'undefined') -> fs_sendmsg_ret().
handle_config_req(Node, Id, <<"amqp.conf">>, _Props) ->
    wh_util:put_callid(Id),
    case ecallmgr_config:get(<<"amqp">>) of
        'undefined' ->
            {'ok', Resp} = ecallmgr_fs_xml:not_found(),
            freeswitch:fetch_reply(Node, Id, 'configuration', iolist_to_binary(Resp));
        JObj ->
            try amqp_conf_xml(JObj) of
                {'ok', ConfigXml} ->
                    lager:debug("sending amqp XML to ~s: ~s", [Node, ConfigXml]),
                    ecallmgr_fs_nodes:add_capability(Node, ?AMQP_CAPABILITY),
                    freeswitch:fetch_reply(Node, Id, 'configuration', erlang:iolist_to_binary(ConfigXml))
            catch
                _E:_R ->
                    lager:info("amqp configuration resp failed to convert to XML (~s): ~p", [_E, _R]),
                    wh_util:log_stacktrace(),
                    {'ok', Resp} = ecallmgr_fs_xml:not_found(),
                    freeswitch:fetch_reply(Node, Id, 'configuration', iolist_to_binary(Resp))
            end
    end;
handle_config_req(Node,  Id, _Conf, _) ->
    wh_util:put_callid(Id),
    lager:debug("ignoring conf ~s: ~s", [_Conf, Id]),
    {'ok', Resp} = ecallmgr_fs_xml:not_found(),
    freeswitch:fetch_reply(Node, Id, 'configuration', iolist_to_binary(Resp)).

-spec amqp_conf_xml(wh_json:object()) -> {'ok', iolist()}.
amqp_conf_xml(JObj) ->
    ProfilesEl = amqp_conf_el(JObj),
    ConfigEl = ecallmgr_fs_xml:config_el(<<"amqp.conf">>, ProfilesEl),
    SectionEl = ecallmgr_fs_xml:section_el(<<"configuration">>, ConfigEl),
    {'ok', xmerl:export([SectionEl], 'fs_xml')}.

amqp_conf_el(JObj) ->
    lists:foldl(fun(Key, Xml) ->
                        Part = wh_json:get_value(Key, JObj),
                        [#xmlElement{name=wh_util:to_atom(Key, 'true')
                                     ,content=amqp_part_el(Part)
                                    }
                         | Xml
                        ]
                end, [], wh_json:get_keys(JObj)).

amqp_part_el(JObj) ->
    lists:foldl(fun(Key, Xml) ->
                        Profile = wh_json:get_value(Key, JObj),
                        [#xmlElement{name='profile'
                                     ,attributes=[ecallmgr_fs_xml:xml_attrib('name', Key)]
                                     ,content=amqp_profile_el(Profile)
                                    }
                         | Xml
                        ]
                end, [], wh_json:get_keys(JObj)).

amqp_profile_el(JObj) ->
    [amqp_connections_el()
     ,#xmlElement{name='params'
                  ,content=amqp_settings_el(JObj)
                 }
    ].

amqp_settings_el(JObj) ->
    EventFilter = amqp_event_filter(JObj),
    Params = wh_json:set_value(<<"event_filter">>, EventFilter, wh_json:get_value(<<"params">>, JObj, wh_json:new())),
    lists:foldl(fun(Key, Xml) ->
                        Value = wh_json:get_binary_value(Key, Params),
                        Name = wh_util:to_lower_binary(Key),
                        [#xmlElement{name='param'
                                     ,attributes=[ecallmgr_fs_xml:xml_attrib('name', Name)
                                                  ,ecallmgr_fs_xml:xml_attrib('value', Value)
                                                 ]
                                    }
                         | Xml
                        ]
                end, [], wh_json:get_keys(Params)).

amqp_event_filter(JObj) ->
    Events = wh_json:get_value(<<"events">>, JObj, []),
    EventFilter = lists:foldl(fun amqp_event_filter_fold/2, [], Events),
    wh_util:to_binary(string:join(EventFilter, ",")).

amqp_event_filter_fold(Ev, Acc) ->
    [ wh_util:to_list(<<"SWITCH_EVENT_", Ev/binary>>) | Acc].

amqp_connections_el() ->
    #xmlElement{name='connections'
                ,content=[amqp_connection_el()]
               }
    .

amqp_connection_el() ->
    #xmlElement{name='connection'
                ,content=amqp_connection_params_el()
                ,attributes=[ecallmgr_fs_xml:xml_attrib('name', <<"primary">>)]
               }
    .

amqp_connection_params_el() ->
    Broker = wh_amqp_connections:primary_broker(),
    {'ok', #'amqp_params_network'{username=Username
                           ,password=Password
                           ,virtual_host=VHost
                           ,host=Host
                           ,port=Port
                          }} = amqp_uri:parse(wh_util:to_list(Broker)),
    lists:filter(fun wh_util:is_not_empty/1,
                 [ecallmgr_fs_xml:param_el("hostname", Host)
                  ,ecallmgr_fs_xml:maybe_param_el("virtualhost", VHost)
                  ,ecallmgr_fs_xml:maybe_param_el("username", Username)
                  ,ecallmgr_fs_xml:maybe_param_el("password", Password)
                  ,ecallmgr_fs_xml:maybe_param_el("port", Port)
                 ]).
