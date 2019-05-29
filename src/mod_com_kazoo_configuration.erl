%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc Send config commands to FS
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(mod_com_kazoo_configuration).

%% API
-export([init/0]).

-export([kazoo/1]).

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
    kazoo_bindings:bind(<<"fetch.configuration.commercial.*.kazoo.conf">>, ?MODULE, 'kazoo'),
    'ok'.

-spec kazoo(map()) -> fs_sendmsg_ret().
kazoo(#{core_uuid := Node, fetch_id := Id, payload := JObj} = Ctx) ->
    kz_util:put_callid(Id),
    lager:debug("received configuration request for kazoo configuration ~p , ~p : ~s", [Node, Id, kz_json:encode(JObj, ['pretty'])]),
    fetch_mod_kazoo_config(kz_api:event_name(JObj), Ctx).


-spec fetch_mod_kazoo_config(kz_term:ne_binary(), map()) -> fs_sendmsg_ret().
fetch_mod_kazoo_config(<<"COMMAND">>, #{payload := _JObj} = Ctx) ->
    lager:debug_unsafe("kazoo conf request : ~s", [kz_json:encode(_JObj, ['pretty'])]),
    config_req_not_handled(Ctx);
fetch_mod_kazoo_config(<<"REQUEST_PARAMS">>, #{payload := JObj} = Ctx) ->
    Action = kz_json:get_ne_binary_value(<<"Action">>, JObj),
    fetch_mod_kazoo_config_action(Action, Ctx);
fetch_mod_kazoo_config(<<"GENERAL">>, #{payload := _JObj} = Ctx) ->
    Path = list_to_binary([code:priv_dir(?APP), "/defaults/configuration/kazoo.conf.xml"]),
    {ok, Bin} = file:read_file(Path),
    Resp = <<"<document type=\"freeswitch/xml\"><section name=\"configuration\">", Bin/binary, "</section></document>">>,
    freeswitch:fetch_reply(Ctx#{reply => Resp});
fetch_mod_kazoo_config(Event, #{core_uuid := Node} = Ctx) ->
    lager:debug("unhandled mod kazoo config event : ~p : ~p", [Node, Event]),
    config_req_not_handled(Ctx).

-spec config_req_not_handled(map()) -> fs_sendmsg_ret().
config_req_not_handled(Ctx) ->
    {'ok', NotHandled} = ecallmgr_fs_xml:not_found(),
    freeswitch:fetch_reply(Ctx#{reply => iolist_to_binary(NotHandled)}).

-spec fetch_mod_kazoo_config_action(kz_term:api_ne_binary(), map()) -> fs_sendmsg_ret().
fetch_mod_kazoo_config_action('undefined', Ctx) ->
    config_req_not_handled(Ctx);
fetch_mod_kazoo_config_action(Action, #{core_uuid := Node} = Ctx) ->
    lager:debug("unhandled mod kazoo config action : ~p : ~p", [Node, Action]),
    config_req_not_handled(Ctx).
