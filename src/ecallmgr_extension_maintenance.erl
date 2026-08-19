%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2026, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_extension_maintenance).

-include("ecallmgr_extension.hrl").

-export([install_licenses/1]).
-export([install_license/3
        ,install_license/4
        ]).
-export([check_licenses/0]).
-export([check_license/1]).

-define(LICENSES_CAT, <<"licenses">>).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec install_licenses(file:name_all()) -> 'no_return'.
install_licenses(Filename) ->
    {'ok', Contents} = file:read_file(Filename),
    JObj = kz_json:decode(Contents),
    Features = kz_json:get_keys(?APP_NAME, JObj),
    install_license(JObj, Features).

-spec install_license(kz_json:object(), kz_term:ne_binaries()) -> 'no_return'.
install_license(_JObj, []) ->
    'no_return';
install_license(JObj, [Feature|Features]) ->
    License = kz_json:get_ne_binary_value([?APP_NAME, Feature, <<"license">>], JObj),
    Type = kz_json:get_ne_binary_value([?APP_NAME, Feature, <<"type">>], JObj, <<"prime">>),
    Expiration = kz_json:get_integer_value([?APP_NAME, Feature, <<"expiration">>], JObj),
    _ = install_license(Feature, Expiration, Type, License),
    install_license(JObj, Features).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec install_license(kz_term:ne_binary(), integer(), kz_term:ne_binary()) -> 'no_return'.
install_license(Feature, Expiration, License) ->
    install_license(Feature, Expiration, <<"prime">>, License).

-spec install_license(kz_term:ne_binary(), integer(), kz_term:ne_binary(), kz_term:ne_binary()) -> 'no_return'.
install_license(Feature, Expiration, Type, License) ->
    _ = kapps_config:set_string(?LICENSES_CAT, [?APP_NAME, Feature, <<"license">>], License),
    _ = kapps_config:set_string(?LICENSES_CAT, [?APP_NAME, Feature, <<"type">>], Type),
    _ = kapps_config:set_integer(?LICENSES_CAT, [?APP_NAME, Feature, <<"expiration">>], Expiration),
    check_license(Feature).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec check_licenses() -> 'no_return'.
check_licenses() ->
    _ = case kapps_config:get_json(?LICENSES_CAT, ?APP_NAME) of
            'undefined' ->
                io:format("no licenses found~n", []);
            JObj ->
                [check_license(Feature)
                 || Feature <- kz_json:get_keys(JObj)
                ]
        end,
    'no_return'.

-spec check_license(kz_term:ne_binary()) -> 'no_return'.
check_license(Feature) ->
    Expiration = pretty_print_expiration(Feature),
    io:format("license checks are no longer enforced for ~s (expiration on file: ~s)~n"
             ,[Feature, Expiration]
             ),
    'no_return'.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec pretty_print_expiration(kz_term:ne_binary()) -> kz_term:ne_binary().
pretty_print_expiration(Feature) ->
    case kapps_config:get_integer(?LICENSES_CAT, [?APP_NAME, Feature, <<"expiration">>]) of
        'undefined' ->
            <<"UNKNOWN">>;
        Expiration->
            kz_time:pretty_print_datetime(
              calendar:gregorian_seconds_to_datetime(Expiration)
             )
    end.
