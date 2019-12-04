%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2019, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(ecallmgr_extension_util).

-include("ecallmgr_extension.hrl").

-export([authenticate/1]).

-define(LICENSES_CAT, <<"licenses">>).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec authenticate(kz_term:ne_binary()) -> boolean().
authenticate(Feature) ->
    Salt = <<"7f1d655ebb494a2a8773554b551e2eb7">>,
    Expiration = kapps_config:get_integer(?LICENSES_CAT, [?APP_NAME, Feature, <<"expiration">>]),
    Type = kapps_config:get_ne_binary(?LICENSES_CAT, [?APP_NAME, Feature, <<"type">>]),
    {'ok', MasterAccountId} = kapps_util:get_master_account_id(),
    License = <<(kz_term:to_binary(Type))/binary
               ,":", Salt/binary
               ,":", MasterAccountId/binary
               ,":", Salt/binary
               ,":", (kz_term:to_binary(Feature))/binary
               ,":", Salt/binary
               ,":", (kz_term:to_binary(Expiration))/binary
              >>,
    Hash = kz_binary:hexencode(
             crypto:hash('sha256', License)
            ),
    CurrentTime = kz_time:now_s(),
    CurrentTime =< calendar:datetime_to_gregorian_seconds({{2020, 1, 5}, {0, 0, 0}})
        orelse (kapps_config:get_ne_binary(?LICENSES_CAT, [?APP_NAME, Feature, <<"license">>]) =:= Hash
                andalso Expiration >= CurrentTime).
