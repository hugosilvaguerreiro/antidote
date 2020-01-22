%% -------------------------------------------------------------------
%%
%% Copyright <2013-2020> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------
-module(antidote_ets_txn_caches).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").


-export([has_prepared_txns_cache/1,
    get_prepared_txns_by_key/2,
    get_prepared_txns/1,
    get_prepared_txns_by_table/1,
    create_prepared_txns_cache/1,
    delete_prepared_txns_cache/1,
    is_prepared_txn_by_table/2,
    delete_prepared_txn_by_table/2,
    insert_prepared_txn_by_table/3,
    get_prepared_cache_name/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec has_prepared_txns_cache(partition_id()) -> true | false.
has_prepared_txns_cache(Partition) ->
    case ets:info(get_prepared_cache_name(Partition)) of
        undefined -> false;
        _ -> true
    end.

-spec get_prepared_txns_by_key(partition_id(), key()) -> list().
get_prepared_txns_by_key(Partition, Key) ->
    case ets:lookup(get_prepared_cache_name(Partition), Key) of
        [] ->
            [];
        [{Key, List}] ->
            List
    end.

-spec get_prepared_txns(partition_id()) -> list().
get_prepared_txns(Partition) ->
    get_prepared_txns_by_table(get_prepared_cache_name(Partition)).

-spec get_prepared_txns_by_table(cache_id()) -> list().
get_prepared_txns_by_table(Table) ->
    case ets:tab2list(Table) of
        [] ->
            [];
        [{Key1, List1} | Rest1] ->
            lists:foldl(fun({_Key, List}, Acc) ->
                case List of
                    [] ->
                        Acc;
                    _ ->
                        List ++ Acc
                end
                        end,
                [], [{Key1, List1} | Rest1])
    end.

-spec is_prepared_txn_by_table(cache_id(), key()) -> true | false.
is_prepared_txn_by_table(Table, Key) ->
    case ets:lookup(Table, Key) of
        [] ->
            true;
        _ ->
            false
    end.

-spec delete_prepared_txn_by_table(cache_id(), key()) -> true.
delete_prepared_txn_by_table(Table, Key) ->
    ets:delete(Table, Key).

-spec insert_prepared_txn_by_table(cache_id(), key(), list()) -> true.
insert_prepared_txn_by_table(Table, Key, List) ->
    ets:insert(Table, {Key, List}).

-spec create_prepared_txns_cache(partition_id()) -> cache_id().
create_prepared_txns_cache(Partition) ->
    case has_prepared_txns_cache(Partition) of
        false ->
            ets:new(get_prepared_cache_name(Partition),
                [set, protected, named_table, ?TABLE_CONCURRENCY]);
        true ->
            %% Other vnode hasn't finished closing tables
            ?LOG_DEBUG("Unable to open ets table in clocksi vnode, retrying"),
            timer:sleep(100),
            delete_prepared_txns_cache(Partition),
            create_prepared_txns_cache(Partition)
    end.

-spec delete_prepared_txns_cache(partition_id()) -> true.
delete_prepared_txns_cache(Partition) ->
    try
        ets:delete(get_prepared_cache_name(Partition))
    catch
        _:Reason ->
            ?LOG_ERROR("Error closing table ~p", [Reason]),
            true
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec get_prepared_cache_name(partition_id()) -> cache_id().
get_prepared_cache_name(Partition) ->
    get_cache_name(Partition, prepared).

-spec get_cache_name(partition_id(), atom()) -> cache_id().
get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(node()) ++ atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).
