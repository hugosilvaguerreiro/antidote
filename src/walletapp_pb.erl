%% @todo Add transaction like operations - buy voucher and reduce
%%       balance

-module(walletapp_pb).

-export([credit/3,
         debit/3,
         getbalance/2,
         buyvoucher/3,
         usevoucher/3,
         readvouchers/2]).

-include("floppy.hrl").

-spec credit(key(), non_neg_integer(), pid()) -> ok | {error, reason()}.
credit(Key, Amount, Pid) ->
    case floppyc_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid) of
        {ok, Counter} ->
            CounterUpdt = floppyc_counter:increment(Amount, Counter),
            case floppyc_pb_socket:store_crdt(CounterUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
             {error, Reason}
    end.

-spec debit(key(), non_neg_integer(), pid()) -> ok | {error, reason()}.
debit(Key, Amount, Pid) ->
    case floppyc_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid) of
        {ok,Counter} ->
            CounterUpdt = floppyc_counter:decrement(Amount, Counter),
            case floppyc_pb_socket:store_crdt(CounterUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec getbalance(key(), pid()) -> {error, reason()} | {ok, integer()}.
getbalance(Key, Pid) ->
    case floppyc_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid) of
        {ok,Counter} ->
            {ok, floppyc_counter:value(Counter)};
        {error, _Reason} ->
            {error, error_in_read}
    end.

-spec buyvoucher(key(), term(), pid()) -> ok | {error, reason()}.
buyvoucher(Key, Voucher, Pid) ->
    case floppyc_pb_socket:get_crdt(Key, riak_dt_orset, Pid) of
        {ok, Set} ->
            SetUpdt = floppyc_set:add(Voucher,Set),
            case floppyc_pb_socket:store_crdt(SetUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                     {error, Reason}
           end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec usevoucher(key(), term(), pid()) -> ok | {error, reason()}.
usevoucher(Key, Voucher, Pid) ->
    case floppyc_pb_socket:get_crdt(Key, riak_dt_orset, Pid) of
        {ok, Set} ->
            SetUpdt = floppyc_set:remove(Voucher, Set),
            case floppyc_pb_socket:store_crdt(SetUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec readvouchers(key(), pid()) -> {ok, list()} | {error, reason()}.
readvouchers(Key, Pid) ->
    case floppyc_pb_socket:get_crdt(Key, riak_dt_orset, Pid) of
        {ok, Set} ->
            {ok, sets:to_list(floppyc_set:value(Set))};
        {error, Reason} ->
            {error, Reason}
    end.