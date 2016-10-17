-include_lib("antidote_utils/include/antidote_utils.hrl").

-define(BUCKET, <<"antidote">>).
-define(MASTER, antidote_vnode_master).
-define(LOGGING_MASTER, logging_vnode_master).
-define(CLOCKSI_MASTER, clocksi_vnode_master).
-define(CLOCKSI_GENERATOR_MASTER,
        clocksi_downstream_generator_vnode_master).
-define(CLOCKSI, clocksi).
-define(REPMASTER, antidote_rep_vnode_master).
-define(N, 1).
-define(OP_TIMEOUT, infinity).
-define(COORD_TIMEOUT, infinity).
-define(COMM_TIMEOUT, infinity).
-define(ZMQ_TIMEOUT, 5000).
-define(NUM_W, 2).
-define(NUM_R, 2).

%% the following is used for backwards compatibility with
%% the previous interface, used by many systests.
%% when a read and update operation does not contain a bucket,
%% this global bucket will be used.
-define(GLOBAL_BUCKET, antidote_bucket).

%% Allow read concurrency on shared ets tables
%% These are the tables that store materialized objects
%% and information about live transactions, so the assumption
%% is there will be several more reads than writes
-define(TABLE_CONCURRENCY, {read_concurrency,true}).
%% The read concurrency is the maximum number of concurrent
%% readers per vnode.  This is so shared memory can be used
%% in the case of keys that are read frequently.  There is
%% still only 1 writer per vnode
-define(READ_CONCURRENCY, 20).
%% The log reader concurrency is the pool of threads per
%% physical node that handle requests from other DCs
%% for example to request missing log operations
-define(INTER_DC_QUERY_CONCURRENCY, 20).
%% This defines the concurrency for the meta-data tables that
%% are responsible for storing the stable time that a transaction
%% can read.  It is set to false because the erlang docs say
%% you should only set to true if you have long bursts of either
%% reads or writes, but here they should be interleaved (maybe).  But should
%% do some performance testing.
-define(META_TABLE_CONCURRENCY, {read_concurrency, false}, {write_concurrency, false}).
-define(META_TABLE_STABLE_CONCURRENCY, {read_concurrency, true}, {write_concurrency, false}).
%% This can be used for testing, so that transactions start with
%% old snapshots to avoid clock-skew.
%% This can break the tests is not set to 0
-define(OLD_SS_MICROSEC,0).
%% The number of supervisors that are responsible for
%% supervising transaction coorinator fsms
-define(NUM_SUP, 100).
%% Threads will sleep for this length when they have to wait
%% for something that is not ready after which they
%% wake up and retry. I.e. a read waiting for
%% a transaction currently in the prepare state that is blocking
%% that read.
-define(SPIN_WAIT, 0).
%% HEARTBEAT_PERIOD: Period of sending the heartbeat messages in interDC layer
-define(HEARTBEAT_PERIOD, 1000).
%% VECTORCLOCK_UPDATE_PERIOD: Period of updates of the stable snapshot per partition
-define(VECTORCLOCK_UPDATE_PERIOD, 100).
%% This is the time that nodes will sleep inbetween sending meta-data
%% to other physical nodes within the DC
-define(META_DATA_SLEEP, 1000).
-define(META_TABLE_NAME, a_meta_data_table).
-define(REMOTE_META_TABLE_NAME, a_remote_meta_data_table).
-define(META_TABLE_STABLE_NAME, a_meta_data_table_stable).
%% At commit, if this is set to true, the logging vnode
%% will ensure that the transaction record is written to disk
-define(SYNC_LOG, false).
%% Uncomment the following line to use erlang:now()
%% Otherwise os:timestamp() is used which can go backwards
%% which is unsafe for clock-si
-define(SAFE_TIME, true).
%% Version of log records being used
-define(LOG_RECORD_VERSION, 0).
%% Bounded counter manager parameters.
%% Period during which transfer requests from the same DC to the same key are ignored.
-define(GRACE_PERIOD, 1000000). % in Microseconds
%% Time to forget a pending request.
-define(REQUEST_TIMEOUT, 500000). % In Microseconds
%% Frequency at which manager requests remote resources.
-define(TRANSFER_FREQ, 100). %in Milliseconds

%% threshold time for physics static reads, in microseconds
%% @doc when running the physics protocol, if multiple reads
%% are sent in parallel in a single read_objects operation, the
%% physics read metadata is set to the current clock of the txn
%% coordinator. In the case a vnode receives that metadata and
%% its clock has not reached that value, it will have to wait
%% until it does. This threshold is used to reduce/avoid that
%% waiting.
-define(PHYSICS_THRESHOLD, 30000).

%% The definition "FIRST_OP" is used by the materializer.
%% The materialzer caches a tuple for each key containing
%% information about the state of operations performed on that key.
%% The first 3 elements in the tuple are the following meta-data:
%% First is the key itself
%% Second is a tuple defined as {the number of update operations stored in the tupe, the size of the tuple}
%% Thrid is a counter that keeps track of how many update operations have been performed on this key
%% Fourth is where the first update operation is stored
%% The remaining elements are update operations
-define(FIRST_OP, 4).

-record (payload, {key:: key(), type :: type(), op_param, actor :: actor()}).

-record(commit_log_payload, {commit_time :: dc_and_commit_time(),
			     causal_dependencies :: snapshot_time()
			    }).

-record(update_log_payload, {key :: key(),
			     bucket :: bucket(),
			     type :: type(),
			     op :: op()
			    }).

-record(abort_log_payload, {}).

-record(prepare_log_payload, {prepare_time :: non_neg_integer()}).

-type any_log_payload() :: #update_log_payload{} | #commit_log_payload{} | #abort_log_payload{} | #prepare_log_payload{}.

-record(log_operation, {
	  tx_id :: txid(),
	  op_type :: update | prepare | commit | abort | noop,
	  log_payload :: #commit_log_payload{}| #update_log_payload{} | #abort_log_payload{} | #prepare_log_payload{}}).
-record(op_number, {
  % TODO 19 undefined is required here, because of the use in inter_dc_log_sender_vnode. The use there should be refactored.
  node :: undefined | {node(),dcid()},
  global :: undefined | non_neg_integer(),
  local :: undefined | non_neg_integer()}).

%% The way records are stored in the log.
-record(log_record,{
	  version :: non_neg_integer(), %% The version of the log record, for backwards compatability
	  op_number :: #op_number{},
	  bucket_op_number :: #op_number{},
	  log_operation :: #log_operation{}}).

%% Clock SI

%% MIN is Used for generating the timeStamp of a new snapshot
%% in the case that a client has already seen a snapshot time
%% greater than the current time at the replica it is starting
%% a new transaction.
-define(MIN, 1).

%% DELTA has the same meaning as in the clock-SI paper.
-define(DELTA, 10000).

-define(CLOCKSI_TIMEOUT, 1000).

-record(operation_payload, {
    key :: key(),
    type :: type(),
    op_param :: op(),
    dependency_vc :: vectorclock(),
    dc_and_commit_time :: commit_time(),
    txid :: txid()}).

-record(transaction, {
    snapshot_clock :: snapshot_time(),
    transactional_protocol :: transactional_protocol(),
    snapshot_vc :: vectorclock(),
    txn_id :: txid() | no_txn_inserting_from_log,
    physics_read_metadata :: physics_read_metadata()}).


-record(physics_read_metadata, {
    dep_upbound :: vectorclock() | undefined,
    commit_time_lowbound :: vectorclock() | undefined
}).


-record(materialized_snapshot, {last_op_id :: op_num(),  %% This is the opid of the latest op in the list
       				                         %% of ops for this key included in this snapshot
			 	                         %% before an op that was not included, so to a new
				                         %% snapshot will be generated by starting from this op
				value :: snapshot()}).

%%---------------------------------------------------------------------
-type client_op() :: {update, {key(), type(), op()}} | {read, {key(), type()}} | {prepare, term()} | commit.
-type transactional_protocol():: clocksi | physics | gr.
-type crdt() :: term().
-type val() :: term().
-type reason() :: term().
%%chash:index_as_int() is the same as riak_core_apl:index().
%%If it is changed in the future this should be fixed also.
-type index_node() :: {chash:index_as_int(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type log() :: term().
-type op_num() :: non_neg_integer().
-type op_id() :: {op_num(), node()}.
-type payload() :: term().
-type partition_id()  :: ets:tid() | integer(). % TODO 19 adding integer basically makes the tid type non-opaque, because some places of the code depend on it being an integer. This dependency should be removed, if possible.
-type log_id() :: [partition_id()].
-type bucket() :: term().
-type snapshot() :: term().
-type orddict() :: orddict().
%%-type snapshot_time() :: vectorclock:vectorclock().
-type commit_time() :: {dcid(), non_neg_integer()}.
%%-type txid() :: #tx_id{} | ignore |no_txn_inserting_from_log.
-type operation_payload() :: #operation_payload{}.
%%-type dcid() :: term().
-type transaction() :: #transaction{} | undefined.
%-type physics_tx() :: #physics_transaction{}.

-type tx() :: #transaction{}.
-type cache_id() :: ets:tab().
-type inter_dc_conn_err() :: {error, {partition_num_mismatch, non_neg_integer(), non_neg_integer()} | {error, connection_error}}.

%%physics
-type physics_read_metadata() :: #physics_read_metadata{} | undefined. %undefined when using any protocol that's not PhysiCS.


-type txn_properties() :: term().
-type op_name() :: atom().
-type op_param() :: term().
-type bound_object() :: {key(), type(), bucket()}.

-type module_name() :: atom().
-type function_name() :: atom().

-export_type([key/0, op/0, crdt/0, val/0, reason/0, preflist/0,
              log/0, op_id/0, payload/0, partition_id/0,
              type/0, snapshot/0, txid/0, tx/0,
              bucket/0,
              txn_properties/0,
              op_param/0, op_name/0,
              bound_object/0,
	          orddict/0,
              module_name/0,
              function_name/0]).
%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------

-record(tx_coord_state, {
    transactional_protocol :: atom(),
    from :: undefined | {pid(), term()},
    transaction :: undefined | transaction(),
    updated_partitions :: list(),
	client_ops :: list(), % list of upstream updates, used for post commit hooks
	num_to_ack :: non_neg_integer(),
    num_to_read :: non_neg_integer(),
    prepare_time :: clock_time(),
    commit_time :: clock_time(),
    commit_protocol :: term(),
    state :: active | prepared | committing | committed | undefined
    | aborted | committed_read_only,
    operations :: undefined | list(),
    internal_read_set :: orddict(),
    return_accumulator :: list() | ok | {error, reason()},
    is_static :: boolean(),
    full_commit :: boolean(),
    stay_alive :: boolean(),
    %% The following is needed by the physics protocol
    version_max :: vectorclock()
}).


%% The record is using during materialization to keep the
%% state needed to materilze an object from the cache (or log)
-record(snapshot_get_response, {
	  ops_list :: [{op_num(),operation_payload()}] | tuple(),  %% list of new ops
	  number_of_ops :: non_neg_integer(), %% size of ops_list
	  materialized_snapshot :: #materialized_snapshot{},  %% the previous snapshot to apply the ops to
	  snapshot_time :: snapshot_time() | ignore,  %% The version vector time of the snapshot
	  is_newest_snapshot :: boolean() %% true if this is the most recent snapshot in the cache
	 }).

%% This record keeps the state of the materializer vnode
%% It is defined here as it is also used by the
%% clocksi_read_item_fsm as pointers to the materializer's ets caches
-record(mat_state, {
	  partition :: partition_id(),
	  ops_cache :: cache_id(),
	  snapshot_cache :: cache_id(),
	  is_ready :: boolean()}).
-type mat_state() :: #mat_state{}.
