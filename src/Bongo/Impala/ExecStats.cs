using System.Collections.Generic;
using ThriftSharp;

namespace Bongo.Impala
{
    [ThriftEnum]
    public enum ExecState
    {
        Registered = 0,
        Planning = 1,
        Queued = 2,
        Running = 3,
        Finished = 4,
        Cancelled = 5,
        Failed = 6
    }

    /// <summary>
    /// Execution stats for a single plan node.
    /// </summary>
    [ThriftStruct("TExecStats")]
    public class ExecStats
    {
        /// <summary>
        /// The wall clock time spent on the "main" thread. This is the user perceived
        /// latency. This value indicates the current bottleneck.
        /// Note: anywhere we have a queue between operators, this time can fluctuate
        /// significantly without the overall query time changing much (i.e. the bottleneck
        /// moved to another operator). This is unavoidable though.
        /// </summary>
        [ThriftField(1, false, "latency_ns")]
        public long? LatencyNs { get; set; }

        /// <summary>
        /// Total CPU time spent across all threads. For operators that have an async
        /// component (e.g. multi-threaded) this will be >= latency_ns.
        /// </summary>
        [ThriftField(2, false, "cpu_time_ns")]
        public long? CpuTimeNs { get; set; }

        /// <summary>
        /// Number of rows returned.
        /// </summary>
        [ThriftField(3, false, "cardinality")]
        public long? Cardinality { get; set; }

        /// <summary>
        /// Peak memory used (in bytes).
        /// </summary>
        [ThriftField(4, false, "memory_used")]
        public long? MemoryUsed { get; set; }
    }

    /// <summary>
    /// Summary for a single plan node. This includes labels for how to display the
    /// node as well as per instance stats.
    /// </summary>
    [ThriftStruct("TPlanNodeExecSummary")]
    public class PlanNodeExecSummary
    {
        [ThriftField(1, true, "node_id")]
        public int NodeId { get; set; }

        [ThriftField(2, true, "fragment_id")]
        public int FragmentId { get; set; }

        [ThriftField(3, true, "label")]
        public string Label { get; set; }

        [ThriftField(4, false, "label_detail")]
        public string LabelDetail { get; set; }

        [ThriftField(5, true, "num_children")]
        public int NumberOfChildren { get; set; }

        [ThriftField(6, false, "estimated_stats")]
        public ExecStats EstimatedStats { get; set; }

        [ThriftField(7, false, "exec_stats")]
        public IList<ExecStats> ExecStats { get; set; }

        [ThriftField(8, false, "is_active")]
        public IList<bool> IsActive { get; set; }

        [ThriftField(8, false, "is_broadcast")]
        public bool? IsBroadcast { get; set; }
    }

    /// <summary>
    /// Execution summary of an entire query.
    /// </summary>
    [ThriftStruct("TExecSummary")]
    public class ExecSummary
    {
        /// <summary>
        /// State of the query.
        /// </summary>
        [ThriftField(1, true, "state")]
        public ExecState State { get; set; }

        /// <summary>
        /// Contains the error if state is FAILED.
        /// </summary>
        [ThriftField(2, false, "status")]
        public Status Status { get; set; }

        /// <summary>
        /// Flattened execution summary of the plan tree.
        /// </summary>
        [ThriftField(3, false, "nodes")]
        public IList<PlanNodeExecSummary> Nodes { get; set; }

        /// <summary>
        /// For each exch node in 'nodes', contains the index to the root node of the sending
        /// fragment for this exch. Both the key and value are indices into 'nodes'.
        /// </summary>
        [ThriftField(4, false, "exch_to_sender_map")]
        public IDictionary<int, int> ExchToSenderMap { get; set; }

        /// <summary>
        /// List of errors that were encountered during execution. This can be non-empty
        /// even if status is okay, in which case it contains errors that impala skipped
        /// over.
        /// </summary>
        [ThriftField(5, false, "error_logs")]
        public IList<string> ErrorLogs { get; set; }
    }
}
