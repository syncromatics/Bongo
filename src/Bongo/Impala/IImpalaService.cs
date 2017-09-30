using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ThriftSharp;

namespace Bongo.Impala
{
    [ThriftService("ImpalaService")]
    public interface IImpalaService
    {
        [ThriftMethod("PingImpalaService")]
        Task<PingImpalaServiceResponse> PingImpalaServiceAsync();

        [ThriftMethod("echo")]
        Task<string> Echo([ThriftParameter(1, "s")] string s);

        [ThriftMethod("query")]
        [ThriftThrows(1, "error", typeof(BeeswaxException))]
        Task<QueryHandle> Query([ThriftParameter(1, "query")] Query query);

        [ThriftMethod("fetch")]
        Task<Results> Fetch([ThriftParameter(1, "query_id")] QueryHandle handle,
            [ThriftParameter(2, "start_over")] bool startOver, [ThriftParameter(3, "fetch_size")] int fetchSize);

        [ThriftMethod("get_state")]
        Task<QueryState> GetState([ThriftParameter(1, "handle")] QueryHandle handle);

        [ThriftMethod("CloseInsert")]
        [ThriftThrows(2, "error2", typeof(BeeswaxException))]
        Task<InsertResult> CloseInsert([ThriftParameter(1, "handle")] QueryHandle handle);

        [ThriftMethod("get_log")]
        Task<string> GetLog([ThriftParameter(1, "context")]string logContextId);

        [ThriftMethod("GetExecSummary")]
        [ThriftThrows(2, "error2", typeof(BeeswaxException))]
        Task<ExecSummary> GetExecSummary([ThriftParameter(1, "handle")]QueryHandle handle);
    }

    [ThriftStruct("TPingImpalaServiceResp")]
    public sealed class PingImpalaServiceResponse
    {
        [ThriftField(1, true, "version")]
        public string Version { get; set; }
    }

    [ThriftStruct("QueryHandle")]
    public sealed class QueryHandle
    {
        [ThriftField(1, true, "id")]
        public string Id { get; set; }

        [ThriftField(2, true, "log_context")]
        public string LogContextId { get; set; }
    }

    [ThriftStruct("Query")]
    public sealed class Query
    {
        [ThriftField(1, true, "query")]
        public string QueryString { get; set; }

        [ThriftField(3, true, "configuration")]
        public List<string> Configuration { get; set; } = new List<string>();

        [ThriftField(4, true, "hadoop_user")]
        public string HadoopUser { get; set; } = "default";

        public Query()
        {
        }

        public Query(string query)
        {
            QueryString = query;
        }
    }

    [ThriftStruct("Results")]
    public sealed class Results
    {
        [ThriftField(1, true, "ready")]
        public bool Ready { get; set; }

        [ThriftField(2, true, "columns")]
        public IList<string> Columns { get; set; }

        [ThriftField(3, true, "data")]
        public IList<string> Data { get; set; }

        [ThriftField(4, true, "start_row")]
        public long StartRow { get; set; }

        [ThriftField(5, true, "has_more")]
        public bool HasMore { get; set; }
    }

    [ThriftEnum]
    public enum QueryState
    {
        Created = 0,
        Initialized = 1,
        Compiled = 2,
        Running = 3,
        Finished = 4,
        Exception = 5
    }

    [ThriftStruct("BeeswaxException")]
    public class BeeswaxException : Exception
    {
        [ThriftField(1, true, "message")]
        public string Message { get; set; }

        [ThriftField(2, true, "log_context")]
        public string LogContextId { get; set; }

        [ThriftField(3, true, "handle")]
        public QueryHandle QueryHandle { get; set; }

        [ThriftField(4, false, "errorCode")]
        public int? ErrorCode { get; set; }

        [ThriftField(5, false, "SQLState")]
        public string SqlState { get; set; }
    }

    [ThriftStruct("TInsertResult")]
    public class InsertResult
    {
        [ThriftField(1, true, "rows_modified")]
        public IDictionary<string, long> RowsModified { get; set; }

        [ThriftField(2, false, "num_row_errors")]
        public long? NumberRowsErrors { get; set; }
    }
}
