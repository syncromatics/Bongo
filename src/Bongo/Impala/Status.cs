using System.Collections.Generic;
using ThriftSharp;

namespace Bongo.Impala
{
    [ThriftEnum]
    public enum StatusCode
    {
        Ok = 0,
        Cancelled = 1,
        AnalysisError = 2,
        NotImplementedError = 3,
        RuntimeError = 4,
        MemLimitError = 5,
        InternalError = 6,
        RecoverableError = 7
    }

    [ThriftStruct("TStatus")]
    public class Status
    {
        [ThriftField(1, true, "status_code")]
        public StatusCode StatusCode { get; set; }

        [ThriftField(2, false, "error_msgs")]
        public IList<string> ErrorMessages { get; set; }
    }
}
