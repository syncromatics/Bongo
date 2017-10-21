using System;
using System.Net;
using System.Threading.Tasks;
using Bongo.InnerClient.Impala;
using Bongo.InnerClient.Thrift.Transport;
using ThriftSharp;

namespace Bongo.InnerClient
{
    internal class ImpalaClient : IDisposable
    {
        private readonly IImpalaService _impalaService;
        private readonly Guid _id = Guid.NewGuid();

        public ImpalaClient(IPEndPoint host)
        {
            var comm = ThriftCommunication.Binary().UsingCustomTransport(token => new ThriftTcpTransport(host, _id));
            _impalaService = ThriftProxy.Create<IImpalaService>(comm);
        }

        public async Task<string> GetImpalaVersion()
        {
            var response = await _impalaService.PingImpalaServiceAsync();
            return response.Version;
        }

        public Task<string> Echo(string s)
        {
            return _impalaService.Echo(s);
        }

        public Task<QueryHandle> Query(Query query)
        {
            return _impalaService.Query(query);
        }

        public Task<Results> Fetch(QueryHandle handle)
        {
            return _impalaService.Fetch(handle, false, -1);
        }

        public Task<QueryState> GetState(QueryHandle handle)
        {
            return _impalaService.GetState(handle);
        }

        public Task<InsertResult> CloseInsert(QueryHandle handle)
        {
            return _impalaService.CloseInsert(handle);
        }

        public Task<string> GetLog(string logContextId)
        {
            return _impalaService.GetLog(logContextId);
        }

        public Task<ExecSummary> GetExecSummary(QueryHandle handle)
        {
            return _impalaService.GetExecSummary(handle);
        }

        public Task<ResultsMetadata> GetResultsMetadata(QueryHandle handle)
        {
            return _impalaService.GetResultsMetadata(handle);
        }

        public Task Close(QueryHandle handle)
        {
            return _impalaService.Close(handle);
        }

        public void Dispose()
        {
            ThriftTcpTransport.EndClient(_id);
        }
    }
}
