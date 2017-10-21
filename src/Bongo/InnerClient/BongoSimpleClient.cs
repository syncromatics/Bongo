using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Bongo.InnerClient.Impala;

namespace Bongo.InnerClient
{
    internal class BongoSimpleClient : IDisposable
    {
        private readonly TimeSpan _timeout;
        private readonly ImpalaClient _impalaClient;

        private static readonly object ImpalaServiceCreationLock = new object();

        public BongoSimpleClient(IPEndPoint host, TimeSpan? timeout = null)
        {
            _timeout = timeout.GetValueOrDefault(TimeSpan.FromSeconds(30));
            lock (ImpalaServiceCreationLock)
            {
                _impalaClient = new ImpalaClient(host);
            }
        }

        public Task<string> GetImpalaVersion()
        {
            return _impalaClient.GetImpalaVersion();
        }

        public async Task CreateDatabase(string databaseName)
        {
            var databases = await ExecuteAndGetResults(new Query("show databases;"));
            if (databases.Results.Any(row => row.Contains(databaseName)))
            {
                throw new Exception($"Database '{databaseName}' already exists");
            }

            await ExecuteAndGetResults(new Query($"CREATE DATABASE {databaseName};"));
        }

        public async Task UseDatabase(string databaseName)
        {
            var databases = await ExecuteAndGetResults(new Query("show databases;"));
            if (!databases.Results.Any(row => row.Contains(databaseName)))
            {
                throw new Exception($"Database '{databaseName}' does not exist");
            }

            await ExecuteAndGetResults(new Query($"USE {databaseName};"));
        }

        public async Task ExecuteNoResult(string query)
        {
            await ExecuteAndGetResults(new Query(query));
        }

        public async Task Insert(string query)
        {
            await ExecuteInsert(new Query(query));
        }

        public Task<QueryResponse> Query(string query)
        {
            return ExecuteAndGetResults(new Query(query));
        }

        private async Task<QueryResponse> ExecuteAndGetResults(Query query)
        {
            var queryHandle = await _impalaClient.Query(query);
            return await Execute(queryHandle);
        }

        private async Task<InsertResult> ExecuteInsert(Query query)
        {
            var queryHandle = await _impalaClient.Query(query);

            await Execute(queryHandle, false);

            var log = await _impalaClient.GetLog(queryHandle.LogContextId);
            var insertResults = await _impalaClient.CloseInsert(queryHandle);
            if ((insertResults.NumberRowsErrors ?? 0) > 0)
            {
                throw new InsertException(log);
            }
            return insertResults;
        }

        private async Task<QueryResponse> Execute(QueryHandle queryHandle, bool shouldClose = true)
        {
            var start = DateTime.Now;
            var results = new QueryResponse();
            var first = true;
            while (DateTime.Now - start < _timeout)
            {
                var result = await _impalaClient.Fetch(queryHandle);
                var metadata = await _impalaClient.GetResultsMetadata(queryHandle);
                results.Metadata = metadata;
                if (result.Ready)
                {
                    if (first)
                    {
                        results.ColumnTypes.AddRange(result.Columns);
                        first = false;
                    }
                    results.Results.AddRange(result.Data);
                    if (!result.HasMore)
                    {
                        if(shouldClose)
                            await _impalaClient.Close(queryHandle);

                        return results;
                    }
                }
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            await _impalaClient.Close(queryHandle);
            throw new Exception("executing query timed out");
        }

        public void Dispose()
        {
            _impalaClient.Dispose();
        }
    }

    internal class QueryResponse
    {
        public ResultsMetadata Metadata { get; set; }
        public List<string> ColumnTypes { get; set; } = new List<string>();
        public List<string> Results { get; set; } = new List<string>();
        public string Warnings { get; set; }
    }
}
