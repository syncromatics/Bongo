using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Bongo.Impala;

namespace Bongo
{
    public class BongoClient : IDisposable
    {
        private readonly TimeSpan _timeout;
        private readonly ImpalaClient _impalaClient;

        public BongoClient(IPEndPoint host, TimeSpan? timeout = null)
        {
            _timeout = timeout.GetValueOrDefault(TimeSpan.FromSeconds(30));
            _impalaClient = new ImpalaClient(host);
        }

        public Task<string> GetImpalaVersion()
        {
            return _impalaClient.GetImpalaVersion();
        }

        public async Task CreateDatabase(string databaseName)
        {
            var databases = await ExecuteAndGetResults(new Query("show databases;"));
            if (databases.data.Any(row => row.Contains(databaseName)))
            {
                throw new Exception($"Database '{databaseName}' already exists");
            }

            await ExecuteAndGetResults(new Query($"CREATE DATABASE {databaseName};"));
        }

        public async Task UseDatabase(string databaseName)
        {
            var databases = await ExecuteAndGetResults(new Query("show databases;"));
            if (!databases.data.Any(row => row.Contains(databaseName)))
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

        public Task<(IList<string> columns, IList<string> data)> Query(string query)
        {
            return ExecuteAndGetResults(new Query(query));
        }

        private async Task<(IList<string>columns, IList<string>data)> ExecuteAndGetResults(Query query)
        {
            var queryHandle = await _impalaClient.Query(query);
            return await Execute(queryHandle);
        }

        private async Task<InsertResult> ExecuteInsert(Query query)
        {
            var queryHandle = await _impalaClient.Query(query);

            await Execute(queryHandle);

            var insertResults = await _impalaClient.CloseInsert(queryHandle);
            if ((insertResults.NumberRowsErrors ?? 0) > 0)
            {
                // todo: findout how to get more info out of impala, this can fail if no partion exists
                throw new InsertException("There were errors inserting");
            }
            return insertResults;
        }

        private async Task<(IList<string> columns, IList<string> data)> Execute(QueryHandle queryHandle)
        {
            var start = DateTime.Now;
            var results = (new List<string>(), new List<string>());
            var first = true;
            while (DateTime.Now - start < _timeout)
            {
                var result = await _impalaClient.Fetch(queryHandle);
                if (result.Ready)
                {
                    if (first)
                    {
                        results.Item1.AddRange(result.Columns);
                        first = false;
                    }
                    results.Item2.AddRange(result.Data);
                    if (!result.HasMore)
                    {
                        return results;
                    }
                }
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            throw new Exception("executing query timed out");
        }
        public void Dispose()
        {
            _impalaClient.Dispose();
        }
    }
}
