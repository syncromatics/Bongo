using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Routing;
using Bongo.Actors.BulkOperations;
using Bongo.Actors.BulkOperations.Messages;
using Bongo.Actors.Pools;
using Bongo.Actors.Query;
using Bongo.Actors.Query.Messages;
using Bongo.Actors.Tables;

namespace Bongo
{
    public class BongoClient : IDisposable
    {
        private readonly ActorSystem _system;
        private readonly IActorRef _bulkManager;
        private readonly IActorRef _queryManager;

        public BongoClient(List<IPEndPoint> impalaHosts, int hostPoolSize)
        {
            _system = ActorSystem.Create($"BongoClient-{Guid.NewGuid()}");
            _system.ActorOf(Props.Create(() => new PoolManager(impalaHosts, hostPoolSize)), "poolManager");
            _system.ActorOf(Props.Create(() => new TablesManager()), "tablesManager");

            _queryManager = _system.ActorOf(Props.Create<QueryWorker>().WithRouter(new SmallestMailboxPool(5)), "queryManager");
            _bulkManager = _system.ActorOf(Props.Create<BulkInsertWorker>().WithRouter(new SmallestMailboxPool(5)), "bulkInsertManager");
        }

        public async Task Insert<T>(IEnumerable<T> items, bool upsert = false) where T : class
        {
            var itemsToInsert = items
                .Cast<object>()
                .ToList();

            if (!itemsToInsert.Any())
                throw new ArgumentException("Must have at least one", nameof(items));

            var result = await _bulkManager.Ask(new InsertRequest(itemsToInsert, upsert));
            switch (result)
            {
                case Exception exception: throw exception;
            }
        }

        public async Task<List<T>> Query<T>(string query) where T : class
        {
            var results = await _queryManager.Ask(new QueryRequest(query, typeof(T)));
            switch (results)
            {
                case List<object> list: return list.Cast<T>().ToList();
                case Exception exception: throw exception;
                default:
                    throw new Exception($"Unknown response {results}");
            }
        }

        public void Dispose()
        {
            _system.Dispose();
        }
    }
}
