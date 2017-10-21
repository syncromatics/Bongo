using System;
using Akka.Actor;

namespace Bongo.Actors.Pools.Messages
{
    internal interface IConnectionLease
    {
        IActorRef Connection { get; }
        IActorRef Leasee { get; }
        Guid ConnectionLeaseId { get; }
    }

    internal class LeaseQuery
    {
        public string Query { get; }
        public Guid ConnectionLeaseId { get; }

        public LeaseQuery(string query, Guid connectionLeaseId)
        {
            Query = query;
            ConnectionLeaseId = connectionLeaseId;
        }
    }

    internal class LeaseInsert
    {
        public string Insert { get; }
        public Guid ConnectionLeaseId { get; }

        public LeaseInsert(string query, Guid connectionLeaseId)
        {
            Insert = query;
            ConnectionLeaseId = connectionLeaseId;
        }
    }

    internal class LeaseInsertSuccess
    {
    }

    internal class LeaseRejection { }
}
