using System;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Bongo.InnerClient;

namespace Bongo.Actors.Pools.Messages
{
    internal class ConnectionLease : IConnectionLease
    {
        public IActorRef Connection { get; }
        public IActorRef Leasee { get; }
        public Guid ConnectionLeaseId { get; }

        public ConnectionLease(IActorRef connection, IActorRef leasee)
        {
            Connection = connection;
            Leasee = leasee;
            ConnectionLeaseId = Guid.NewGuid();
        }
    }
}
