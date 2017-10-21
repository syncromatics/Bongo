namespace Bongo.Actors.Pools.Messages
{
    internal class ConnectionLeaseRelease
    {
        public IConnectionLease Lease { get; }

        public ConnectionLeaseRelease(IConnectionLease lease)
        {
            Lease = lease;
        }
    }
}
