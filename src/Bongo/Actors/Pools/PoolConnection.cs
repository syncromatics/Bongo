using System;
using System.Net;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Routing;
using Bongo.Actors.Pools.Messages;
using Bongo.InnerClient;
using Bongo.InnerClient.Impala;

namespace Bongo.Actors.Pools
{
    public class PoolConnection : ReceiveActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }

        private readonly ActorRefRoutee _routee;

        public PoolConnection(IPEndPoint host, IActorRef router)
        {
            _routee = new ActorRefRoutee(Self);

            Become(() => InitializeConnection(host, router));
        }

        private void InitializeConnection(IPEndPoint host, IActorRef router)
        {
            ReceiveAsync<string>(async _ =>
            {
                var (success, client) = await TryCreateClient(host);
                if (!success)
                {
                    Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10), Self, "connect", Self);
                    return;
                }

                router.Tell(new AddRoutee(_routee));
                Become(() => WaitForLease(host, router, client));
                Stash.UnstashAll();
            });

            Receive<ConnectionLeaseRequest>(request =>
            {
                router.Forward(request);
            });

            Self.Tell("connect");
        }

        private void WaitForLease(IPEndPoint host, IActorRef router, BongoSimpleClient client)
        {
            Receive<ConnectionLeaseRequest>(request =>
            {
                var lease = new ConnectionLease(Self, Sender);
                Become(() => ConnectionLeased(host, router, client, lease));
            });
        }

        private void ConnectionLeased(
            IPEndPoint host,
            IActorRef router,
            BongoSimpleClient client,
            IConnectionLease lease)
        {
            Receive<ConnectionLeaseRelease>(release =>
            {
                if (release.Lease != lease)
                    return;

                Become(() => WaitForLease(host, router, client));
                Stash.UnstashAll();
            });

            ReceiveAsync<LeaseQuery>(async query =>
            {
                if (query.ConnectionLeaseId != lease.ConnectionLeaseId)
                {
                    Sender.Tell(new LeaseRejection());
                    return;
                }

                try
                {
                    var response = await client.Query(query.Query);
                    Sender.Tell(response);
                }
                catch (BeeswaxException e)
                {
                    Sender.Tell(e);
                }
                catch (Exception e)
                {
                    Context.GetLogger().Error(e, "Error in connection");
                    Sender.Tell(e);

                    router.Tell(new RemoveRoutee(_routee));

                    Become(() => InitializeConnection(host, router));
                    Stash.UnstashAll();
                }
            });

            ReceiveAsync<LeaseInsert>(async query =>
            {
                if (query.ConnectionLeaseId != lease.ConnectionLeaseId)
                {
                    Sender.Tell(new LeaseRejection());
                    return;
                }

                try
                {
                    await client.Insert(query.Insert);
                    Sender.Tell(new LeaseInsertSuccess());
                }
                catch (BeeswaxException e)
                {
                    Sender.Tell(e);
                }
                catch (Exception e)
                {
                    Context.GetLogger().Error(e, "Error in connection");
                    Sender.Tell(e);

                    router.Tell(new RemoveRoutee(_routee));

                    Become(() => InitializeConnection(host, router));
                    Stash.UnstashAll();
                }
            });

            ReceiveAny(_ => Stash.Stash());

            Sender.Tell(lease);
        }

        private async Task<(bool IsCreated, BongoSimpleClient Client)> TryCreateClient(IPEndPoint host)
        {
            BongoSimpleClient client = null;

            try
            {
                client = new BongoSimpleClient(host);

                await client.Query("show databases;");

                return (true, client);
            }
            catch (Exception e)
            {
                client?.Dispose();

                Context
                    .GetLogger()
                    .Warning("Could not start bongo client.. will retry");

                return (false, null);
            }
        }
    }
}
