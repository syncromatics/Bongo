using System.Collections.Generic;
using System.Linq;
using System.Net;
using Akka.Actor;
using Akka.Routing;
using Bongo.Actors.Pools.Messages;

namespace Bongo.Actors.Pools
{
    internal class PoolManager : ReceiveActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }

        public PoolManager(List<IPEndPoint> hosts, int poolPerHostCount)
        {
            Become(() => InitializePools(hosts, poolPerHostCount));
        }

        private void InitializePools(List<IPEndPoint> hosts, int poolPerHostCount)
        {
            Receive<string>(_ =>
            {
                var self = Self;

                for (var i = 0; i < poolPerHostCount; i++)
                {
                    foreach (var host in hosts)
                    {
                        Context.ActorOf(Props.Create(() => new PoolConnection(host, self)));
                    }
                }

                Become(WaitForRoutee);
                Stash.UnstashAll();
            });

            ReceiveAny(_ => Stash.Stash());

            Self.Tell("init");
        }

        private void WaitForRoutee()
        {
            Receive<AddRoutee>(add =>
            {
                Become(() => RouteRequests(new []{ add.Routee }));
                Stash.UnstashAll();
            });

            ReceiveAny(_ =>
            {
                Stash.Stash();
            });
        }

        private void RouteRequests(Routee[] routees)
        {
            var routingLogic = new SmallestMailboxRoutingLogic();

            Receive<ConnectionLeaseRequest>(request =>
            {
                var routee = routingLogic.Select(request, routees);
                routee.Send(request, Sender);
            });

            Receive<AddRoutee>(routee =>
            {
                Become(() => RouteRequests(routees.Append(routee.Routee).ToArray()));
            });

            Receive<RemoveRoutee>(routee =>
            {
                var routingList = routees.Where(r => r != routee.Routee).ToArray();
                if(routingList.Length == 0)
                    Become(WaitForRoutee);
                else
                    Become(() => RouteRequests(routingList));
            });
        }
    }
}
