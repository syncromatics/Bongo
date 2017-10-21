using System.Collections.Generic;
using System.Net;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Bongo.Actors.Pools;

namespace Bongo.UnitTests.Actors.Pools
{
    public class PoolManagerTests : TestKit
    {
        private IActorRef _actor;
        private List<IPEndPoint> _endpoints;

        public PoolManagerTests()
        {
            _endpoints = new List<IPEndPoint>
            {
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 2100),
                new IPEndPoint(IPAddress.Parse("127.0.0.2"), 2101)
            };

            _actor = ActorOfAsTestActorRef<PoolManager>(Props.Create(() => new PoolManager(_endpoints, 1)), TestActor);
        }
    }
}
