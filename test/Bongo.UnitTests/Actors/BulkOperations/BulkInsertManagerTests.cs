using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.TestActors;
using Akka.TestKit.Xunit2;
using Bongo.Actors.BulkOperations;
using Bongo.Actors.BulkOperations.Messages;
using Bongo.Actors.Pools.Messages;
using Bongo.Actors.Tables;
using Bongo.TableDefinitions;
using FluentAssertions;
using Xunit;

namespace Bongo.UnitTests.Actors.BulkOperations
{
    public class BulkInsertManagerTests : TestKit
    {
        private readonly IActorRef _actor;
        private readonly TestProbe _tablesManagerProbe;
        private readonly TestProbe _poolManagerProbe;

        public BulkInsertManagerTests()
        {
            _tablesManagerProbe = CreateTestProbe();
            _poolManagerProbe = CreateTestProbe();

            _actor = ActorOfAsTestActorRef<BulkInsertWorker>(Props.Create(() => new BulkInsertWorker()), TestActor);

            Sys.ActorOf(Props.Create(() => new ForwardActor(_tablesManagerProbe)), "tablesManager");
            Sys.ActorOf(Props.Create(() => new ForwardActor(_poolManagerProbe)), "poolManager");
        }

        [Fact]
        public void Should_request_for_table_manager_for_type_table()
        {
            _actor.Tell(new InsertRequest(new List<object>
            {
                new BulkInsertManagerTests_Item()
            }));

            _tablesManagerProbe.ExpectMsg<TableManagerRequest>(request =>
            {
                request.EntityType
                    .Should()
                    .Be<BulkInsertManagerTests_Item>();
            });
        }

        [Fact]
        public void Should_request_new_partitions_be_created_if_buckets_for_items_dont_exist()
        {
            var now = DateTimeOffset.Now;
            var tableManager = CreateTestProbe();

            _actor.Tell(new InsertRequest(new List<object>
            {
                new BulkInsertManagerTests_Item
                {
                    Time = now
                }
            }));

            _tablesManagerProbe.ExpectMsg<TableManagerRequest>();

            _actor.Tell(new TableManagerResponse(tableManager));

            tableManager.ExpectMsg<PartitionInformationRequest>();

            _actor.Tell(new PartitionInformation
            {
                HashPartition = null,
                RangePartitions = new List<RangePartition>()
            });

            tableManager.ExpectMsg<List<AddRangePartitionRequest>>(requests =>
            {
                var bucketSize = (long) TimeSpan.FromDays(30).TotalMilliseconds;
                var start = now.ToUnixTimeMilliseconds() / bucketSize * bucketSize;

                requests.Should().HaveCount(1);

                var request = requests.First();

                request.Start
                    .Should()
                    .Be(start);

                request.End
                    .Should()
                    .Be(start + bucketSize);
            });
        }

        [Fact]
        public void Should_not_request_new_partitions_be_created_if_buckets_for_items_exist()
        {
            var now = DateTimeOffset.Now;
            var tableManager = CreateTestProbe();

            var bucketSize = (long)TimeSpan.FromDays(30).TotalMilliseconds;
            var start = now.ToUnixTimeMilliseconds() / bucketSize * bucketSize;

            _actor.Tell(new InsertRequest(new List<object>
            {
                new BulkInsertManagerTests_Item
                {
                    Time = now
                }
            }));

            _tablesManagerProbe.ExpectMsg<TableManagerRequest>();

            _actor.Tell(new TableManagerResponse(tableManager));

            tableManager.ExpectMsg<PartitionInformationRequest>();

            _actor.Tell(new PartitionInformation
            {
                HashPartition = null,
                RangePartitions = new List<RangePartition>
                {
                    new RangePartition(start, start + bucketSize)
                }
            });

            tableManager.ExpectNoMsg();
        }

        [Fact]
        public void Should_lease_connection_from_pool_and_insert_rows()
        {
            var now = DateTimeOffset.Now;
            var tableManager = CreateTestProbe();

            var bucketSize = (long)TimeSpan.FromDays(30).TotalMilliseconds;
            var start = now.ToUnixTimeMilliseconds() / bucketSize * bucketSize;

            var itemToInsert = new BulkInsertManagerTests_Item
            {
                Id = 43252435,
                Time = now
            };

            _actor.Tell(new InsertRequest(new List<object>
            {
                itemToInsert
            }));

            _tablesManagerProbe.ExpectMsg<TableManagerRequest>();

            _actor.Tell(new TableManagerResponse(tableManager));

            tableManager.ExpectMsg<PartitionInformationRequest>();

            _actor.Tell(new PartitionInformation
            {
                HashPartition = null,
                RangePartitions = new List<RangePartition>
                {
                    new RangePartition(start, start + bucketSize)
                }
            });

            _poolManagerProbe.ExpectMsg<ConnectionLeaseRequest>();

            var connectionProbe = CreateTestProbe();

            var connection = new ConnectionLease(connectionProbe, _actor);

            _actor.Tell(connection);

            connectionProbe.ExpectMsg<LeaseInsert>((insertQuery, sender) =>
            {
                insertQuery.Insert
                    .Should()
                    .Be($@"
insert into
    default.BulkInsertManagerTests_Item
        (id, time)
values
    ({itemToInsert.Id},{itemToInsert.Time.ToUnixTimeMilliseconds()});
");

                sender.Tell(new LeaseInsertSuccess());
            });

            connectionProbe.ExpectMsg<ConnectionLeaseRelease>();

            ExpectMsg<InsertResponse>();
        }

        [RangePartition("time", 30)]
        private class BulkInsertManagerTests_Item
        {
            [PrimaryKey]
            public long Id { get; set; }

            public DateTimeOffset Time { get; set; }
        }
    }
}
