using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Bongo.TableDefinitions;
using FluentAssertions;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
namespace Bongo.IntegrationTests
{
    public class AggregationAndProjectionTests : IClassFixture<ImpalaTestFixture>
    {
        private readonly ImpalaTestFixture _fixture;

        public AggregationAndProjectionTests(ImpalaTestFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Should_be_able_to_aggregate_and_return_results_in_projection()
        {
            var hosts = new List<IPEndPoint>
            {
                new IPEndPoint(_fixture.ImpalaIp, 21000)
            };

            var api = new BongoClient(hosts, 1);
            var items = new List<AggregationAndProjectionTests_Item>
            {
                new AggregationAndProjectionTests_Item
                {
                    Time = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero),
                    Passes = 10,
                    Fails = 2
                },
                new AggregationAndProjectionTests_Item
                {
                    Time = new DateTimeOffset(2000, 1, 2, 0, 0, 0, TimeSpan.Zero),
                    Passes = 5,
                    Fails = 20
                },
                new AggregationAndProjectionTests_Item
                {
                    Time = new DateTimeOffset(2000, 1, 10, 0, 0, 0, TimeSpan.Zero),
                    Passes = 10,
                    Fails = 2
                },
                new AggregationAndProjectionTests_Item
                {
                    Time = new DateTimeOffset(2000, 1, 11, 0, 0, 0, TimeSpan.Zero),
                    Passes = 1,
                    Fails = 3
                }
            };

            await api.Insert(items);

            var results = await api.Query<Projection>($@"
select
    sum(passes) as totalpasses,
    sum(fails) as totalfails,
    floor(time/{TimeSpan.FromDays(30).TotalMilliseconds}) as bucket
from
    aggregation_test_1
group by
    floor(time/{TimeSpan.FromDays(30).TotalMilliseconds})
    ");

            results.Should().HaveCount(1);
            results.First().ShouldBeEquivalentTo(new Projection
            {
                Bucket = 365,
                TotalPasses = 26,
                TotalFails = 27
            });
        }

        [RangePartition("time", 5)]
        [Table("aggregation_test_1", true)]
        [KuduReplicas(1)]
        private class AggregationAndProjectionTests_Item
        {
            [PrimaryKey]
            public DateTimeOffset Time { get; set; }

            public int Passes { get; set; }

            public int Fails { get; set; }
        }

        private class Projection
        {
            public long Bucket { get; set; }
            public int TotalPasses { get; set; }
            public int TotalFails { get; set; }
        }
    }
}
