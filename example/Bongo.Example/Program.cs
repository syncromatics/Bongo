using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Bongo.TableDefinitions;

namespace Bongo.Example
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var ip = Dns.GetHostAddresses("impala");
            using (var bongoClient = new BongoClient(new List<IPEndPoint> { new IPEndPoint(ip[0], 21000) }, 50))
            {
                var items = new List<TestEntity>
                {
                    new TestEntity
                    {
                        Time = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero),
                        Passes = 10,
                        Fails = 2
                    },
                    new TestEntity
                    {
                        Time = new DateTimeOffset(2000, 1, 2, 0, 0, 0, TimeSpan.Zero),
                        Passes = 5,
                        Fails = 20
                    },
                    new TestEntity
                    {
                        Time = new DateTimeOffset(2000, 1, 10, 0, 0, 0, TimeSpan.Zero),
                        Passes = 10,
                        Fails = 2
                    },
                    new TestEntity
                    {
                        Time = new DateTimeOffset(2000, 1, 20, 0, 0, 0, TimeSpan.Zero),
                        Passes = 1,
                        Fails = 3
                    }
                };

                await bongoClient.Insert(items, true);

                var results = await bongoClient.Query<Projection>($@"
select
    sum(passes) as totalpasses,
    sum(fails) as totalfails,
    floor(time/{TimeSpan.FromDays(30).TotalMilliseconds}) as bucket
from
    aggregation_test_1
group by
    floor(time/{TimeSpan.FromDays(30).TotalMilliseconds});
");
            }
        }
    }

    [RangePartition("time", 5)]
    [Table("tests", true)]
    [KuduReplicas(1)]
    public class TestEntity
    {
        [PrimaryKey]
        public DateTimeOffset Time { get; set; }

        public int Passes { get; set; }

        public int Fails { get; set; }
    }

    public class Projection
    {
        public long Bucket { get; set; }
        public int TotalPasses { get; set; }
        public int TotalFails { get; set; }
    }
}
