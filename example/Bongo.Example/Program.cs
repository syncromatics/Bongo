using System;
using System.Net;
using System.Threading.Tasks;
using Bongo.Impala;

namespace Bongo.Example
{
    class Program
    {
        static async Task Main(string[] args)
        {
            QueryHandle result;
            //await Task.Delay(TimeSpan.FromMinutes(1));
            var ip = Dns.GetHostAddresses("impala");
            using (var bongoClient = new BongoClient(new IPEndPoint(ip[0], 21000)))
            {
                var version = await bongoClient.GetImpalaVersion();
                //await bongoClient.CreateDatabase("test_database");
                await bongoClient.UseDatabase("test_database");
                await bongoClient.ExecuteNoResult(@"
CREATE TABLE IF NOT EXISTS vehicle_positions (
    vehicle_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    time BIGINT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    PRIMARY KEY (vehicle_id, customer_id, time)
)
PARTITION BY HASH (vehicle_id) PARTITIONS 3,
RANGE(time)
(
    PARTITION 1498867200 <= VALUES <= 1501545599,
    PARTITION 1501545600 <= VALUES <= 1504223999,
    PARTITION 1504224000 <= VALUES <= 1506815999
)
STORED AS KUDU
TBLPROPERTIES (
    'kudu.num_tablet_replicas' = '1'
);");
                await bongoClient.Insert($@"
insert into 
    vehicle_positions
values
    (1,2,{DateTimeOffset.UtcNow.ToUnixTimeSeconds()},30.12, 120.2);");

                var rows = await bongoClient.Query("select * from vehicle_positions;");
            }
        }
    }
}
