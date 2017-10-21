using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using LclDckr;
using LclDckr.Commands.Run;

namespace Bongo.IntegrationTests
{
    public class ImpalaTestFixture : IDisposable
    {
        public IPAddress ImpalaIp { get; }

        public ImpalaTestFixture()
        {
            var client = new DockerClient();
            client.PullImage("andreysabitov/impala-kudu");
            client.RunOrReplace("andreysabitov/impala-kudu", "latest", new RunArguments
            {
                Name = "impala-test",
                HostName = "impala-test",
                PortMappings = new Dictionary<string, string>
                {
                    {"21000", "21000" }
                }
            });

            IPAddress ip = IPAddress.Parse("127.0.0.1");

            // Wait for impala here so we dont scew test times
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            while (stopWatch.Elapsed < TimeSpan.FromMinutes(5))
            {
                try
                {
                    using (var tcpClient = new TcpClient())
                    {
                        tcpClient.Connect(ip, 21000);
                        break;
                    }
                }
                catch (Exception e)
                {

                    Thread.Sleep(TimeSpan.FromMilliseconds(500));
                }
            }

            ImpalaIp = ip;
        }

        public void Dispose()
        {
            var client = new DockerClient();
            client.StopAndRemoveContainer("impala-test");
        }
    }
}
