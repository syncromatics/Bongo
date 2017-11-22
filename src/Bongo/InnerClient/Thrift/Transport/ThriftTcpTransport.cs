using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using ThriftSharp.Transport;

namespace Bongo.InnerClient.Thrift.Transport
{
    internal sealed class ThriftTcpTransport : IThriftTransport
    {
        private static readonly ConcurrentDictionary<Guid, TcpClient> Clients = new ConcurrentDictionary<Guid, TcpClient>();

        private readonly TcpClient _client;

        public ThriftTcpTransport(IPEndPoint host, Guid clientGuid)
        {
            _client = Clients.GetOrAdd(clientGuid, point =>
            {
                var client = new TcpClient();
                client.Connect(host);
                return client;
            });
        }

        public void Dispose()
        {
        }

        public static void EndClient(Guid id)
        {
            if (Clients.TryRemove(id, out var client))
            {
                client.Dispose();
            }
        }

        public async Task FlushAndReadAsync()
        {
            await _client.GetStream().FlushAsync();
        }

        public void ReadBytes(byte[] output, int offset, int count)
        {
            var totalRead = 0;
            var innerOffset = offset;
            var remainingCount = count;

            for (var i = 0; i < 7; i++)
            {
                var read = _client.GetStream().Read(output, innerOffset, remainingCount);
                if (remainingCount - read == 0)
                    return;

                totalRead += read;
                innerOffset += read;
                remainingCount = count - totalRead;
            }

            if (totalRead != count)
            {
                throw new Exception($"Expected to read {count} bytes but only read {totalRead}");
            }
        }

        public void WriteBytes(byte[] bytes, int offset, int count)
        {
            _client.GetStream().Write(bytes, offset, count);
        }
    }
}
