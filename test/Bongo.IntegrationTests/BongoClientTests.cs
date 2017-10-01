using System;
using System.Net;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace Bongo.IntegrationTests
{
    public class BongoClientTests : IClassFixture<ImpalaTestFixture>
    {
        private readonly ImpalaTestFixture _fixture;

        public BongoClientTests(ImpalaTestFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Should_get_version()
        {
            using (var client = new BongoClient(new IPEndPoint(_fixture.ImpalaIp, 21000)))
            {
                var version = await client.GetImpalaVersion();
                version.Should().NotBeNullOrWhiteSpace();
            }
        }
    }
}
