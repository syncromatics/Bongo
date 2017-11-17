using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Bongo.TableDefinitions;
using FluentAssertions;
using Xunit;

namespace Bongo.IntegrationTests
{
    public class TableCreationAndStartupCheckTests : IClassFixture<ImpalaTestFixture>
    {
        private readonly ImpalaTestFixture _fixture;

        public TableCreationAndStartupCheckTests(ImpalaTestFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Should_be_able_to_parse_all_types_when_checking_for_table_creation()
        {
            var hosts = new List<IPEndPoint>
            {
                new IPEndPoint(_fixture.ImpalaIp, 21000)
            };

            var api = new BongoClient(hosts, 1);
            var items = new List<TableCreationAndStartupCheckTests_TypeCheck>
            {
                new TableCreationAndStartupCheckTests_TypeCheck
                {
                    Id = 1,
                    TestEnum = TestEnum.Off,
                    SomeTime = TimeSpan.FromMinutes(2)
                },
                new TableCreationAndStartupCheckTests_TypeCheck
                {
                    Id = 2,
                    TestEnum = TestEnum.On,
                    SomeTime = TimeSpan.FromMilliseconds(456)
                }
            };

            await api.Insert(items);

            var returnedItems =
                await api.Query<TableCreationAndStartupCheckTests_TypeCheck>(
                    "select * from table_creation_and_startup_check_tests_type_check_1;");

            returnedItems
                .OrderBy(item => item.Id)
                .ShouldAllBeEquivalentTo(items);
        }

        [Table("table_creation_and_startup_check_tests_type_check_1", true)]
        [KuduReplicas(1)]
        [HashPartition(new [] {"id"}, 2)]
        public class TableCreationAndStartupCheckTests_TypeCheck
        {
            [PrimaryKey]
            public long Id { get; set; }
            public TestEnum TestEnum { get; set; }

            public TimeSpan SomeTime { get; set; }
        }

        public enum TestEnum
        {
            On = 0,
            Off = 1
        }
    }
}
