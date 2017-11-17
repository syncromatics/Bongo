using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Bongo.Actors.BulkOperations.Messages;
using Bongo.Actors.Pools.Messages;
using Bongo.Actors.Tables;
using Bongo.InnerClient.Impala;
using Bongo.TableDefinitions;

namespace Bongo.Actors.BulkOperations
{
    internal class BulkInsertWorker : ReceiveActor, IWithUnboundedStash
    {
        private readonly ActorSelection _poolManager;

        public IStash Stash { get; set; }

        public BulkInsertWorker()
        {
            _poolManager = Context.System.ActorSelection("/user/poolManager");
            
            Become(ProcessRequests);
        }

        private void ProcessRequests()
        {
            Receive<InsertRequest>(request =>
            {
                var sender = Sender;
                Become(() => GetTableManager(request, sender));
            });
        }

        private void GetTableManager(InsertRequest request, IActorRef sender)
        {
            Receive<TableManagerResponse>(manager =>
            {
                Become(() => GetPartitionInformation(request, manager.Manager, sender));
            });

            Context
                .ActorSelection("/user/tablesManager")
                .Tell(new TableManagerRequest(request.Items.First().GetType()));
        }

        private void GetPartitionInformation(InsertRequest request, IActorRef tableManager, IActorRef sender)
        {
            Receive<PartitionInformation>(partitionInfo =>
            {
                var neededPartitions = CheckForNeededPartitions(request, partitionInfo);
                if(neededPartitions.Any())
                    tableManager.Tell(neededPartitions);
                else
                    Become(() => InsertData(request, sender));
            });

            Receive<AddRangeResponse>(_ =>
            {
                Become(() => InsertData(request, sender));
            });

            tableManager.Tell(new PartitionInformationRequest());
        }

        private void InsertData(InsertRequest request, IActorRef sender)
        {
            ReceiveAsync<IConnectionLease>(async lease =>
            {
                try
                {
                    var insertQuery = GetInsertStatement(request);
                    var response = await lease.Connection.Ask(new LeaseInsert(insertQuery, lease.ConnectionLeaseId));
                    switch (response)
                    {
                        case LeaseInsertSuccess leaseInsertSuccess:
                            sender.Tell(new InsertResponse());
                            break;
                        case BeeswaxException exception:
                            sender.Tell(exception);
                            break;
                        case Exception exception:
                            sender.Tell(exception);
                            break;
                    }
                }
                catch (Exception e)
                {
                    sender.Tell(e);
                }

                lease.Connection.Tell(new ConnectionLeaseRelease(lease));

                Become(ProcessRequests);
                Stash.UnstashAll();
            });

            _poolManager.Tell(new ConnectionLeaseRequest());
        }

        private (string Database, string TableName, bool isKudu) GetTableName(Type type)
        {
            var titleAttribute = Attribute
                .GetCustomAttributes(type, typeof(TableAttribute))
                .Cast<TableAttribute>()
                .FirstOrDefault();

            var tableName = !string.IsNullOrWhiteSpace(titleAttribute?.Name)
                ? titleAttribute.Name
                : type.Name;

            var isKudu = titleAttribute?.IsKuduTable ?? false;

            return ("default", tableName, isKudu);
        }

        private List<AddRangePartitionRequest> CheckForNeededPartitions(InsertRequest request, PartitionInformation currentPartitions)
        {
            var type = request.Items.First().GetType();

            var rangePartitionAttribute = Attribute
                .GetCustomAttributes(type, typeof(RangePartitionAttribute))
                .Cast<RangePartitionAttribute>()
                .FirstOrDefault();

            if(rangePartitionAttribute ==null)
                return new List<AddRangePartitionRequest>();

            var rangeProperty = type
                .GetProperties()
                .FirstOrDefault(property =>
                {
                    var columnNameAttribute = Attribute
                        .GetCustomAttributes(property, typeof(ColumnNameAttribute))
                        .Cast<ColumnNameAttribute>()
                        .FirstOrDefault();

                    if (columnNameAttribute != null)
                    {
                        return columnNameAttribute.Name.ToLower() == rangePartitionAttribute.Column.ToLower();
                    }

                    return property.Name.ToLower() == rangePartitionAttribute.Column.ToLower();
                });

            if(rangeProperty == null)
                throw new Exception($"Column '{rangePartitionAttribute.Column}' not found");

            var times = request.Items
                .Select(item =>
                {
                    var value = rangeProperty.GetValue(item);
                    switch (value)
                    {
                        case DateTimeOffset time: return time.ToUnixTimeMilliseconds();
                        case DateTime time: return new DateTimeOffset(time).ToUnixTimeMilliseconds();
                        case int time: return time;
                        case long time: return time;
                        default:
                            throw new Exception($"Type '{value.GetType().Name}' is invalid for partitioning." +
                                                $" The valid types are DateTimeOffset, DateTime, long, and int");
                    }
                })
                .ToList();

            var bucketSize = (long)TimeSpan.FromDays(rangePartitionAttribute.BucketSizeInDays).TotalMilliseconds;

            var unPartitionedTimes = times
                .Where(time => currentPartitions.RangePartitions.All(partition => !partition.Contains(time)))
                .GroupBy(time => (long)Math.Floor(time / (double)bucketSize))
                .Select(group => group.Key)
                .ToList();

            return unPartitionedTimes
                .Select(bucketNumber =>
                    new AddRangePartitionRequest(bucketNumber * bucketSize, bucketNumber * bucketSize + bucketSize))
                .ToList();
        }

        private string GetInsertStatement(InsertRequest request)
        {
            var type = request.Items.First().GetType();
            var tableInformation = GetTableName(type);

            List<string> columnNames = new List<string>();
            List<Func<object, string>> columnValues = new List<Func<object, string>>();

            foreach (var property in type.GetProperties())
            {
                var columnNameAttribute = Attribute
                    .GetCustomAttributes(property, typeof(ColumnNameAttribute))
                    .Cast<ColumnNameAttribute>()
                    .FirstOrDefault();

                var columnName = !string.IsNullOrWhiteSpace(columnNameAttribute?.Name)
                    ? columnNameAttribute.Name
                    : property.Name.ToLower();

                columnNames.Add(columnName);

                columnValues.Add(obj =>
                {
                    var value = property.GetValue(obj);
                    if (value == null)
                        return "null";

                    return GetValueText(value);
                });
            }

            var values = request.Items
                .Select(item => $"({string.Join(",", columnValues.Select(func => func(item)))})");

            var commandString = request.Upsert ? "upsert" : "insert";

            return $@"
{commandString} into
    {tableInformation.Database}.{tableInformation.TableName}
        ({string.Join(", ", columnNames)})
values
    {string.Join(", ", values)};
";
        }

        private static string GetValueText(object value)
        {
            switch (value)
            {
                case DateTimeOffset time: return time.ToUnixTimeMilliseconds().ToString();
                case int time: return time.ToString();
                case long time: return time.ToString();
                case string str: return $"\"{str}\"";
                case Enum en: return Convert.ToInt32(en).ToString();
                case TimeSpan time: return ((long) time.TotalMilliseconds).ToString();
                default:
                    return value.ToString();
            }
        }
    }
}
