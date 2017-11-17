using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Bongo.Actors.Pools.Messages;
using Bongo.InnerClient;
using Bongo.InnerClient.Impala;
using Bongo.TableDefinitions;

namespace Bongo.Actors.Tables
{
    internal class TableManager : ReceiveActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }

        private readonly ActorSelection _poolManager;

        public TableManager(Type entityType)
        {
            _poolManager = Context.System.ActorSelection("/user/poolManager");

            Become(() => CheckForExistence(entityType));
        }

        private void CheckForExistence(Type type)
        {
            ReceiveAsync<ConnectionLease>(async lease =>
            {
                try
                {
                    var tableInformation = GetTableName(type);
                    var response = await lease.Connection.Ask(new LeaseQuery(
                        $"use {tableInformation.Database};",
                        lease.ConnectionLeaseId));

                    switch (response)
                    {
                        case QueryResponse queryResponse:
                            break;
                        case BeeswaxException exception:
                            throw exception;
                        case Exception exception:
                            throw exception;
                    }

                    var tablesResponse = await lease.Connection.Ask(new LeaseQuery("show tables;",
                        lease.ConnectionLeaseId));

                    QueryResponse tables = null;
                    switch (tablesResponse)
                    {
                        case QueryResponse queryResponse:
                            tables = queryResponse;
                            break;
                        case Exception exception:
                            throw exception;
                    }

                    if (tables != null)
                    {
                        var exists = tables.Results.Any(table => table == tableInformation.TableName);
                        if (!exists)
                        {
                            await CreateTable(lease, type);
                        }
                        else
                        {
                            var currentTableDefinition = await lease.Connection
                                .Ask<QueryResponse>(new LeaseQuery($"show create table {tableInformation.TableName};",
                                    lease.ConnectionLeaseId));
                            // todo compare existing table with what we would generate to ensure they are the same
                        }
                    }

                    lease.Connection.Tell(new ConnectionLeaseRelease(lease));

                    Become(() => WaitForRequests(type));
                    Stash.UnstashAll();
                }
                catch (BeeswaxException e)
                {
                    lease.Connection.Tell(new ConnectionLeaseRelease(lease));
                    Context
                        .GetLogger()
                        .Error(e, $"Error in CheckForExistence: {e.Message}");

                    Context.System.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromSeconds(30),
                        _poolManager,
                        new ConnectionLeaseRequest(),
                        Self);
                }
                catch (Exception e)
                {
                    lease.Connection.Tell(new ConnectionLeaseRelease(lease));
                    Context.GetLogger().Error(e, $"Error in CheckForExistence: {e.Message}");

                    Context.System.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromSeconds(30),
                        _poolManager,
                        new ConnectionLeaseRequest(),
                        Self);
                }
            });

            ReceiveAny(_ => Stash.Stash());

            _poolManager.Tell(new ConnectionLeaseRequest());
        }

        private void WaitForRequests(Type type)
        {
            Receive<PartitionInformationRequest>(_ =>
            {
                var sender = Sender;
                Become(() => RetrievePartitionInformation(type, sender));
            });

            Receive<List<AddRangePartitionRequest>>(newPartitions =>
            {
                var sender = Sender;
                Become(() => CreateNewPartitions(type, sender, newPartitions));
            });
        }

        private void RetrievePartitionInformation(Type type, IActorRef sender)
        {
            ReceiveAsync<ConnectionLease>(async lease =>
            {
                try
                {
                    var tableInformation = GetTableName(type);
                    var response = await lease.Connection
                        .Ask(new LeaseQuery($"show create table {tableInformation.Database}.{tableInformation.TableName};", lease.ConnectionLeaseId));

                    QueryResponse currentTableDefinition = null;
                    switch (response)
                    {
                        case QueryResponse queryResponse:
                            currentTableDefinition = queryResponse;
                            break;
                        case BeeswaxException exception:
                            sender.Tell(exception);
                            break;
                        case Exception exception:
                            sender.Tell(exception);
                            break;
                    }

                    if (currentTableDefinition != null)
                    {
                        var hasRangeAttribute = Attribute
                            .GetCustomAttributes(type, typeof(RangePartitionAttribute))
                            .Cast<RangePartitionAttribute>()
                            .Any();

                        if (hasRangeAttribute)
                        {
                            var partitionResponse = await lease.Connection
                                .Ask(new LeaseQuery(
                                    $"show range partitions {tableInformation.Database}.{tableInformation.TableName};",
                                    lease.ConnectionLeaseId));

                            switch (partitionResponse)
                            {
                                case QueryResponse partitionDefinition:
                                    sender.Tell(PartitionInformation.Parse(currentTableDefinition.Results[0],
                                        partitionDefinition.Results));
                                    break;
                                case BeeswaxException exception:
                                    sender.Tell(exception);
                                    break;
                                case Exception exception:
                                    sender.Tell(exception);
                                    break;
                            }
                        }
                        else
                        {
                            sender.Tell(PartitionInformation.Parse(currentTableDefinition.Results[0], new List<string>()));
                        }
                    }
                }
                catch (Exception e)
                {
                    sender.Tell(e);
                }

                lease.Connection.Tell(new ConnectionLeaseRelease(lease));

                Become(() => WaitForRequests(type));

                Stash.UnstashAll();
            });

            ReceiveAny(_ => Stash.Stash());

            _poolManager.Tell(new ConnectionLeaseRequest());
        }

        private void CreateNewPartitions(Type type, IActorRef sender, List<AddRangePartitionRequest> request)
        {
            ReceiveAsync<ConnectionLease>(async lease =>
            {
                var tableInformation = GetTableName(type);
                try
                {
                    foreach (var addRangePartitionRequest in request)
                    {
                        try
                        {
                            var query = $@"
ALTER TABLE
    {tableInformation.Database}.{tableInformation.TableName}
ADD RANGE PARTITION
    {addRangePartitionRequest.Start} <= VALUES < {addRangePartitionRequest.End};
";

                            await lease.Connection.Ask<QueryResponse>(new LeaseQuery(query, lease.ConnectionLeaseId));
                        }
                        catch (BeeswaxException e)
                        {

                        }
                    }

                    sender.Tell(new AddRangeResponse());
                }
                catch (Exception e)
                {
                    sender.Tell(e);
                }

                lease.Connection.Tell(new ConnectionLeaseRelease(lease));

                Become(() => WaitForRequests(type));
                Stash.UnstashAll();
            });

            ReceiveAny(_ => Stash.Stash());

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

        private async Task CreateTable(ConnectionLease client, Type type)
        {
            var createString = GetCreateTable(type);
            var response = await client.Connection.Ask(new LeaseQuery(createString, client.ConnectionLeaseId));

            switch (response)
            {
                case QueryResponse queryResponse:
                    return;
                case Exception exception:
                    throw exception;
            }
        }

        private string GetCreateTable(Type type)
        {
            var tableInformation = GetTableName(type);

            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE {tableInformation.TableName} (");

            List<string> privateKeyColumns = new List<string>();
            foreach (var property in type.GetProperties())
            {
                var typeString = GetImpalaTypeString(property.PropertyType);
                var attributes = Attribute.GetCustomAttributes(property);

                var isNullable = property.PropertyType.Name == "Nullable`1";
                var nullable = isNullable || attributes.Any(attribute => attribute is NullableAttribute)
                    ? "NULL"
                    : "NOT NULL";

                var primaryKey = attributes.Any(attribute => attribute is PrimaryKeyAttribute);

                var columnNameAttribute = attributes.FirstOrDefault(attribute => attribute is ColumnNameAttribute) as ColumnNameAttribute;

                var columnName = !string.IsNullOrWhiteSpace(columnNameAttribute?.Name)
                    ? columnNameAttribute.Name
                    : property.Name.ToLower();

                if(primaryKey)
                    privateKeyColumns.Add(columnName);

                builder.AppendLine($"    {columnName} {typeString} {nullable},");
            }

            builder.AppendLine($"    PRIMARY KEY ({string.Join(", ", privateKeyColumns)})");
            builder.AppendLine(")");

            var hashPartitionAttribute = Attribute
                .GetCustomAttributes(type, typeof(HashPartitionAttribute))
                .Cast<HashPartitionAttribute>()
                .FirstOrDefault();

            if (hashPartitionAttribute != null)
            {
                builder.AppendLine($"PARTITION BY HASH ({string.Join(", ", hashPartitionAttribute.Columns)}) PARTITIONS {hashPartitionAttribute.Partitions}");
            }

            var rangePartitionAttribute = Attribute
                .GetCustomAttributes(type, typeof(RangePartitionAttribute))
                .Cast<RangePartitionAttribute>()
                .FirstOrDefault();

            if (rangePartitionAttribute != null)
            {
                string pre = hashPartitionAttribute != null ? "," : "PARTITION BY";
                builder.AppendLine($"{pre} RANGE({rangePartitionAttribute.Column.ToLower()}) (PARTITION VALUE = 0)");
            }

            if (tableInformation.isKudu)
            {
                builder.AppendLine("STORED AS KUDU");
            }

            var replicasAttribute = Attribute
                .GetCustomAttributes(type, typeof(KuduReplicasAttribute))
                .Cast<KuduReplicasAttribute>()
                .FirstOrDefault();

            if (replicasAttribute != null)
            {
                builder.AppendLine($"TBLPROPERTIES('kudu.num_tablet_replicas' = '{replicasAttribute.NumReplicas}')");
            }

            builder.Append(";");

            return builder.ToString();
        }

        private static string GetImpalaTypeString(Type t)
        {
            var typeCode = Type.GetTypeCode(t);
            switch (typeCode)
            {
                case TypeCode.Int32: return "INT";
                case TypeCode.Int64: return "BIGINT";
                case TypeCode.Double: return "DOUBLE";
                case TypeCode.String: return "STRING";
                case TypeCode.DateTime: return "BIGINT";
            }

            switch (t.Name)
            {
                case "DateTimeOffset":
                    return "BIGINT";
                case "TimeSpan":
                    return "BIGINT";
                case "Nullable`1":
                    return GetImpalaTypeString(t.GetGenericArguments()[0]);
            }

            throw new Exception($"Cannot find Impala type for type '{t.Name}'. " +
                                $"Supported types are int, long, double, string, DateTime, DateTimeOffset, and TimeSpan");
        }
    }
}
