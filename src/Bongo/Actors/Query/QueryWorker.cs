using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Bongo.Actors.Pools.Messages;
using Bongo.Actors.Query.Messages;
using Bongo.InnerClient;
using Bongo.InnerClient.Impala;
using Bongo.TableDefinitions;

namespace Bongo.Actors.Query
{
    internal class QueryWorker : ReceiveActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }

        private readonly ActorSelection _poolManager;

        public QueryWorker()
        {
            _poolManager = Context.System.ActorSelection("/user/poolManager");

            Become(HandleRequests);
        }

        private void HandleRequests()
        {
            Receive<QueryRequest>(query =>
            {
                var sender = Sender;
                Become(() => ProcessQueryRequest(query, sender));
            });
        }

        private void ProcessQueryRequest(QueryRequest request, IActorRef sender)
        {
            ReceiveAsync<ConnectionLease>(async lease =>
            {
                try
                {
                    var response =
                        await lease.Connection.Ask(new LeaseQuery(request.QueryString, lease.ConnectionLeaseId));
                    switch (response)
                    {
                        case QueryResponse queryResponse:
                            var parsedResponse = ParseResults(queryResponse, request.ResponseType);
                            sender.Tell(parsedResponse);
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

                Become(HandleRequests);
                Stash.UnstashAll();
            });

            ReceiveAny(_ => Stash.Stash());

            _poolManager.Tell(new ConnectionLeaseRequest());
        }

        private List<object> ParseResults(QueryResponse response, Type requestedType)
        {
            var columns = response.Metadata.Schema.FieldSchemas
                .Select((schema, index) => (Index: index, Name: schema.Name, Type: schema.Type))
                .ToList();

            var properties = requestedType
                .GetProperties()
                .Select(property =>
                {
                    var columnNameAttribute = Attribute
                        .GetCustomAttributes(property, typeof(ColumnNameAttribute))
                        .Cast<ColumnNameAttribute>()
                        .FirstOrDefault();

                    var columnName = !string.IsNullOrWhiteSpace(columnNameAttribute?.Name)
                        ? columnNameAttribute.Name
                        : property.Name.ToLower();

                    return (Name: columnName, Property: property);
                })
                .ToList();

            List<Action<object, string[]>> valueActions = new List<Action<object, string[]>>();

            foreach (var column in columns)
            {
                var matchingProperties = properties
                    .Where(property => property.Name.ToLower() == column.Name.ToLower())
                    .ToList();

                if(!matchingProperties.Any())
                    throw new Exception($"Property '{column.Name}' is umatched in object");

                var matchingProperty = matchingProperties.First();
                var valueFunction = GetValueFunction(matchingProperty.Property.PropertyType);

                void PropertyAction(object o, IList<string> list)
                {
                    matchingProperty.Property.SetValue(o, valueFunction(list[column.Index]));
                }

                valueActions.Add(PropertyAction);
            }

            return response.Results
                .Select(result => result.Split(response.Metadata.Delimiter.ToCharArray()))
                .Select(result =>
                {
                    var obj = Activator.CreateInstance(requestedType);
                    foreach (var action in valueActions)
                    {
                        action(obj, result);
                    }
                    return obj;
                })
                .ToList();
        }

        private Func<string, object> GetValueFunction(Type type)
        {
            var typeCode = Type.GetTypeCode(type);
            switch (typeCode)
            {
                case TypeCode.Int32: return str => int.Parse(str);
                case TypeCode.Int64: return str => long.Parse(str);
                case TypeCode.Double: return str => double.Parse(str);
                case TypeCode.String: return str => str;
                case TypeCode.DateTime: return str => DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(str)).DateTime;
            }

            switch (type.Name)
            {
                case "DateTimeOffset":
                    return str => DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(str));
                case "TimeSpan":
                    return str => TimeSpan.FromMilliseconds(long.Parse(str));
                case "Nullable`1":
                    var innerFunction = GetValueFunction(type.GetGenericArguments()[0]);
                    return str => str == "NULL" ? null : innerFunction(str);
            }

            throw new Exception($"'{type.Name}' type unknown. " +
                                $"Supported types are int, long, double, string, DateTime, DateTimeOffset, and TimeSpan");
        }
    }
}
