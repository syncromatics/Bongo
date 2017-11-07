using System;
using System.Collections.Generic;
using Akka.Actor;

namespace Bongo.Actors.Tables
{
    internal class TablesManager : ReceiveActor
    {
        public TablesManager()
        {
            Become(() => WaitForRequests(new Dictionary<Type, IActorRef>()));
        }

        private void WaitForRequests(IDictionary<Type, IActorRef> managers)
        {
            Receive<TableManagerRequest>(request =>
            {
                if (!managers.ContainsKey(request.EntityType))
                {
                    var manager = CreateManagerForType(request.EntityType);
                    managers.Add(request.EntityType, manager);
                }

                Sender.Tell(new TableManagerResponse(managers[request.EntityType]));
            });
        }

        private IActorRef CreateManagerForType(Type type)
        {
            return Context.ActorOf(Props.Create(() => new TableManager(type)));
        }
    }
}
