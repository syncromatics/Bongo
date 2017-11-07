using Akka.Actor;

namespace Bongo.Actors.Tables
{
    internal class TableManagerResponse
    {
        public IActorRef Manager { get; set; }

        public TableManagerResponse(IActorRef manager)
        {
            Manager = manager;
        }
    }
}
