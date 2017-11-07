using System;

namespace Bongo.Actors.Tables
{
    internal class TableManagerRequest
    {
        public Type EntityType { get; }

        public TableManagerRequest(Type entityType)
        {
            EntityType = entityType;
        }
    }
}
