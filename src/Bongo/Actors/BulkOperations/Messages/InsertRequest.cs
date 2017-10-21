using System.Collections.Generic;

namespace Bongo.Actors.BulkOperations.Messages
{
    internal class InsertRequest
    {
        public List<object> Items { get; }

        public bool Upsert { get; set; }

        public InsertRequest(List<object> items, bool upsert = false)
        {
            Items = items;
            Upsert = upsert;
        }
    }
}
