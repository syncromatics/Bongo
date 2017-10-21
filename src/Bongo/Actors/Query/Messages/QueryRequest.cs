using System;

namespace Bongo.Actors.Query.Messages
{
    internal class QueryRequest
    {
        public Type ResponseType { get; }
        public string QueryString { get; set; }

        public QueryRequest(string queryString, Type responseType)
        {
            ResponseType = responseType;
            QueryString = queryString;
        }
    }
}
