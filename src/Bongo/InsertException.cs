using System;

namespace Bongo
{
    public class InsertException : Exception
    {
        public InsertException(string message) : base(message)
        {
        }
    }
}
