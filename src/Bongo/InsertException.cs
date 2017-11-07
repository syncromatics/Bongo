using System;

namespace Bongo
{
    internal class InsertException : Exception
    {
        public InsertException(string message) : base(message)
        {
        }
    }
}
