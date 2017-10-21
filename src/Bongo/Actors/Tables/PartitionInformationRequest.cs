namespace Bongo.Actors.Tables
{
    internal class PartitionInformationRequest
    {
    }

    internal class AddRangePartitionRequest
    {
        public long Start { get; set; }
        public long End { get; set; }

        public AddRangePartitionRequest(long start, long end)
        {
            Start = start;
            End = end;
        }
    }

    internal class AddRangeResponse
    {
        
    }

    internal class HashPartition
    {
        public string[] Columns { get; set; }
        public int Partitions { get; set; }
    }
}
