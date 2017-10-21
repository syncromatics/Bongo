using System;

namespace Bongo.TableDefinitions
{
    public class TableAttribute : Attribute
    {
        public string Name { get; set; }
        public bool IsKuduTable { get; set; }

        public TableAttribute(string name, bool isKudu = false)
        {
            Name = name;
            IsKuduTable = isKudu;
        }
    }

    public class KuduReplicasAttribute : Attribute
    {
        public int NumReplicas { get; set; }

        public KuduReplicasAttribute(int numReplicas)
        {
            NumReplicas = numReplicas;
        }
    }

    public class HashPartitionAttribute : Attribute
    {
        public string[] Columns { get; set; }
        public int Partitions { get; set; }

        public HashPartitionAttribute(string[] columns, int partitions)
        {
            Columns = columns;
            Partitions = partitions;
        }
    }

    public class RangePartitionAttribute : Attribute
    {
        public string Column { get; set; }
        public int BucketSizeInDays { get; set; }
        public int? TTLinDays { get; set; }

        public RangePartitionAttribute(string column, int bucketSizeInDays)
        {
            Column = column;
            BucketSizeInDays = bucketSizeInDays;
        }
    }
}
