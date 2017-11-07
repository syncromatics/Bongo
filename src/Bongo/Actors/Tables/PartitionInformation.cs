using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Bongo.Actors.Tables
{
    internal class PartitionInformation
    {
        public HashPartition HashPartition { get; set; }
        public List<RangePartition> RangePartitions { get; set; }

        public static PartitionInformation Parse(string showTableResult, IList<string> rangePartitions)
        {
            var hashSearch = new Regex(@"PARTITION BY HASH \((?<columns>.*?)\) PARTITIONS (?<partitions>.*),", RegexOptions.IgnoreCase);

            HashPartition hashPartition = null;

            if (hashSearch.IsMatch(showTableResult))
            {
                var match = hashSearch.Match(showTableResult);
                var columns = match.Groups["columns"].Value;
                var partitions = match.Groups["partitions"].Value;

                hashPartition = new HashPartition
                {
                    Columns = columns.Split(','),
                    Partitions = int.Parse(partitions)
                };
            }

            var rangeRegex = new Regex("(?<min>.*?) <= VALUES < (?<max>.*)");
            var ranges = new List<RangePartition>();
            foreach (var range in rangePartitions)
            {
                if (!rangeRegex.IsMatch(range))
                    continue;

                var match = rangeRegex.Match(range);
                var min = long.Parse(match.Groups["min"].Value);
                var max = long.Parse(match.Groups["max"].Value);
                ranges.Add(new RangePartition(min, max));
            }

            return new PartitionInformation
            {
                HashPartition = hashPartition,
                RangePartitions = ranges
            };
        }
    }

    public class RangePartition
    {
        public long Start { get; }
        public long End { get; }

        public RangePartition(long start, long end)
        {
            Start = start;
            End = end;
        }

        public bool Contains(long time)
        {
            return time >= Start && time < End;
        }
    }
}
