using Confluent.Kafka;
using System.Collections.Generic;

namespace kafka_exemple.Helpers.Configs
{
    public class ConsumerConfiguration
    {
        public IEnumerable<string> Topics { get; set; }
        public IEnumerable<string> GroupId { get; set; }
        public ConsumerConfig Configuration { get; init; }
    }
}
