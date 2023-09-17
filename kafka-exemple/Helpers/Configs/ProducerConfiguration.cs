using Confluent.Kafka;
using System.Collections.Generic;

namespace kafka_exemple.Helpers.Configs
{
    public class ProducerConfiguration
    {
        public IEnumerable<string> Topics { get; set; }

        public ProducerConfig Configuration { get; set; }
    }
}
