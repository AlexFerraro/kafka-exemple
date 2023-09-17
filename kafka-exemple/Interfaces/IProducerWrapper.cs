using kafka_exemple.Kafka.SendMessage;
using System;
using System.Threading.Tasks;

namespace kafka_exemple.Interfaces
{
    public interface IProducerWrapper : IDisposable
    {
        Task<int> SendMessageAsync(string topic, KafkaMessage message);
    }
}
