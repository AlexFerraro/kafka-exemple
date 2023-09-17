using Confluent.Kafka;

namespace kafka_exemple.Interfaces
{
    public interface IKafkaProducerFactory<TKey, TValue>
    {
        IProducer<TKey, TValue> BuildProducer();
    }
}
