using Confluent.Kafka;

namespace kafka_exemple.Interfaces
{
    public interface IKafkaConsumerFactory<TKey, TValue>
    {
        IConsumer<TKey, TValue> BuildXXXConsumer();

        IConsumer<TKey, TValue> BuildXXYConsumer();
    }
}
