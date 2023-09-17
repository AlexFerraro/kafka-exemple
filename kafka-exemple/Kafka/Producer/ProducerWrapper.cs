using Confluent.Kafka;
using kafka_exemple.Helpers.Configs;
using kafka_exemple.Interfaces;
using kafka_exemple.Kafka.SendMessage;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace kafka_exemple.Kafka.Producer
{
    public class ProducerWrapper : IProducerWrapper
    {
        private IProducer<Null, string> _producer;
        private readonly IKafkaProducerFactory<Null, string> _producerFactory;
        private readonly ILogger<ProducerWrapper> _logger;
        private readonly IEnumerable<string> _topics;

        public ProducerWrapper(IKafkaProducerFactory<Null, string> producerFactory, ILogger<ProducerWrapper> logger
            , ProducerConfiguration producerConfiguration)
        {
            _producerFactory = producerFactory;
            _logger = logger;
            _topics = producerConfiguration.Topics;
        }

        public async Task<int> SendMessageAsync(string topic, KafkaMessage message)
        {
            try
            {
                var producer = _producer = _producerFactory.BuildProducer();
                var stringPayload = JsonSerializer.Serialize(message);

                var deliveryResult =
                    await producer.ProduceAsync(
                        _topics.First(f => f.Contains(topic))
                        , new Message<Null, string> { Timestamp = Timestamp.Default, Value = stringPayload });

                _logger.LogInformation($"[Kafka Producer] -> ID: {message.Id} - {(deliveryResult.Status == PersistenceStatus.Persisted ? "Enviada com sucesso" : "Falha ao enviar")} para o tópico: {deliveryResult.Topic}, Partição: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}");

                return (int)deliveryResult.Status;
            }
            catch (ProduceException<Null, string> px)
            {
                _logger.LogError($"[Kafka Producer] -> ID: {message.Id} - Falha na tentativa de envio da mensagem para o tópico: {px.DeliveryResult.Topic}. Code: {px.Error.Code}, Reason: {px.Error.Reason}. Data/Hora: {DateTime.Now}");

                throw;
            }
        }

        public void InitTransactions() => _producer.InitTransactions(DateTime.Now.TimeOfDay);
        public void BeginTransaction() => _producer.BeginTransaction();
        public void CommitTransaction() => _producer.CommitTransaction();
        public void AbortTransaction() => _producer.AbortTransaction();
        public void Flush() => _producer.Flush();

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
