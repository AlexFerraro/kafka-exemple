using Confluent.Kafka;
using kafka_exemple.Interfaces;
using kafka_exemple.Kafka.SendMessage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_exemple.Services
{
    public sealed class KafkaRetryBackgroundService : BackgroundService
    {
        private readonly IKafkaConsumerFactory<Null, string> _kafkaConsumerFactory;
        private readonly ILogger<KafkaRetryBackgroundService> _logger;

        public KafkaRetryBackgroundService(IKafkaConsumerFactory<Null, string> kafkaConsumerFactory
            , ILogger<KafkaRetryBackgroundService> logger)
        {
            _kafkaConsumerFactory = kafkaConsumerFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Run(async () =>
            {
                KafkaMessage deserializedMessage = null;

                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumerRetry = _kafkaConsumerFactory.BuildXXYConsumer();

                    try
                    {
                        var consumeResult = consumerRetry.Consume(stoppingToken);

                        if (consumeResult?.Message is not null)
                        {
                            var consumedMessage = consumeResult.Message.Value;

                            deserializedMessage = JsonSerializer.Deserialize<KafkaMessage>(consumedMessage);

                            consumerRetry.Commit(consumeResult);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError($"[Kafka Consumer] -> Falha ao consumir mensagem do tópico {ex.ConsumerRecord.Topic}. Código: {ex.Error.Code}, Motivo: {ex.Error.Reason}. Data/Hora: {DateTime.Now}");
                    }
                    catch (KafkaException commitEx)
                    {
                        _logger.LogError($"[Kafka Consumer Commit] -> ID: {deserializedMessage.Id} - Falha ao realizar o commit da mensagem para o kafka. Código: {commitEx.Error.Code}, Motivo: {commitEx.Error.Reason}. Data/Hora: {DateTime.Now}");
                    }
                }
            }, stoppingToken);
        }
    }
}
