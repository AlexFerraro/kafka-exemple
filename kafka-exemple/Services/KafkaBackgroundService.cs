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
    public sealed class KafkaBackgroundService : BackgroundService
    {
        private readonly IProducerWrapper _producerWrapper;
        private readonly IKafkaConsumerFactory<Null, string> _kafkaConsumerFactory;
        private readonly ILogger<KafkaBackgroundService> _logger;

        public KafkaBackgroundService(IProducerWrapper producerWrapper
                                        , IKafkaConsumerFactory<Null, string> kafkaConsumerFactory
                                        , ILogger<KafkaBackgroundService> logger)
        {
            _producerWrapper = producerWrapper;
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
                    var consumerXXX = _kafkaConsumerFactory.BuildXXXConsumer();

                    try
                    {
                        var consumeResult = consumerXXX.Consume(stoppingToken);

                        if (consumeResult?.Message is not null)
                        {
                            var consumedMessage = consumeResult.Message.Value;

                            deserializedMessage = JsonSerializer.Deserialize<KafkaMessage>(consumedMessage);

                            consumerXXX.Commit(consumeResult);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError($"[Kafka Consumer] -> Falha ao consumir mensagem do tópico {ex.ConsumerRecord.Topic}. Código: {ex.Error.Code}, Motivo: {ex.Error.Reason}. Data/Hora: {DateTime.Now}");
                    }
                    catch (KafkaException commitEx)
                    {
                        _logger.LogError($"[Kafka Consumer Commit] -> ID: {deserializedMessage.Id} - Falha ao realizar o commit da mensagem para o tópic. Código: {commitEx.Error.Code}, Motivo: {commitEx.Error.Reason}. Data/Hora: {DateTime.Now}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Falha ao processar a mensagem: {deserializedMessage.Id}. Error: {ex.Message}");

                        await ProduceTopicRetry(deserializedMessage);
                    }
                }
            }, stoppingToken);
        }

        private async Task ProduceTopicRetry(KafkaMessage message)
        {
            try
            {
                var kafkaResponse = await _producerWrapper.SendMessageAsync("xxy", message);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Falha ao enviar mensagem para tópico de retentativa [{ex.Message}]");
            }
        }
    }
}
