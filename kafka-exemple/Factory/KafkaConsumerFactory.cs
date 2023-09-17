using Confluent.Kafka;
using kafka_exemple.Helpers.Configs;
using kafka_exemple.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;

namespace kafka_exemple.Factory
{
    public class KafkaConsumerFactory<TKey, TValue> : IKafkaConsumerFactory<TKey, TValue>
    {
        private IConsumer<TKey, TValue>? _consumerXXX = null;
        private IConsumer<TKey, TValue>? _consumerXXY = null;
        private readonly ILogger<KafkaConsumerFactory<TKey, TValue>> _logger;
        private readonly ConsumerConfiguration _config;

        public KafkaConsumerFactory(ILogger<KafkaConsumerFactory<TKey, TValue>> logger, ConsumerConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        public IConsumer<TKey, TValue> BuildXXXConsumer()
        {
            if (_consumerXXX is null || _consumerXXY.Subscription.Count is 0)
            {
                _consumerXXX = CreateConsumer(_config.GroupId.First());
                _consumerXXX.Subscribe(_config.Topics.First(f => f.Contains("xxx")));
            }

            return _consumerXXX;
        }

        public IConsumer<TKey, TValue> BuildXXYConsumer()
        {
            if (_consumerXXY is null || _consumerXXY.Subscription.Count is 0)
            {
                _consumerXXY = CreateConsumer(_config.GroupId.Last());
                _consumerXXY.Subscribe(_config.Topics.First(f => f.Contains("xxy")));
            }

            return _consumerXXY;
        }

        private IConsumer<TKey, TValue> CreateConsumer(string groupID)
        {
            _config.Configuration.GroupId = groupID;
            return new ConsumerBuilder<TKey, TValue>(_config.Configuration)
                .SetLogHandler(OnLogHandler)
                .SetErrorHandler(OnErrorHandler)
                .Build();
        }
        private void OnErrorHandler(IConsumer<TKey, TValue> consumer, Error error)
        {
            string consumerName = GetConsumerName(consumer);
            string defaultErrorMessage = $"Consumer: {consumerName}, Código: {error.Code}, Motivo do Erro: {error.Reason}. Origem: {(error.IsLocalError ? "Lib" : "Broker")}. Data/Hora: {DateTime.Now}";

            if (error.IsFatal)
            {
                _logger.LogCritical($"[Kafka Consumer] -> Erro fatal no consumidor. O consumidor será reiniciado. {defaultErrorMessage}");

                if (consumer == _consumerXXX)
                {
                    _consumerXXX.Dispose();
                    _consumerXXX = null;
                }
                else if (consumer == _consumerXXY)
                {
                    _consumerXXY.Dispose();
                    _consumerXXY = null;
                }
            }
            else
            {
                _logger.LogError($"[Kafka Consumer] -> Erro não fatal no consumidor. {defaultErrorMessage}");
            }
        }

        private void OnLogHandler(IConsumer<TKey, TValue> consumer, LogMessage logMessage)
        {
            LogLevel logLevel = MapSyslogLevelToLogLevel(logMessage.Level);

            if (logLevel <= LogLevel.Warning)
            {
                _logger.Log(logLevel, $"[Kafka Producer] -> Log: {logMessage.Message}");
            }
        }

        private string GetConsumerName(IConsumer<TKey, TValue> consumer)
        {
            if (consumer == _consumerXXX)
            {
                return "xxx";
            }
            else if (consumer == _consumerXXY)
            {
                return "xxy";
            }
            else
            {
                return "Desconhecido";
            }
        }

        private LogLevel MapSyslogLevelToLogLevel(SyslogLevel syslogLevel)
        {
            return syslogLevel switch
            {
                SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
                SyslogLevel.Error => LogLevel.Error,
                SyslogLevel.Warning => LogLevel.Warning,
                SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
                SyslogLevel.Debug => LogLevel.Debug,
                _ => LogLevel.Information
            };
        }
    }
}
