using Confluent.Kafka;
using kafka_exemple.Helpers.Configs;
using kafka_exemple.Interfaces;
using Microsoft.Extensions.Logging;
using System;

namespace kafka_exemple.Factory
{
    public class KafkaProducerFactory<TKey, TValue> : IKafkaProducerFactory<TKey, TValue>
    {
        private IProducer<TKey, TValue>? _producer = null;
        private readonly ILogger<KafkaProducerFactory<TKey, TValue>> _logger;
        private readonly ProducerConfiguration _config;

        public KafkaProducerFactory(ILogger<KafkaProducerFactory<TKey, TValue>> logger
            , ProducerConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        public IProducer<TKey, TValue> BuildProducer()
        {
            _producer ??= new ProducerBuilder<TKey, TValue>(_config.Configuration)
                .SetLogHandler(OnLogHandler)
                .SetErrorHandler(OnErrorHandler)
                .Build();

            return _producer;
        }

        private void OnErrorHandler(IProducer<TKey, TValue> producer, Error error)
        {
            string defaultErrorMessage = $"Code: {error.Code}, Error Reason: {error.Reason}. Origem: {(error.IsLocalError ? "Lib" : "Broker")}. Data/Hora: {DateTime.Now}";

            if (error.IsFatal)
            {
                _logger.LogCritical($"[Kafka Producer] -> Erro fatal no produtor, o mesmo será reiniciado. {defaultErrorMessage}");

                _producer?.Dispose();
                _producer = null;
            }
            else
            {
                _logger.LogError($"[Kafka Producer] -> Erro não fatal no produtor. {defaultErrorMessage}");
            }
        }

        private void OnLogHandler(IProducer<TKey, TValue> producer, LogMessage logMessage)
        {
            LogLevel logLevel = MapSyslogLevelToLogLevel(logMessage.Level);

            if (logLevel <= LogLevel.Warning)
            {
                _logger.Log(logLevel, $"[Kafka Producer] -> Log: {logMessage.Message}");
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
