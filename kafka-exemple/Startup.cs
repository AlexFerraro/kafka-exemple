using Confluent.Kafka;
using kafka_exemple.Factory;
using kafka_exemple.Helpers.Configs;
using kafka_exemple.Interfaces;
using kafka_exemple.Kafka.Producer;
using kafka_exemple.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using System;
using System.Linq;
using System.Net;
using System.Net.Mime;
using System.Text.Json;

namespace kafka_exemple
{
    public class Startup
    {
        public Startup(IHostEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddControllers();
            services.AddHealthChecks();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "kafka_exemple", Version = "v1" });
            });

            #region Producer configurations
            ProducerConfiguration producerConfiguration = new()
            {
                Configuration = new ProducerConfig
                {
                    ClientId = Dns.GetHostName(),
                    SaslMechanism = SaslMechanism.Plain,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    EnableDeliveryReports = true,
                    EnableIdempotence = true,
                    Acks = Acks.All,
                    MessageSendMaxRetries = 10,
                    RetryBackoffMs = 300,
                    ReconnectBackoffMaxMs = 30000,
                    ReconnectBackoffMs = 300
                },

                //Topics = Configuration.GetSection("Kafka:Producer:Topics").Get<IEnumerable<string>>()
            };

            //Configuration.Bind("Kafka:Producer:Configuration", producerConfiguration.Configuration);
            Configuration.Bind("Kafka:Producer", producerConfiguration);

            services.AddTransient(p => new ProducerBuilder<Null, string>(producerConfiguration.Configuration).Build());

            services.AddSingleton(producerConfiguration);
            services.AddSingleton<IKafkaProducerFactory<Null, string>, KafkaProducerFactory<Null, string>>();
            services.AddSingleton<IProducerWrapper, ProducerWrapper>();
            #endregion

            #region Consumer configurations
            var consumerConfig = new ConsumerConfiguration
            {
                Configuration = new ConsumerConfig
                {
                    ClientId = Dns.GetHostName(),
                    SaslMechanism = SaslMechanism.Plain,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoOffsetStore = true,
                    EnablePartitionEof = true,
                    EnableAutoCommit = false,
                    SessionTimeoutMs = 50000
                }
            };

            Configuration.Bind("Kafka:Consumer", consumerConfig);

            services.AddSingleton(consumerConfig);
            services.AddSingleton<IKafkaConsumerFactory<Null, string>, KafkaConsumerFactory<Null, string>>();
            #endregion


            services.AddHostedService<KafkaBackgroundService>();
            services.AddHostedService<KafkaRetryBackgroundService>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHealthChecks("/health");
            app.UseHealthChecks("/actuator/health");
            app.UseHealthChecks("/status",
               new HealthCheckOptions()
               {
                   ResponseWriter = async (context, report) =>
                   {
                       var result = JsonSerializer.Serialize(
                           new
                           {
                               statusApplication = report.Status.ToString(),
                               healthChecks = report.Entries.Select(e => new
                               {
                                   check = e.Key,
                                   ErrorMessage = e.Value.Exception?.Message,
                                   status = Enum.GetName(typeof(HealthStatus), e.Value.Status)
                               })
                           });
                       context.Response.ContentType = MediaTypeNames.Application.Json;
                       await context.Response.WriteAsync(result);
                   }
               });

            app.UseSwagger();
            app.UseSwaggerUI(x =>
            {
                x.RoutePrefix = string.Empty;
                x.SwaggerEndpoint("/swagger/v1/swagger.json", "kafka-exemple API v1");
            });

            app.UseHttpsRedirection();
            app.UseRouting();
            app.UseAuthorization();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
