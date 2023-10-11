using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer
{
     class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "dad-puc-igor.servicebus.windows.net:9093",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = "Endpoint=sb://dad-puc-igor.servicebus.windows.net/;SharedAccessKeyName=dad-kafka-producer;SharedAccessKey=xWfGw9nI8HyZFgEbgyNbj8AewW71x0NLl+AEhIRCKw8="
            };

            var topic = "dad-kafka";

            var message = new
            {
                name = "Igor Frederico Gomes Quaresma",
                login_id = "1366404@sga.pucminas.br",
                group = 5
            };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                var jsonMessage = Newtonsoft.Json.JsonConvert.SerializeObject(message);
                var deliveryReport = await producer.ProduceAsync(topic, new Message<string, string> { Key = null, Value = jsonMessage });
                Console.WriteLine(jsonMessage);
                Console.WriteLine($"Mensagem enviada para: {deliveryReport.TopicPartitionOffset}");

                Console.ReadKey();
            }
        }
    }
}
