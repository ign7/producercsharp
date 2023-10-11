﻿/*Nome: Igor Frederico Gomes Quaresma
Professor: - (2751100) Desenvolvimento de Aplicações Distribuídas
 Descricao: Implementar um producer, que vai enviar a mensagem para um tópico específico no Event Hub, utilizando c#
 */

using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        string brokerList = "puc-dad.servicebus.windows.net:9093";
        string topicName = "dad-atividade-kafka";
        string principal = "$ConnectionString";
        string secret = "Endpoint=sb://puc-dad.servicebus.windows.net/;SharedAccessKeyName=aluno-dad;SharedAccessKey=Kds6a1hYMueVSSbu7bgiKXBpjBbzT4Kol+AEhNOt3FQ=";

        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = principal,
            SaslPassword = secret,

        };

         var producer = new ProducerBuilder<string, string>(config).Build();

        try
        {
            var data = new
            {
                name = "Igor Frederico Gomes Quaresma",
                login_id = "1366404@sga.pucminas.br",
                group = 5
            };

            string jsonValue = JsonConvert.SerializeObject(data);

            var message = new Message<string, string>
            {
                Key = null,
                Value = jsonValue
            };

            var deliveryReport = await producer.ProduceAsync(topicName, message);
            Console.WriteLine("Mensagem enviada: " + message.Value.ToString());
            Console.WriteLine($"Mensagem enviada para: {deliveryReport.TopicPartitionOffset}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Erro ao enviar mensagem: {e.Error.Reason}");
        }
    }
}