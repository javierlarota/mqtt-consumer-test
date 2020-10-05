using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Client.Receiving;

namespace mqtt_consumer_test
{
    class Program
    {
        private static IMqttClient mqttClient = null;
        private static IMqttClientOptions mqttOptions = null;

        static void Main(string[] args)
        {
            MqttFactory factory = new MqttFactory();
            mqttClient = factory.CreateMqttClient();
            
            mqttOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", 1883)
                .WithClientId("Subscriber1")
                .WithCleanSession()
                .Build();

            mqttClient.UseDisconnectedHandler(
                new MqttClientDisconnectedHandlerDelegate(e => MqttClient_Disconnected(e)));
            mqttClient.UseApplicationMessageReceivedHandler(
                new MqttApplicationMessageReceivedHandlerDelegate(e => MqttClient_ApplicationMessageReceived(e)));
            mqttClient.ConnectAsync(mqttOptions).Wait();

            // Topic with wildcard topic
            var topic = "casa/basement/fishtank/light";

            mqttClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                .GetAwaiter()
                .GetResult();

            // Publishing messages
            var i = 0;
            while (true)
            {
                i++;
                var payload = $"Message {i}";
                Console.WriteLine($"Publishing message: Topic={topic} Payload={payload}");

                var message = new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(payload)
                    .WithExactlyOnceQoS()
                    .Build();

                mqttClient.PublishAsync(message).Wait();

                Thread.Sleep(15100);
            }
        }

        private static void MqttClient_ApplicationMessageReceived(MqttApplicationMessageReceivedEventArgs e)
        {
            Console.WriteLine(
                $"Message Received: ClientId={e.ClientId} Topic={e.ApplicationMessage.Topic} Payload={e.ApplicationMessage.ConvertPayloadToString()}");
        }

        private static async void MqttClient_Disconnected(MqttClientDisconnectedEventArgs e)
        {
            Debug.WriteLine("Disconnected");
            await Task.Delay(TimeSpan.FromSeconds(5));

            try
            {
                await mqttClient.ConnectAsync(mqttOptions);
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Reconnect failed {0}", ex.Message);
            }
        }
    }
}
