using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using MakoIoT.Core.Services.Interface;
using MakoIoT.Core.Services.Mqtt.Configuration;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace MakoIoT.Core.Services.Mqtt
{
    public class MqttCommunicationService : ICommunicationService
    {
        private const string directTopicPrefix = "direct";
        private const string broadcastTopicPrefix = "broadcast";


        private readonly ILogger<MqttCommunicationService> _logger;
        private IMqttClient? _mqttClient;
        private readonly MqttConfig _config;
        private readonly X509Certificate _caCert;
        private readonly MqttFactory _mqttFactory;

        public MqttCommunicationService(ILogger<MqttCommunicationService> logger, MqttConfig config)
        {
            _logger = logger;
            _config = config;

            if (!String.IsNullOrEmpty(_config.CACert))
                _caCert = new X509Certificate(_config.CACert);

            _mqttFactory = new MqttFactory();
        }

        public event EventHandler<string> MessageReceived;
        public bool CanSend => MqttClientConnected;
        public string ClientName => _config.ClientId;
        public string ClientAddress => InternalGetIpAddress();

        private bool MqttClientConnected => _mqttClient != null && _mqttClient.IsConnected;

        public void Publish(string messageString, string messageType)
        {
            string topic = $"{_config.TopicPrefix}/{broadcastTopicPrefix}/{messageType}";
            _logger.LogDebug($"Publishing message to topic: {topic}");
            PublishInternal(messageString, topic);
        }

        public void Connect(string[] subscriptions)
        {
            if (_mqttClient == null)
            {
                _mqttClient = _mqttFactory.CreateMqttClient();

                _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;
            }

            if (!_mqttClient.IsConnected)
            {
                var ob = new MqttClientOptionsBuilder()
                    .WithTcpServer(_config.BrokerAddress, _config.Port)
                    .WithClientId(_config.ClientId);
                ob = _config.UseTLS ? ob.WithTls() : ob;
                ob = String.IsNullOrEmpty(_config.Username)
                    ? ob
                    : ob.WithCredentials(_config.Username, _config.Password);

                _mqttClient.ConnectAsync(ob.Build()).ConfigureAwait(false).GetAwaiter().GetResult();

                var topics = GetSubscriptionTopics(subscriptions);

                var mqttSubscribeOptions = _mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(f =>
                    {
                        foreach (var topic in topics)
                        {
                            _logger.LogDebug($"Subscribing to topic: {topic}");
                            f.WithTopic(topic).WithAtLeastOnceQoS();
                        }
                    })
                    .Build();

                _mqttClient.SubscribeAsync(mqttSubscribeOptions).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        public void Disconnect()
        {
            _mqttClient.DisconnectAsync().Wait();
        }

        public void Send(string messageString, string recipient)
        {
            string topic = $"{_config.TopicPrefix}/{directTopicPrefix}/{recipient}";
            _logger.LogDebug($"Publishing message to topic: {topic}");
            PublishInternal(messageString, topic);
        }

        private void PublishInternal(string messageString, string topic)
        {
            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(messageString)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();


            _mqttClient.PublishAsync(applicationMessage).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            _logger.LogDebug($"Received message from topic: {e.ApplicationMessage.Topic}");
            MessageReceived?.Invoke(this, 
                Encoding.UTF8.GetString(e.ApplicationMessage.Payload, 0, e.ApplicationMessage.Payload.Length));

            return Task.CompletedTask;
        }

        private string[] GetSubscriptionTopics(string[] subscriptions)
        {
            var topics = new string[subscriptions.Length + 1];
            topics[0] = $"{_config.TopicPrefix}/{directTopicPrefix}/{_config.ClientId}";
            for (int i = 1; i < subscriptions.Length + 1; i++)
            {
                topics[i] = $"{_config.TopicPrefix}/{broadcastTopicPrefix}/{subscriptions[i-1]}";
            }

            return topics;
        }

        private string InternalGetIpAddress()
        {
            return Dns.GetHostEntry(Environment.MachineName).AddressList
                .FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork)?.ToString() ?? "127.0.0.1";
        }
    }
}
