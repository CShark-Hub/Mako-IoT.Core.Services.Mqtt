namespace MakoIoT.Core.Services.Mqtt.Configuration
{
    public class MqttConfig
    {
        public string BrokerAddress { get; set; }
        public int Port { get; set; }
        public bool UseTLS { get; set; }
        public string CACert { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string ClientId { get; set; }

        public string TopicPrefix { get; set; }

        public static string SectionName => "Mqtt";
    }
}
