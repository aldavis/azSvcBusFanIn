using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Configuration;

namespace Sender
{
    public class TopicOneSender
    {
        private readonly NamespaceManager _namespaceManager;
        private readonly string _topicName;

        private MessageSender sender;

        public TopicOneSender(NamespaceManager namespaceManager,string topicName)
        {
            _namespaceManager = namespaceManager;
            _topicName = topicName;

            Console.ForegroundColor = ConsoleColor.Cyan;

            if (!_namespaceManager.TopicExists(_topicName))
            {
                Console.WriteLine($"Topic {_topicName} does not exist, creating it");

                var description = new TopicDescription(_topicName);
                description.AutoDeleteOnIdle = TimeSpan.FromMinutes(10);

                _namespaceManager.CreateTopic(description);

                Console.WriteLine("Topic Created");
            }

            var serviceBusNamespace = ConfigurationManager.AppSettings["ServiceBusNamespace"];
            var name = ConfigurationManager.AppSettings["senderKeyName"];
            var key = ConfigurationManager.AppSettings["SenderKey"];

            var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(name, key);


            Console.WriteLine("Creating Sender");

            var senderFactory = MessagingFactory.Create(
            new Uri(serviceBusNamespace),
            new MessagingFactorySettings
            {
                TransportType = TransportType.Amqp,
                TokenProvider = tokenProvider
            });

            sender = senderFactory.CreateMessageSender(_topicName);
            sender.RetryPolicy = new RetryExponential(TimeSpan.Zero, TimeSpan.FromSeconds(5), 10);
        }



        public void Send()
        {

            dynamic data = new[]
{
                new {name = "Banner", firstName = "Bruce", universe = "marvel"},
                new {name = "Stark", firstName = "Tony", universe = "marvel"},
                new {name = "Rogers", firstName = "Steven", universe = "marvel"},
                new {name = "Parker", firstName = "Peter", universe = "marvel"},
                new {name = "Wayne", firstName = "Bruce", universe = "dc"},
                new {name = "Allen", firstName = "Barry", universe = "dc"},
                new {name = "Kent", firstName = "Clark", universe = "dc"},
                new {name = "Lane", firstName = "Lois", universe = "dc"},
                new {name = "Stacey", firstName = "Gwen", universe = "marvel"}
            };


            for (var i = 0; i < data.Length; i++)
            {
                var message = new BrokeredMessage(JsonConvert.SerializeObject(data[i]))
                {
                    ContentType = "application/json",
                    Label = "mylabel",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                message.Properties.Add("universe", data[i].universe);

                try
                {
                    sender.Send(message);
                }
                catch (MessagingException exception)
                {
                    Console.WriteLine(exception);
                }


                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                    Console.ResetColor();
                }
            }
            sender.Close();
        }
    }
}
