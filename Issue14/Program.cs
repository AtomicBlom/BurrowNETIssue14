using System;
using Burrow;
using Burrow.Extras;

namespace Issue14
{
	internal class Program
	{
		public const string SubscriptionName = "RabbitMQIssue14";

		private const string Hostname = "MBMSG01.dev.local";
		private const string Username = "steven";
		private const string Password = "mrblur";
		private const int Port = 5672;
		private const string VirtualHost = "steven";

		private const int ThreadCount = 8;

		private void Start()
		{
			var connectionString = string.Format("Host={0};Username={1};Password={2};Port={3};VirtualHost={4}", Hostname, Username, Password, Port, VirtualHost);

			var setup = new RabbitSetup(connectionString);
			var routeSetupData = new RouteSetupData
				{
					ExchangeSetupData = new ExchangeSetupData
						{
							Durable = true,
							ExchangeType = "direct"
						},
					QueueSetupData = new QueueSetupData
						{
							Durable = true,
						},
					SubscriptionName = SubscriptionName,
					RouteFinder = new DefaultRouteFinder()
				};
			setup.CreateRoute<OrderDetail>(routeSetupData);


			var subscriptionTunnel = RabbitTunnel.Factory.Create(connectionString);
			_subscription = new SubscriptionHandler[ThreadCount];
			for (var i = 0; i < ThreadCount; ++i)
			{
				_subscription[i] = new SubscriptionHandler(subscriptionTunnel);
				_subscription[i].Start();
			}

			var publishTunnel = RabbitTunnel.Factory.Create(connectionString);

			Console.WriteLine("Escape - Quit");
			Console.WriteLine("P - Publish");
			Console.WriteLine("B - Break it (Double-ack)");

			ConsoleKeyInfo key;
			//Console.Write("> ");
			var r = new Random();
			while ((key = Console.ReadKey()).Key != ConsoleKey.Escape)
			{
				Console.WriteLine();
				switch (key.Key)
				{
					case ConsoleKey.P:
						Console.WriteLine("Publishing 20 messages");
						for (int i = 0; i < 20; ++i)
						{
							publishTunnel.Publish(new OrderDetail
								{
									Name = "Google Nexus 7",
									Color = "Black",
									Amount = 1
								});
						}
						break;
					case ConsoleKey.B:
						_subscription[r.Next(ThreadCount)].DoubleAck();
						break;
				}
				//Console.Write("> ");
			}
			setup.DestroyRoute<OrderDetail>(routeSetupData);
		}

		private SubscriptionHandler[] _subscription;

		private static void Main()
		{
			var p = new Program();
			p.Start();
		}
	}
}