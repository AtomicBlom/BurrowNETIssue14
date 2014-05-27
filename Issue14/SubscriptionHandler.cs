using System;
using Burrow;

namespace Issue14
{
	internal class SubscriptionHandler
	{
		private readonly ITunnel _subscriptionTunnel;
		private Subscription _subscription;
		private bool _doubleAck;

		public SubscriptionHandler(ITunnel subscriptionTunnel)
		{
			_subscriptionTunnel = subscriptionTunnel;
		}

		public void DoubleAck()
		{
			_doubleAck = true;
		}

		public void Start()
		{
			_subscription = _subscriptionTunnel.SubscribeAsync(new AsyncSubscriptionOption<OrderDetail>
				{
					BatchSize = 1,
					MessageHandler = OnMessageReceived,
					QueuePrefetchSize = 1,
					SubscriptionName = Program.SubscriptionName
				});
		}

		private void OnMessageReceived(OrderDetail message, MessageDeliverEventArgs eventArgs)
		{
			Console.WriteLine("Received");
			_subscription.Ack(eventArgs.DeliveryTag);
			if (_doubleAck)
			{
				Console.WriteLine("Double Acking");
				_doubleAck = false;
				_subscription.Ack(eventArgs.DeliveryTag);
			}
		}
	}
}