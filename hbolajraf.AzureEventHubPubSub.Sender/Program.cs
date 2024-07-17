using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading.Tasks;


/// <summary>
/// This Project was created as a part of my post : Real-Time Financial Transaction Monitoring in Banking Using Azure Event Hub
/// that you can foun Here : https://hbolajraf.net/
/// </summary>
class Program
{
    private const string ehubNamespaceConnectionString = "PUT_YOUR_Eventhub_Namespace_ConnectionString_HERE";
    private const string eventHubName = "PUT_YOUR_Eventhub_Name_HERE";
    static async Task Main(string[] args)
    {
        await using (var producerClient = new EventHubProducerClient(ehubNamespaceConnectionString, eventHubName))
        {
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 0; i < 100; i++)
            {
                var transaction = new
                {
                    TransactionId = Guid.NewGuid(),
                    Amount = new Random().Next(1, 10000),
                    Timestamp = DateTime.UtcNow
                };

                var eventData = new EventData(Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(transaction)));

                if (!eventBatch.TryAdd(eventData))
                {
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            await producerClient.SendAsync(eventBatch);
            Console.WriteLine("A batch of 100 events has been published.");
        }
    }
}