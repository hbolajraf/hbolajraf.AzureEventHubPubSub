using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
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
    private const string blobStorageConnectionString = "PUT_YOUR_Blob_Storage_ConnectionString_HERE";
    private const string blobContainerName = "PUT_YOUR_Blob_Container_Name_HERE";

    static async Task Main()
    {
        BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);
        EventProcessorClient processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, ehubNamespaceConnectionString, eventHubName);

        processor.ProcessEventAsync += ProcessEventHandler;
        processor.ProcessErrorAsync += ProcessErrorHandler;

        await processor.StartProcessingAsync();

        Console.WriteLine("Press [enter] to stop the processor.");
        Console.ReadLine();

        await processor.StopProcessingAsync();
    }

    static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        string eventData = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
        Console.WriteLine($"Received event: {eventData}");

        // Process transaction data (e.g., check for fraud)

        await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
    }

    static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        Console.WriteLine($"Error on partition {eventArgs.PartitionId}: {eventArgs.Exception.Message}");
        return Task.CompletedTask;
    }
}