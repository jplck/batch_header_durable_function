using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace HeaderVerifier
{
    public static class HeaderVerifier
    {
        [FunctionName("Verifier_Orchestrator")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var gridEvents = context.GetInput<List<EventGridEventPayload>>();

            if (gridEvents == null) { return; }

            var tasks = new List<Task>();

            foreach (EventGridEventPayload gridEvent in gridEvents)
            {
                var blobUri = new Uri(gridEvent.Data.Url);

                var blobUriBuilder = new BlobUriBuilder(blobUri);
                var prefix = Helpers.GetBlobFolderPath(blobUriBuilder.BlobName);

                var entityId = new EntityId(nameof(StatusEntity), prefix);
                var statusEntityProxy = context.CreateEntityProxy<IStatusEntity>(entityId);

                var containerName = blobUriBuilder.BlobContainerName;

                var existingHeader = await statusEntityProxy.GetHeaderAsync();

                if (existingHeader != null && existingHeader.HasHeader)
                {
                    tasks.Add(context.CallActivityAsync("Verifier_CopyBlob_Activity", (blobUriBuilder.BlobName, containerName, existingHeader)));
                }
                else
                {
                    var header = await context.CallActivityAsync<Header>("Verifier_ReadVerifyBlob_Activity", blobUri);
                    if (header != null && header.HasHeader && !header.IsPartial)
                    {
                        log.LogInformation($"Added header for prefix {prefix} into entity storage.");
                        statusEntityProxy.SetHeaderAsync(header);
                        var blobItems = await context.CallActivityAsync<List<string>>("Verifier_ReadBlobsFromPath_Activity", blobUri);

                        foreach (var blobName in blobItems)
                        {
                            tasks.Add(context.CallActivityAsync("Verifier_CopyBlob_Activity", (blobName, containerName, header)));
                        }
                    }
                }
            }

            await Task.WhenAll(tasks);
        }

        [FunctionName("Verifier_ReadBlobsFromPath_Activity")]
        public static async Task<List<string>> RunVerifierReadBlobsFromPathActivity([ActivityTrigger] Uri blobUri, ILogger log)
        {
            var storageConnection = Helpers.GetStorageConnection();
            var blobUriBuilder = new BlobUriBuilder(blobUri);
            var prefix = Helpers.GetBlobFolderPath(blobUriBuilder.BlobName);
            var containerClient = Helpers.GetContainerClient(storageConnection, blobUriBuilder.BlobContainerName);

            var blobItems = new List<string>();

            await foreach (BlobItem blobItem in containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, prefix))
            {
                blobItems.Add(blobItem.Name);
            };

            return blobItems;
        }

        [FunctionName("Verifier_ReadVerifyBlob_Activity")]
        public static async Task<Header> RunVerifierReadVerifyBlobActivity([ActivityTrigger] Uri blobUri, ILogger log)
        {
            var blobUriBuilder = new BlobUriBuilder(blobUri);

            var header = await Lib.CheckAgainstPatternsAsync(blobUriBuilder.BlobName, blobUriBuilder.BlobContainerName, log);
            if (header != null && header.HasHeader && !header.IsPartial)
            {
                log.LogInformation($"Found header in blob {blobUriBuilder.BlobName}");
            }
            return header;
        }

        [FunctionName("Verifier_CopyBlob_Activity")]
        public static async Task VerifierCopyBlob_Activity([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            var (blobName, containerName, header) = inputs.GetInput<(string, string, Header)>();
            await Lib.CopyBlobAsync(blobName, containerName, header, log);
        }

        [FunctionName("Verifier_Start_EventGridTrigger")]
        public static async Task<IActionResult> RunVerifierStartEventGridTrigger(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "trigger")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId = await starter.StartNewAsync("Verifier_Orchestrator", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var basePayload = JsonConvert.DeserializeObject<EventGridEvent[]>(requestBody)[0];

            if (basePayload.EventType == @"Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                log.LogTrace(@"Event Grid Validation event received.");
                var data = basePayload.Data as dynamic;
                dynamic validationCode = data?.validationCode;
                return new OkObjectResult($"{{ \"validationResponse\" : \"{validationCode}\" }}");
            }

            var eventPayload = JsonConvert.DeserializeObject<EventGridEventPayload[]>(requestBody);

            var filteredEvents = eventPayload.Where(payload => 
                payload.EventType == @"Microsoft.Storage.BlobCreated" 
                && !string.IsNullOrEmpty(payload.Data.Url)
                && Helpers.GetConfig()["SourceContainer"] == new BlobUriBuilder(new Uri(payload.Data.Url)).BlobContainerName).ToList();

            if (filteredEvents != null && filteredEvents.Count > 0)
            {
                var t = JsonConvert.SerializeObject(filteredEvents);
                await starter.StartNewAsync("Verifier_Orchestrator", Guid.NewGuid().ToString(), filteredEvents);
            }

            return new AcceptedResult();
        }
    }
}