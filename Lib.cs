using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace HeaderVerifier
{
    class Lib
    {
        public static async Task<Header> CheckAgainstPatternsAsync(string blobName, string containerName, ILogger logger)
        {
            int regexCount = 0;

            var blobClient = Helpers.GetContainerClient(Helpers.GetStorageConnection(), containerName).GetBlobClient(blobName);

            var headerPattern = Helpers.GetConfig().GetSection("HeaderRegexPattern")?
                                      .GetChildren()?.ToList()
                                      ?? throw new ArgumentNullException();

            var header = new Header();

            using (var stream = await blobClient.OpenReadAsync())
            {
                using (var reader = new StreamReader(stream))
                {
                    while (!reader.EndOfStream)
                    {
                        var row = reader.ReadLine();

                        foreach (var pattern in headerPattern)
                        {
                            var regex = new Regex(pattern.Value);
                            if (regex.Match(row).Success)
                            {
                                logger.LogInformation($"Found header matching pattern: {pattern.Value} in file: {blobClient.Name}");

                                header.Lines.Add(row + Environment.NewLine);

                                regexCount++;
                                if (regexCount == headerPattern.Count)
                                {
                                    header.IsPartial = false;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            return header;
        }

        public static async Task CopyBlobAsync(string blobName, string containerName, Header header, ILogger logger)
        {
            //Verifiy if blob has already a header
            var blobHeaderVerification = await CheckAgainstPatternsAsync(blobName, containerName, logger);
            var hasPartialHeaders = blobHeaderVerification.HasHeader && blobHeaderVerification.IsPartial;

            var destinationContainer = Helpers.GetConfig()["TargetContainer"] ?? throw new ArgumentNullException("Target container cannot be null and needs to be set in configuration.");
            var storageConnection = Helpers.GetStorageConnection();

            var blockIds = new List<string>();
            var sourceBlobClient = Helpers.GetBlockBlobClient(storageConnection, blobName, containerName);
            var destBlobClient = Helpers.GetBlockBlobClient(storageConnection, blobName, destinationContainer);

            BlobLeaseClient blobLeaseClient = sourceBlobClient.GetBlobLeaseClient();

            try
            {
                if (await destBlobClient.ExistsAsync())
                {
                    logger.LogError("Skipping copy. Blob exists at destination");
                    return;
                }

                BlobLease blobLease = await blobLeaseClient.AcquireAsync(new TimeSpan(-1));
                logger.LogWarning("Blob lease acquired. LeaseId = {0}", blobLease.LeaseId);

                /*
                 * Creating memory streams and block IDs to store header lines into block blob.
                 * This part is skipped if it already contains a header. There is only one occasion (copying the original header file) were
                 * the part below this code block needs to be executed even though no headers are added. 
                 **/
                if (!blobHeaderVerification.HasHeader || hasPartialHeaders)
                {
                    //Remove header lines if already partially present in existing blob.
                    var headerLinesWithoutPartials = header.Lines.Except(blobHeaderVerification.Lines).ToList();

                    logger.LogWarning($"Adding header to block {blobName}");
                    foreach (var headerRow in headerLinesWithoutPartials)
                    {
                        var rowId = Helpers.Base64Encode(Guid.NewGuid().ToString());
                        blockIds.Add(rowId);
                        using (var headerStream = new MemoryStream(Encoding.UTF8.GetBytes(headerRow)))
                        {
                            await destBlobClient.StageBlockAsync(rowId, headerStream);
                        }
                    }
                }

                //Reading existing blob content into stream.
                using (var s = await sourceBlobClient.OpenReadAsync())
                {
                    var reader = new StreamReader(s);
                    var line1 = reader.ReadLine();
                    var regex = new Regex(@"^[a-zA-Z]:\\[a-zA-Z]+\\[0-9]+_[0-9]+-[0-9]+ [a-zA-Z]+\.txt$");

                    if (regex.Match(line1).Success)
                    {
                        s.Position = line1.Length + Environment.NewLine.Length;
                        var rowId = Helpers.Base64Encode(Guid.NewGuid().ToString());
                        blockIds.Insert(0, rowId);
                        using (var headerStream = new MemoryStream(Encoding.UTF8.GetBytes(line1 + Environment.NewLine)))
                        {
                            await destBlobClient.StageBlockAsync(rowId, headerStream);
                        }
                    }

                    //Prepare blockId for main content stream.
                    var mainContentBlockId = Helpers.Base64Encode(Guid.NewGuid().ToString());
                    blockIds.Add(mainContentBlockId);

                    //Staging block blob block with existing blob content.
                    logger.LogWarning($"Reading blob content into new block: {blobName}");
                    await destBlobClient.StageBlockAsync(mainContentBlockId, s);
                }

                //Write/Commit blob into target container
                await destBlobClient.CommitBlockListAsync(blockIds.ToArray());
                logger.LogWarning($"Blocks commited into blob: {blobName}");

            }
            catch (RequestFailedException e)
            {
                if (e.Status == (int)HttpStatusCode.PreconditionFailed)
                {
                    Console.WriteLine(
                        @"Precondition failure as expected. The lease ID was not provided.");
                }
                else
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }
            finally
            {
                await blobLeaseClient.BreakAsync();
            }
        }
    }
}
