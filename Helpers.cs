using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;

namespace HeaderVerifier
{
    public class Helpers
    {
        public static string Base64Encode(string inputText)
        {
            if (string.IsNullOrEmpty(inputText))
            {
                return string.Empty;
            }
            var bytes = Encoding.UTF8.GetBytes(inputText);
            return Convert.ToBase64String(bytes);
        }

        public static BlockBlobClient GetBlockBlobClient(string storageConnection, string blobName, string containerName)
            =>  new BlockBlobClient(storageConnection, containerName, blobName);

        public static string GetBlobFolderPath(string blobFullName)
        {
            var seperatorPos = blobFullName.LastIndexOf("/");
            return seperatorPos < 0 ? string.Empty : blobFullName.Substring(0, seperatorPos);
        }

        public static BlobContainerClient GetContainerClient(string storageConnection, string containerName)
        {
            var blobServiceClient = new BlobServiceClient(storageConnection);
            return blobServiceClient.GetBlobContainerClient(containerName);
        }

        public static string GetStorageConnection() => GetConfig()["StorageConnectionString"];

        public static IConfigurationRoot GetConfig()
        {
            return new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddEnvironmentVariables()
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .Build();
        }
    }
}
