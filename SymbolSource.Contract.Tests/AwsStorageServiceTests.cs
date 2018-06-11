using System;
using System.IO;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.S3;
using Amazon.S3.Model;
using SymbolSource.Contract.Storage;
using SymbolSource.Contract.Storage.Aws;
using Xunit;
using Xunit.Abstractions;

namespace SymbolSource.Contract.Tests
{
    public class AwsStorageServiceTests : StorageServiceTests
    {
        public AwsStorageServiceTests()
        {
            Storage = new StorageTestService(
                new AwsStorageService(),
                Path.GetRandomFileName().Replace(".", ""));
        }

        protected override IStorageService Storage { get; }

        [Fact
           (Skip = "Should be run explicitly")
        ]
        public async Task DeleteAllBuckets()
        {
            var client = new AmazonS3Client(new AmazonS3Config{ RegionEndpoint = RegionEndpoint.EUWest1 });

            var response = await client.ListBucketsAsync();

            foreach (var bucket in response.Buckets)
            {
                var objects = client.ListObjectsAsync(bucket.BucketName);

                try
                {
                    foreach (var o in objects.Result.S3Objects)
                    {
                        await client.DeleteObjectAsync(bucket.BucketName, o.Key);
                    }

                    await client.DeleteBucketAsync(bucket.BucketName);
                }
                catch (Exception)
                {
                }
            }
        }

        [Fact
            (Skip = "Should be run explicitly")
        ]
        public async Task DeleteAllTables()
        {
            var client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig { RegionEndpoint = RegionEndpoint.EUWest1 });

            var response = await client.ListTablesAsync();

            foreach (var table in response.TableNames)
            {
                await client.DeleteTableAsync(table);
            }
        }
    }
}