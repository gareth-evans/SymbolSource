using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;

namespace SymbolSource.Contract.Storage.Aws
{
    public class AwsStorageService : IStorageService
    {
        private const string FeedPrefix = "symbolsource-feed-";
        private const string NamedFeedPrefix = "named-";
        private const string DefaultFeedName = "default";

        private static readonly RegionEndpoint Region = RegionEndpoint.EUWest1;
        private static IAmazonS3 _s3Client;

        public AwsStorageService()
        {
            _s3Client = new AmazonS3Client(new AmazonS3Config
            {
                RegionEndpoint = Region
            });
        }

        public IStorageFeed GetFeed(string feedName)
        {
            var bucketName = GetBucketName(feedName);
            var tableName = GetTableName(bucketName);

            var dynamoConfig = new AmazonDynamoDBConfig
            {
                RegionEndpoint = Region
            };

            var s3Config = new AmazonS3Config
            {
                RegionEndpoint = Region
            };

            var packageTable = new PackageTable(dynamoConfig, tableName);
            var packageBucket = new PackageBucket(s3Config, bucketName);

            return new AwsStorageFeed(bucketName, packageTable, packageBucket, s3Config, dynamoConfig);
        }

        public IEnumerable<string> QueryFeeds()
        {
            var bucketsResponse = _s3Client.ListBuckets();

            return bucketsResponse.Buckets
                .Select(container => container.BucketName)
                .Where(name => name.StartsWith(FeedPrefix))
                .Select(name => name.Substring(FeedPrefix.Length))
                .Select(name =>
                {
                    if (name == DefaultFeedName)
                        return null;

                    if (name.StartsWith(NamedFeedPrefix))
                        return name.Substring(NamedFeedPrefix.Length);

                    throw new ArgumentOutOfRangeException();
                });
        }

        private static readonly Regex BucketRegex = new Regex("[^A-Za-z0-9\\-\\.]", RegexOptions.Compiled);

        private static string GetBucketName(string feedName)
        {
            if (feedName != null)
            {
                if (feedName.ToLower() != feedName)
                    throw new ArgumentOutOfRangeException(nameof(feedName));

                feedName = NamedFeedPrefix + feedName;
            }

            if (feedName == null)
                feedName = DefaultFeedName;

            feedName = FeedPrefix + feedName;

            return BucketRegex
                .Replace(feedName, "")
                .ToLower();
        }

        private static readonly Regex TableRegex = new Regex("[^A-Za-z0-9_\\-\\.]", RegexOptions.Compiled);

        private static string GetTableName(string containerName)
        {
            return TableRegex.Replace(containerName, "");
        }
    }

    public class AwsStorageFeed : IStorageFeed
    {
        private readonly PackageTable _table;
        private readonly PackageBucket _bucket;
        private readonly AmazonS3Config _s3Config;
        private readonly AmazonDynamoDBConfig _dynamoConfig;

        public AwsStorageFeed(
            string feedName,
            PackageTable table,
            PackageBucket bucket,
            AmazonS3Config s3Config,
            AmazonDynamoDBConfig dynamoConfig)
        {
            _table = table;
            _bucket = bucket;
            _s3Config = s3Config;
            _dynamoConfig = dynamoConfig;
            Name = feedName;
        }

        public string Name { get; }

        public IEnumerable<string> QueryInternals()
        {
            using (var client = new AmazonS3Client(_s3Config))
            {
                var objects = client.ListObjects(_bucket.Name);

                return objects.S3Objects.Select(o => string.Join("/", o.Key.Split('/').Skip(2)));
            }
        }

        public Task<IEnumerable<PackageName>> QueryPackages(PackageState packageState)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<PackageName>> QueryPackages(string userName, PackageState packageState)
        {
            return Task.FromResult(Enumerable.Empty<PackageName>());
        }

        public Task<IEnumerable<PackageName>> QueryPackages(PackageState packageState, string packageNamePrefix, int skip, int take)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<PackageName>> QueryPackages(string userName, PackageState packageState, string packageNamePrefix, int skip, int take)
        {
            throw new NotImplementedException();
        }

        public IPackageStorageItem GetPackage(string userName, PackageState packageState, PackageName packageName)
        {
            return new AwsPackageStorageItem(this, packageName, packageState, _table, _bucket, _s3Config, _dynamoConfig, userName);
        }

        public IPackageRelatedStorageItem GetSymbol(PackageName packageName, SymbolName symbolName)
        {
            throw new NotImplementedException();
        }

        public IPackageRelatedStorageItem GetSource(PackageName packageName, SourceName sourceName)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> Delete()
        {
            //delete table
            var tableExisted = await _table.DeleteIfExistsAsync();

            //delete bucket
            var bucketExsisted = await _bucket.DeleteIfExistsAsync();

            Debug.Assert(tableExisted == bucketExsisted);

            return tableExisted || bucketExsisted;
        }
    }

    public class AwsPackageStorageItem : IPackageStorageItem
    {
        private readonly PackageTable _table;
        private readonly PackageBucket _bucket;
        private readonly AmazonS3Config _s3Config;
        private readonly AmazonDynamoDBConfig _dynamoConfig;
        private readonly string _username;

        public AwsPackageStorageItem(
            IStorageFeed feed,
            PackageName name,
            PackageState state,
            PackageTable table,
            PackageBucket bucket,
            AmazonS3Config s3Config,
            AmazonDynamoDBConfig dynamoConfig,
            string username)
        {
            _table = table;
            _bucket = bucket;
            _s3Config = s3Config;
            _dynamoConfig = dynamoConfig;
            _username = username;

            Feed = feed;
            Name = name;
            State = state;
        }

        public IStorageFeed Feed { get; }

        public bool CanGetUri => false;

        public async Task<bool> Exists()
        {
            if (!await _bucket.Exists()) return false;

            using (var client = new AmazonS3Client(_s3Config))
            {
                var key = GetPath(State, _username, Name);

                var response = await client.ListObjectsAsync(_bucket.Name, key);

                return response.S3Objects.SingleOrDefault(x => x.Key == key) != null;
            }
        }

        public Task<Uri> GetUri()
        {
            throw new NotImplementedException();
        }

        public async Task<Stream> Get()
        {
            using (var client = new AmazonS3Client(_s3Config))
            {
                try
                {
                    var obj = await client.GetObjectAsync(_bucket.Name, GetPath(State, _username, Name));

                    return obj.ResponseStream;
                }
                catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket" || ex.ErrorCode == "NoSuchKey")
                {
                    return null;
                }
            }
        }

        public async Task<Stream> Put()
        {
            await _bucket.CreateIfNotExists();
            await _table.CreateIfNotExists();

            return new WriteS3ObjectOnDisposeStream(_s3Config, _bucket.Name, GetPath(State, _username, Name));
        }

        public Task Get(Stream target)
        {
            throw new NotImplementedException();
        }

        public Task Put(Stream source)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> Delete()
        {
            //delete blob

            if (!await Exists()) return false;

            try
            {
                using (var client = new AmazonS3Client(_s3Config))
                {
                    var response = await client.DeleteObjectAsync(_bucket.Name, GetPath(State, _username, Name));

                    return response.HttpStatusCode == HttpStatusCode.NoContent;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket" || ex.ErrorCode == "NoSuchKey")
            {
                return false;
            }

            //delete dynamo row

            return false;
        }

        public PackageName Name { get; }

        public PackageState State { get; }

        public async Task<string> GetUserName()
        {
            var packageEntity = await _table.RetrieveAsync(State, Name);

            if (packageEntity != null)
            {
                if (_username == null)
                    return packageEntity["username"];

                if (packageEntity["username"] != _username)
                    return null;
            }

            return _username;
        }

        public Task<IPackageStorageItem> Move(PackageState newState, PackageName newName)
        {
            throw new NotImplementedException();
        }

        public async Task<IPackageStorageItem> Copy(PackageState newState, PackageName newName)
        {
            if (!await Exists())
                return null;

            var newItem = await PrepareMoveOrCopy(newState, newName);
            var packageEntity = await _table.RetrieveAsync(State, Name);
            await newItem._table.InsertOrReplaceAsync(newState, newName, packageEntity);

            var username = await GetUserName();

            using (var client = new AmazonS3Client(_s3Config))
            {

                var request = new CopyObjectRequest
                {
                    SourceBucket = this.Feed.Name,
                    SourceKey = GetPath(State, username, Name),
                    DestinationBucket = this.Feed.Name,
                    DestinationKey = GetPath(newState, username, newName)
                };

                CopyObjectResponse response = await client.CopyObjectAsync(request);
            }

            return newItem;
        }

        private static string GetDirectoryPath(PackageState packageState, string userName)
        {
            return string.Format("pkg/{0}/{1}", packageState.ToString().ToLower(), userName);
        }

        private static string GetPackagePath(PackageName packageName)
        {
            return string.Format("{0}/{1}/{0}.{1}.nupkg", packageName.Id, packageName.Version);
        }

        private static string GetPath(PackageState packageState, string userName, PackageName packageName)
        {
            return string.Format("{0}/{1}", GetDirectoryPath(packageState, userName), GetPackagePath(packageName));
        }

        private Task<AwsPackageStorageItem> PrepareMoveOrCopy(PackageState newState, PackageName newName)
        {
            var newPackage = new AwsPackageStorageItem(Feed, newName, newState, _table, _bucket, _s3Config, _dynamoConfig, _username);

            return Task.FromResult(newPackage);
        }
    }

    public class PackageTable
    {
        private readonly AmazonDynamoDBConfig _config;
        public string TableName { get; }

        public PackageTable(AmazonDynamoDBConfig config, string tableName)
        {
            _config = config;
            TableName = tableName;
        }

        public async Task<Document> RetrieveAsync(PackageState packageState, PackageName packageName)
        {
            var partitionKey = GetPartitionKey(packageState);
            var rangeKey = GetRangeKey(packageName);

            using (var client = new AmazonDynamoDBClient(_config))
            {
                var table = Table.LoadTable(client, TableName);
                var document = await table.GetItemAsync(partitionKey, rangeKey);

                return document;
            }
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            using (var client = new AmazonDynamoDBClient(_config))
            {
                try
                {
                    var response = await client.DeleteTableAsync(TableName);

                    return response.HttpStatusCode == HttpStatusCode.OK;
                }
                catch (ResourceNotFoundException)
                {
                    return false;
                }
            }
        }

        private static string GetPartitionKey(PackageState packageState)
        {
            return $"pkg*{packageState.ToString().ToLower()}";
        }

        private static string GetRangeKey(PackageName packageName)
        {
            return $"{packageName.Id}*{packageName.Version}";
        }

        public Task InsertOrReplaceAsync(PackageState newState, PackageName newName, Document packageEntity)
        {
            throw new NotImplementedException();
        }

        public async Task CreateIfNotExists()
        {
            if (await Exists()) return;

            using (var client = new AmazonDynamoDBClient(_config))
            {
                var attributeDefinitions = new List<AttributeDefinition> { new AttributeDefinition { AttributeName = "id", AttributeType = ScalarAttributeType.S } };
                var keySchemaElements = new List<KeySchemaElement> { new KeySchemaElement { AttributeName = "id", KeyType = KeyType.HASH } };

                var request = new CreateTableRequest
                {
                    TableName = TableName,
                    AttributeDefinitions = attributeDefinitions,
                    KeySchema = keySchemaElements,
                    ProvisionedThroughput = new ProvisionedThroughput(5, 1)
                };

                await client.CreateTableAsync(request);

                DescribeTableResponse describeTableResponse = null;

                do
                {
                    describeTableResponse = await client.DescribeTableAsync(TableName);

                } while (describeTableResponse.Table.TableStatus != TableStatus.ACTIVE);
            }
        }

        public async Task<bool> Exists()
        {
            try
            {
                using (var client = new AmazonDynamoDBClient(_config))
                {
                    var response = await client.DescribeTableAsync(TableName);

                    return true;
                }
            }
            catch (ResourceNotFoundException)
            {
                return false;
            }
        }

    }

    public class PackageBucket
    {
        private readonly AmazonS3Config _config;

        public PackageBucket(AmazonS3Config config, string bucketName)
        {
            Name = bucketName;
            _config = config;
        }

        public string Name { get; }

        public async Task<bool> Exists()
        {
            using (var client = new AmazonS3Client(_config))
            {
                var response = await client.ListBucketsAsync();
                var bucket = response
                    .Buckets
                    .SingleOrDefault(b => b.BucketName == Name);

                return bucket != null;
            }
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    var response = await client.DeleteBucketAsync(Name);

                    return response.HttpStatusCode == HttpStatusCode.NoContent;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket")
            {
                return false;
            }
        }

        public async Task CreateIfNotExists()
        {
            using (var client = new AmazonS3Client(_config))
            {
                await client.PutBucketAsync(Name);
            }
        }
    }

    public class WriteS3ObjectOnDisposeStream : MemoryStream
    {
        private readonly AmazonS3Config _s3Config;
        private readonly string _bucketName;
        private readonly string _path;
        private bool _disposed = false;

        public WriteS3ObjectOnDisposeStream(AmazonS3Config s3Config, string bucketName, string path)
        {
            _s3Config = s3Config;
            _bucketName = bucketName;
            _path = path;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                Seek(0, SeekOrigin.Begin);

                using (var client = new AmazonS3Client(_s3Config))
                {
                    var request = new PutObjectRequest
                    {
                        BucketName = _bucketName,
                        AutoCloseStream = false,
                        Key = _path,
                        InputStream = this
                    };

                    var response = client.PutObject(request);

                    _disposed = true;
                }
            }

            base.Dispose(disposing);
        }
    }

}