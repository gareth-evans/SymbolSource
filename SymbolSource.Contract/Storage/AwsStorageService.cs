using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;

namespace SymbolSource.Contract.Storage.Aws
{
    public class AwsStorageService : IStorageService
    {
        private const string FeedPrefix = "feed-";
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

            return new AwsStorageFeed(
                bucketName,
                packageTable,
                s3Config,
                dynamoConfig);
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
        private readonly PackageBucket _packageBucket;
        private readonly AmazonS3Config _s3Config;
        private readonly AmazonDynamoDBConfig _dynamoConfig;
        private SourceBucket _sourceBucket;
        private SymbolBucket _symbolBucket;

        public AwsStorageFeed(
            string feedName,
            PackageTable table,
            AmazonS3Config s3Config,
            AmazonDynamoDBConfig dynamoConfig)
        {
            _table = table;
            _s3Config = s3Config;
            _dynamoConfig = dynamoConfig;

            Name = feedName;
            _packageBucket = new PackageBucket(s3Config, feedName);
            _sourceBucket = new SourceBucket(s3Config, feedName);
            _symbolBucket = new SymbolBucket(s3Config, feedName);
        }

        public string Name { get; }

        public IEnumerable<string> QueryInternals()
        {
            using (var client = new AmazonS3Client(_s3Config))
            {
                var objects = client.ListObjects(_packageBucket.Name);

                return objects.S3Objects.Select(o => string.Join("/", o.Key.Split('/').Skip(2)));
            }
        }

        public Task<IEnumerable<PackageName>> QueryPackages(PackageState packageState)
        {
            return _table.Query(packageState);
        }

        public Task<IEnumerable<PackageName>> QueryPackages(string userName, PackageState packageState)
        {
            return _table.Query(userName, packageState);
        }

        public Task<IEnumerable<PackageName>> QueryPackages(PackageState packageState, string packageNamePrefix,
            int skip, int take)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<PackageName>> QueryPackages(string userName, PackageState packageState,
            string packageNamePrefix, int skip, int take)
        {
            throw new NotImplementedException();
        }

        public IPackageStorageItem GetPackage(string userName, PackageState packageState, PackageName packageName)
        {
            return new AwsPackageStorageItem(this, packageName, packageState, _table, _packageBucket, _s3Config,
                _dynamoConfig,
                userName);
        }

        public IPackageRelatedStorageItem GetSymbol(PackageName packageName, SymbolName symbolName)
        {
            return new AwsPackageRelatedStorageItem(
                this,
                packageName,
                new SymbolRelatedPackageTable(_dynamoConfig, _table.TableName, symbolName),
                _symbolBucket.GetObjectReference(symbolName)               
                );
        }

        public IPackageRelatedStorageItem GetSource(PackageName packageName, SourceName sourceName)
        {
            return new AwsPackageRelatedStorageItem(
                this,
                packageName,
                new SourceRelatedPackageTable(_dynamoConfig, _table.TableName, sourceName), 
                _sourceBucket.GetObjectReference(sourceName));
        }

        public async Task<bool> Delete()
        {
            //delete table
            var tableExisted = await _table.DeleteIfExistsAsync();

            //delete bucket
            var bucketExsisted = await _packageBucket.DeleteIfExistsAsync();

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
        private readonly string _userName;

        public AwsPackageStorageItem(
            IStorageFeed feed,
            PackageName name,
            PackageState state,
            PackageTable table,
            PackageBucket bucket,
            AmazonS3Config s3Config,
            AmazonDynamoDBConfig dynamoConfig,
            string userName)
        {
            _table = table;
            _bucket = bucket;
            _s3Config = s3Config;
            _dynamoConfig = dynamoConfig;
            _userName = userName;

            Feed = feed;
            Name = name;
            State = state;
        }

        public IStorageFeed Feed { get; }

        public bool CanGetUri => false;

        public async Task<bool> Exists()
        {
            //TODO: we're just checking the bucket exists here, what about the dynamo row? How does the Azure version do it?
            if (!await _bucket.Exists()) return false;

            using (var client = new AmazonS3Client(_s3Config))
            {
                var key = GetPath(State, await GetUserName(), Name);

                var response = await client.ListObjectsAsync(_bucket.Name, key);

                return response.S3Objects.SingleOrDefault(x => x.Key == key) != null;
            }
        }

        public Task<Uri> GetUri()
        {
            throw new NotSupportedException();
        }

        public async Task<Stream> Get()
        {
            using (var client = new AmazonS3Client(_s3Config))
            {
                try
                {
                    var obj = await client.GetObjectAsync(_bucket.Name, GetPath(State, await GetUserName(), Name));

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
            await PreparePut();

            return new WriteS3ObjectOnDisposeStream(_s3Config, _bucket.Name, GetPath(State, _userName, Name));
        }

        private async Task PreparePut()
        {
            if (_userName == null)
                throw new InvalidOperationException();

            var document = await _table.RetrieveAsync(State, Name);

            if (document != null)
            {
                if (document["UserName"] != _userName)
                {
                    await _bucket.DeleteObjectIfExistsAsync(GetPath(State, document["UserName"], Name));
                    document["UserName"] = _userName;
                }
            }
            else
            {
                document = new Document { ["UserName"] = _userName };
            }

            await _bucket.CreateIfNotExists();
            await _table.CreateIfNotExists();

            await _table.InsertOrReplaceAsync(State, Name, document);
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
            var objectDeleted = await _bucket.DeleteObjectIfExistsAsync(GetPath(State, await GetUserName(), Name));
            var rowDeleted = await _table.DeleteRowIfExists(State, Name);

            return objectDeleted || rowDeleted;
        }

        public PackageName Name { get; }

        public PackageState State { get; }

        public async Task<string> GetUserName()
        {
            var packageEntity = await _table.RetrieveAsync(State, Name);

            if (packageEntity != null)
            {
                if (_userName == null)
                    return packageEntity["UserName"];

                if (packageEntity["UserName"] != _userName)
                    return null;
            }

            return _userName;
        }

        public async Task<IPackageStorageItem> Move(PackageState newState, PackageName newName)
        {
            if (!await Exists())
                return null;

            var newItem = await PrepareMoveOrCopy(newState, newName);
            var packageEntity = await _table.RetrieveAsync(State, Name);

            await _table.InsertOrReplaceAsync(newState, newName, packageEntity);
            await _table.DeleteRowIfExists(State, Name);

            var username = await GetUserName();

            var sourceKey = GetPath(State, username, Name);
            var destinationKey = GetPath(newState, username, newName);

            await _bucket.MoveObjectAsync(sourceKey, destinationKey);

            return newItem;
        }

        public async Task<IPackageStorageItem> Copy(PackageState newState, PackageName newName)
        {
            if (!await Exists())
                return null;

            var newItem = await PrepareMoveOrCopy(newState, newName);
            var packageEntity = await _table.RetrieveAsync(State, Name);
            await newItem._table.InsertOrReplaceAsync(newState, newName, packageEntity);

            var username = await GetUserName();
            var sourceKey = GetPath(State, username, Name);
            var destinationKey = GetPath(newState, username, newName);

            await _bucket.CopyObjectAsync(sourceKey, destinationKey);

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
            var newPackage = new AwsPackageStorageItem(Feed, newName, newState, _table, _bucket, _s3Config,
                _dynamoConfig, _userName);

            return Task.FromResult(newPackage);
        }
    }

    public class S3ObjectReference
    {
        private readonly AmazonS3Config _config;
        private readonly string _bucketName;
        private readonly string _path;

        public S3ObjectReference(AmazonS3Config config, string bucketName, string path)
        {
            _config = config;
            _bucketName = bucketName;
            _path = path;
        }

        public async Task<bool> Exists()
        {
            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    var response = await client.GetObjectMetadataAsync(_bucketName, _path);
                    return response.HttpStatusCode == HttpStatusCode.OK;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NotFound" || ex.ErrorCode == "NoSuchBucket")
            {
                return false;
            }
        }

        public async Task<Stream> OpenReadAsync()
        {
            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    var response = await client.GetObjectAsync(_bucketName, _path);

                    return response.ResponseStream;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket" || ex.ErrorCode == "NoSuchKey")
            {
                return null;
            }
        }

        public async Task<bool> DeleteIfExists()
        {
            try
            {
                if (!await Exists()) return false;

                using (var client = new AmazonS3Client(_config))
                {
                    var response = await client.DeleteObjectAsync(_bucketName, _path);

                    return response.HttpStatusCode == HttpStatusCode.NoContent;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NotFound" || ex.ErrorCode == "NoSuchBucket")
            {
                return false;
            }
        }

        public Task<Stream> OpenWriteAsync()
        {
            Stream stream = new WriteS3ObjectOnDisposeStream(_config, _bucketName, _path);
            return Task.FromResult(stream);
        }

        public async Task CreateBucketIfNotExistsAsync()
        {
            using (var client = new AmazonS3Client(_config))
            {
                if(AmazonS3Util.DoesS3BucketExist(client, _bucketName)) return;

                await client.PutBucketAsync(_bucketName);

                while (!AmazonS3Util.DoesS3BucketExist(client, _bucketName))
                {
                    await Task.Delay(100);
                }
            }
        }
    }

    public class AwsPackageRelatedStorageItem : IPackageRelatedStorageItem
    {
        private readonly AwsStorageFeed _feed;
        private readonly PackageName _packageName;
        private readonly RelatedPackageTable _relatedPackageTable;
        private readonly S3ObjectReference _objectReference;

        public AwsPackageRelatedStorageItem(
            AwsStorageFeed feed,
            PackageName packageName,
            RelatedPackageTable relatedPackageTable,
            S3ObjectReference objectReference)
        {
            _feed = feed;
            _packageName = packageName;
            _relatedPackageTable = relatedPackageTable;
            _objectReference = objectReference;
        }

        public IStorageFeed Feed
        {
            get { return _feed; }
        }

        public bool CanGetUri
        {
            get { return false; }
        }

        public Task<bool> Exists()
        {
            return _objectReference.Exists();
        }

        public Task<Uri> GetUri()
        {
            throw new NotSupportedException();
        }

        public Task<Stream> Get()
        {
            return _objectReference.OpenReadAsync();
        }

        public async Task<Stream> Put()
        {
            await PreparePut();
            return await _objectReference.OpenWriteAsync();
        }

        private async Task PreparePut()
        {
            if (_packageName == null)
                throw new InvalidOperationException();

            await _objectReference.CreateBucketIfNotExistsAsync();

            await PackageNames.Add(_packageName);
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
            if (_packageName == null)
                throw new InvalidOperationException();

            await PackageNames.Remove(_packageName);

            if (!(await PackageNames.List()).Any())
                return await _objectReference.DeleteIfExists();

            return false;
        }

        public IStorageSet<PackageName> PackageNames
        {
            get
            {
                return new AwsRelatedPackageNameSet(_relatedPackageTable);
            }
        }
    }

    public class AwsRelatedPackageNameSet : IStorageSet<PackageName>
    {
        private readonly RelatedPackageTable _table;

        public AwsRelatedPackageNameSet(RelatedPackageTable table)
        {
            _table = table;
        }

        public async Task Add(PackageName item)
        {
            await _table.CreateIfNotExists();
            await _table.InsertOrReplaceAsync(item);
        }

        public Task Remove(PackageName item)
        {
            return _table.DeleteIfExistsAsync(item);
        }

        public Task<IEnumerable<PackageName>> List()
        {
            return _table.Query();
        }
    }

    public class PackageTable : DynamoTable
    {
        public PackageTable(
            AmazonDynamoDBConfig config, 
            string tableName) : base(config, tableName)
        {
        }

        public Task<Document> RetrieveAsync(PackageState packageState, PackageName packageName)
        {
            var partitionKey = GetPartitionKey(packageState);
            var rangeKey = GetRangeKey(packageName);

            return RetrieveAsync(partitionKey, rangeKey);
        }

        protected static string GetPartitionKey(PackageState packageState)
        {
            return $"pkg*{packageState.ToString().ToLower()}";
        }

        protected static string GetRangeKey(PackageName packageName)
        {
            return $"{packageName.Id}*{packageName.Version}";
        }

        private static PackageName GetPackageName(string rangeKey)
        {
            var rowKeyParts = rangeKey.Split('*');
            return new PackageName(rowKeyParts[0], rowKeyParts[1]);
        }

        public Task InsertOrReplaceAsync(PackageState newState, PackageName newName, Document packageEntity)
        {
            packageEntity[StateKey] = GetPartitionKey(newState);
            packageEntity[IdKey] = GetRangeKey(newName);

            return InsertOrReplaceAsync(packageEntity);
        }

        public async Task<IEnumerable<PackageName>> Query(PackageState packageState)
        {
            using (var client = new AmazonDynamoDBClient(_config))
            {
                var partitionKey = GetPartitionKey(packageState);

                var table = Table.LoadTable(client, TableName);

                var search = table.Query(partitionKey, new QueryFilter());

                //TODO: need to handle paging
                var set = await search.GetNextSetAsync();

                var packageNames = set.Select(d => GetPackageName(d[IdKey]));

                return packageNames;
            }
        }

        public async Task<IEnumerable<PackageName>> Query(string userName, PackageState packageState)
        {
            using (var client = new AmazonDynamoDBClient(_config))
            {
                var partitionKey = GetPartitionKey(packageState);

                var table = Table.LoadTable(client, TableName);

                var search = table.Query(partitionKey, new QueryFilter("UserName", QueryOperator.Equal, userName));

                //TODO: need to handle paging
                var set = await search.GetNextSetAsync();

                var packageNames = set.Select(d => GetPackageName(d[IdKey]));

                return packageNames;
            }
        }

        public Task<bool> DeleteRowIfExists(PackageState state, PackageName name)
        {
            var partitionKey = GetPartitionKey(state);
            var rangeKey = GetRangeKey(name);

            return DeleteRowIfExists(partitionKey, rangeKey);
        }
    }

    public abstract class RelatedPackageTable : DynamoTable
    {
        private readonly string _partitionKey;

        protected RelatedPackageTable(
            AmazonDynamoDBConfig config, 
            string tableName,
            string partitionKey) : base(config, tableName)
        {
            _partitionKey = partitionKey;
        }

        public static string GetRangeKey(PackageName packageName)
        {
            return $"{packageName.Id}*{packageName.Version}";
        }

        public Task InsertOrReplaceAsync(PackageName name)
        {
            var packageEntity = new Document
            {
                [StateKey] = _partitionKey,
                [IdKey] = GetRangeKey(name)
            };

            return base.InsertOrReplaceAsync(packageEntity);
        }

        public Task<IEnumerable<PackageName>> Query()
        {
            var result = Query(_partitionKey).Select(e => GetPackageName(e[IdKey]));

            return Task.FromResult(result);
        }

        internal static PackageName GetPackageName(string rowKey)
        {
            var rowKeyParts = rowKey.Split('*');
            return new PackageName(rowKeyParts[0], rowKeyParts[1]);
        }

        internal Task DeleteIfExistsAsync(PackageName item)
        {
            return DeleteRowIfExists(_partitionKey, GetRangeKey(item));
        }
    }

    public class SymbolRelatedPackageTable : RelatedPackageTable
    {
        public SymbolRelatedPackageTable(AmazonDynamoDBConfig config, string tableName, SymbolName symbolName)
            : base(config, tableName, GetPartitionKey(symbolName))
        {
        }

        private static string GetPartitionKey(SymbolName symbolName)
        {
            return $"pdb*{symbolName.ImageName}*{symbolName.SymbolHash}";
        }
    }

    public class SourceRelatedPackageTable : RelatedPackageTable
    {
        public SourceRelatedPackageTable(AmazonDynamoDBConfig config, string tableName, SourceName sourceName) 
            : base(config, tableName, GetPartitionKey(sourceName))
        {
        }

        private static string GetPartitionKey(SourceName sourceName)
        {
            return $"src*{sourceName.FileName}*{sourceName.Hash}";
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
                return await AmazonS3Util.DoesS3BucketExistAsync(client, Name);
            }
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    await AmazonS3Util.DeleteS3BucketWithObjectsAsync(client, Name);

                    return true;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket")
            {
                return false;
            }
        }

        public async Task CreateIfNotExists()
        {
            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    await client.PutBucketAsync(Name);

                    while (!AmazonS3Util.DoesS3BucketExist(client, Name))
                    {
                        await Task.Delay(100);
                    }
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "BucketAlreadyOwnedByYou")
            {
            }
        }

        public async Task<bool> DeleteObjectIfExistsAsync(string path)
        {
            if (!await Exists()) return false;
            if (!await ObjectExists(Name, path)) return false;

            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    var response = await client.DeleteObjectAsync(Name, path);

                    return response.HttpStatusCode == HttpStatusCode.NoContent;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchBucket" || ex.ErrorCode == "NoSuchKey")
            {
                return false;
            }
        }

        private async Task<bool> ObjectExists(string bucketName, string key)
        {
            try
            {
                using (var client = new AmazonS3Client(_config))
                {
                    var response = await client.GetObjectMetadataAsync(bucketName, key);

                    return response.HttpStatusCode == HttpStatusCode.OK;
                }
            }
            catch (AmazonS3Exception ex) when (ex.ErrorCode == "NotFound")
            {
                return false;
            }
        }

        public async Task<bool> CopyObjectAsync(string sourceKey, string destinationKey)
        {
            using (var client = new AmazonS3Client(_config))
            {
                var request = new CopyObjectRequest
                {
                    SourceBucket = Name,
                    SourceKey = sourceKey,
                    DestinationBucket = Name,
                    DestinationKey = destinationKey
                };

                var response = await client.CopyObjectAsync(request);

                return response.HttpStatusCode == HttpStatusCode.OK;
            }
        }

        public async Task<bool> MoveObjectAsync(string sourceKey, string destinationKey)
        {
            var copySuccess = await CopyObjectAsync(sourceKey, destinationKey);
            var deleteSuccess = false;

            if (copySuccess)
            {
                deleteSuccess = await DeleteObjectIfExistsAsync(sourceKey);
            }

            return copySuccess && deleteSuccess;
        }
    }

    public abstract class Bucket<T>
    {
        private readonly string _bucketName;
        private readonly AmazonS3Config _config;

        protected Bucket(AmazonS3Config config, string bucketName)
        {
            _bucketName = bucketName;
            _config = config;
        }

        protected abstract string GetPath(T sourceName);

        public S3ObjectReference GetObjectReference(T name)
        {
            return new S3ObjectReference(_config, _bucketName, GetPath(name));
        }

        public async Task CreateIfNotExistsAsync()
        {
            using (var client = new AmazonS3Client(_config))
            {
                try
                {
                    await client.PutBucketAsync(_bucketName);

                    while (!AmazonS3Util.DoesS3BucketExist(client, _bucketName))
                    {
                        await Task.Delay(100);
                    }
                }
                catch (AmazonS3Exception ex) when (ex.ErrorCode == "BucketAlreadyOwnedByYou")
                {
                }
            }
        }
    }

    public class SourceBucket : Bucket<SourceName>
    {
        public SourceBucket(AmazonS3Config config, string bucketName) : base(config, bucketName)
        {
        }

        protected override string GetPath(SourceName sourceName)
        {
            return $"src/{sourceName.FileName}/{sourceName.Hash}";
        }
    }

    public class SymbolBucket : Bucket<SymbolName>
    {
        public SymbolBucket(AmazonS3Config config, string bucketName) : base(config, bucketName)
        {
        }

        protected override string GetPath(SymbolName symbolName)
        {
            return $"pdb/{symbolName.ImageName}/{symbolName.SymbolHash}";
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