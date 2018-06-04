using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;

namespace SymbolSource.Contract.Storage
{
    public class AwsStorageService : IStorageService
    {
        private const string FeedPrefix = "feed-";
        private const string NamedFeedPrefix = "named-";
        private const string DefaultFeedName = "default";

        private static readonly RegionEndpoint BucketRegion = RegionEndpoint.EUWest1;
        private static IAmazonS3 _s3Client;

        public AwsStorageService()
        {
            _s3Client = new AmazonS3Client(BucketRegion);
        }

        public IStorageFeed GetFeed(string feedName)
        {
            var bucketName = GetBucketName(feedName);

            _s3Client.
            throw new System.NotImplementedException();
        }

        public IEnumerable<string> QueryFeeds()
        {
            throw new System.NotImplementedException();
        }

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
            return feedName;
        }
    }

    public class AwsStorageFeed : IStorageFeed
    {
        public AwsStorageFeed(string feedName)
        {
            Name = feedName;
        }

        public string Name { get; }

        public IEnumerable<string> QueryInternals()
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<PackageName>> QueryPackages(PackageState packageState)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<PackageName>> QueryPackages(string userName, PackageState packageState)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public IPackageRelatedStorageItem GetSymbol(PackageName packageName, SymbolName symbolName)
        {
            throw new NotImplementedException();
        }

        public IPackageRelatedStorageItem GetSource(PackageName packageName, SourceName sourceName)
        {
            throw new NotImplementedException();
        }

        public Task<bool> Delete()
        {
            throw new NotImplementedException();
        }
    }
}