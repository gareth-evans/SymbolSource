using SymbolSource.Contract.Storage;
using SymbolSource.Contract.Storage.Aws;

namespace SymbolSource.Contract.Tests
{
    public class AwsStorageServiceTests : StorageServiceTests
    {
        public AwsStorageServiceTests()
        {
            Storage = new AwsStorageService();
        }

        protected override IStorageService Storage { get; }
    }


}