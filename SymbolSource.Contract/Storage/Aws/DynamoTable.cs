using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Microsoft.WindowsAzure.Storage;

namespace SymbolSource.Contract.Storage.Aws
{
    public class DynamoTable
    {
        protected const string StateKey = "state";
        protected const string IdKey = "id";

        //TODO: ideally this should be private
        protected readonly AmazonDynamoDBConfig _config;
        public string TableName { get; }

        public DynamoTable(AmazonDynamoDBConfig config, string tableName)
        {
            _config = config;
            TableName = tableName;
        }

        protected async Task<Document> RetrieveAsync(string partitionKey, string rangeKey)
        {
            try
            {
                using (var client = new AmazonDynamoDBClient(_config))
                {
                    var table = Table.LoadTable(client, TableName);
                    var document = await table.GetItemAsync(partitionKey, rangeKey);

                    return document;
                }
            }
            catch (ResourceNotFoundException)
            {
                return null;
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

        public async Task InsertOrReplaceAsync(Document document)
        {
            using (var client = new AmazonDynamoDBClient(_config))
            {
                var table = Table.LoadTable(client, TableName);

                await table.UpdateItemAsync(document);
            }
        }

        public async Task CreateIfNotExists()
        {
            if (await Exists()) return;

            using (var client = new AmazonDynamoDBClient(_config))
            {
                var attributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition {AttributeName = StateKey, AttributeType = ScalarAttributeType.S},
                    new AttributeDefinition {AttributeName = IdKey, AttributeType = ScalarAttributeType.S}
                };
                var keySchemaElements = new List<KeySchemaElement>
                {
                    new KeySchemaElement {AttributeName = StateKey, KeyType = KeyType.HASH},
                    new KeySchemaElement {AttributeName = IdKey, KeyType = KeyType.RANGE}
                };

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
                    await Task.Delay(200);
                } while (describeTableResponse.Table.TableStatus != TableStatus.ACTIVE);
            }
        }

        public async Task<bool> Exists()
        {
            using (var client = new AmazonDynamoDBClient(_config))
            {
                var tableResponse = await client.ListTablesAsync();

                return tableResponse.TableNames.Exists(name => string.Equals(name, TableName));
            }
        }

        protected async Task<bool> DeleteRowIfExists(string partitionKey, string rangeKey)
        {
            try
            {
                using (var client = new AmazonDynamoDBClient(_config))
                {
                    var request = new DeleteItemRequest
                    {
                        ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.SIZE,
                        TableName = TableName,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {StateKey, new AttributeValue(partitionKey)},
                            {IdKey, new AttributeValue(rangeKey)}
                        },
                        ReturnValues = ReturnValue.ALL_OLD
                    };

                    var response = await client.DeleteItemAsync(request);

                    return response.Attributes.Count > 0;
                }
            }
            catch (ResourceNotFoundException)
            {
                return false;
            }
        }

        protected IEnumerable<Document> Query(string partitionKey)
        {
            using (var client = new AmazonDynamoDBClient(_config))
            {
                var table = Table.LoadTable(client, TableName);

                //TODO: empty query filter here, can we pass something in to make it a bit smarter?
                var search = table.Query(partitionKey, new QueryFilter());

                do
                {
                    foreach (var document in search.GetNextSet())
                    {
                        yield return document;
                    }
                } while (!search.IsDone);
            }
        }
    }
}