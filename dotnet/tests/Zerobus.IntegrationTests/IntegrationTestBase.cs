using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

public abstract class IntegrationTestBase
{
    protected const string TestTableName = "test_catalog.test_schema.test_table";

    protected static ZerobusSdk CreateDefaultSdk(MockServerFixture fixture)
    {
        return ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();
    }

    protected static TableProperties CreateTableProperties()
    {
        return new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());
    }

    protected static StreamConfigurationOptions CreateDefaultOptions()
    {
        return StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };
    }
}
