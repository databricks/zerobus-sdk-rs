using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class IngestionIntegrationTests : IntegrationTestBase
{
    [Test]
    public async Task IngestSingleRecord()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = stream.IngestRecord(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestSingleRecord_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = await stream.IngestRecordAsync(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        await stream.WaitForOffsetAsync(offsetId);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestMultipleRecords()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
            MockResponses.RecordAckResponse(1),
            MockResponses.RecordAckResponse(2),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        const int numRecords = 3;
        for (var i = 0; i < numRecords; i++)
        {
            var testRecord = System.Text.Encoding.UTF8.GetBytes($"test record {i}");
            var offsetId = stream.IngestRecord(testRecord);

            Assert.That(offsetId, Is.EqualTo(i));
        }

        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)numRecords));
        Assert.That(maxOffset, Is.EqualTo(numRecords - 1));
    }

    [Test]
    public async Task IngestMultipleRecords_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_2"),
            MockResponses.RecordAckResponse(0),
            MockResponses.RecordAckResponse(1),
            MockResponses.RecordAckResponse(2),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        const int numRecords = 3;
        for (var i = 0; i < numRecords; i++)
        {
            var testRecord = System.Text.Encoding.UTF8.GetBytes($"test record {i}");
            var offsetId = await stream.IngestRecordAsync(testRecord);

            Assert.That(offsetId, Is.EqualTo(i));
        }

        await stream.WaitForOffsetAsync(numRecords - 1);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)numRecords));
        Assert.That(maxOffset, Is.EqualTo(numRecords - 1));
    }

    [Test]
    public async Task IngestSingleStreamConcurrently()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        const int numRecords = 32;

        var responses = new List<MockResponse>
        {
            MockResponses.CreateStreamResponse("test_stream_concurrent_1"),
        };

        for (var i = 0; i < numRecords; i++)
        {
            responses.Add(MockResponses.RecordAckResponse(i));
        }

        fixture.MockServer.InjectResponses(TestTableName, responses);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var offsets = new long[numRecords];
        var tasks = new Task[numRecords];

        for (var i = 0; i < numRecords; i++)
        {
            var index = i;
            tasks[index] = Task.Run(() =>
            {
                var payload = System.Text.Encoding.UTF8.GetBytes($"concurrent test record {index}");
                offsets[index] = stream.IngestRecord(payload);
            });
        }

        await Task.WhenAll(tasks);

        var sortedOffsets = (long[])offsets.Clone();
        Array.Sort(sortedOffsets);

        for (var i = 0; i < numRecords; i++)
        {
            Assert.That(sortedOffsets[i], Is.EqualTo(i));
        }

        stream.WaitForOffset(numRecords - 1);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)numRecords));
        Assert.That(maxOffset, Is.EqualTo(numRecords - 1));
    }

    [Test]
    public async Task IngestSingleStreamConcurrently_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        const int numRecords = 32;

        var responses = new List<MockResponse>
        {
            MockResponses.CreateStreamResponse("test_stream_concurrent_async_1"),
        };

        for (var i = 0; i < numRecords; i++)
        {
            responses.Add(MockResponses.RecordAckResponse(i));
        }

        fixture.MockServer.InjectResponses(TestTableName, responses);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        var offsets = new long[numRecords];
        var tasks = new Task[numRecords];

        for (var i = 0; i < numRecords; i++)
        {
            var index = i;
            tasks[index] = Task.Run(async () =>
            {
                var payload = System.Text.Encoding.UTF8.GetBytes($"concurrent test record {index}");
                offsets[index] = await stream.IngestRecordAsync(payload);
            });
        }

        await Task.WhenAll(tasks);

        var sortedOffsets = (long[])offsets.Clone();
        Array.Sort(sortedOffsets);

        for (var i = 0; i < numRecords; i++)
        {
            Assert.That(sortedOffsets[i], Is.EqualTo(i));
        }

        await stream.WaitForOffsetAsync(numRecords - 1);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)numRecords));
        Assert.That(maxOffset, Is.EqualTo(numRecords - 1));
    }

    [Test]
    public async Task IngestBatchRecords()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        byte[][] batch =
        [
            "record 1"u8.ToArray(),
            "record 2"u8.ToArray(),
            "record 3"u8.ToArray(),
            "record 4"u8.ToArray(),
            "record 5"u8.ToArray(),
        ];

        var offsetId = stream.IngestRecords(batch);

        Assert.That(offsetId, Is.EqualTo(0));

        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)batch.Length));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestBatchRecords_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_batch"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        byte[][] batch =
        [
            "record 1"u8.ToArray(),
            "record 2"u8.ToArray(),
            "record 3"u8.ToArray(),
            "record 4"u8.ToArray(),
            "record 5"u8.ToArray(),
        ];

        var offsetId = await stream.IngestRecordsAsync(batch);

        Assert.That(offsetId, Is.EqualTo(0));

        await stream.WaitForOffsetAsync(offsetId);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)batch.Length));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestRecordsAfterClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_batch_after_close"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Close();

        byte[][] batch = ["record 1"u8.ToArray(), "record 2"u8.ToArray()];

        Assert.That(() => stream.IngestRecords(batch),
            Throws.InstanceOf<Exception>());
    }

    [Test]
    public async Task IngestRecordsAfterClose_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_batch_after_close_async"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        await stream.CloseAsync();

        byte[][] batch = ["record 1"u8.ToArray(), "record 2"u8.ToArray()];

        Assert.ThrowsAsync<ZerobusException>(async () =>
        {
            await stream.IngestRecordsAsync(batch);
        });
    }
}
