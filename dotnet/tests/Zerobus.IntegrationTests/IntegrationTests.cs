using System.Diagnostics;
using Grpc.Core;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class IntegrationTests
{
    private const string TestTableName = "test_catalog.test_schema.test_table";

    // ── Parameter Validation ─────────────────────────────────────────

    [Test]
    public async Task CreateStream_NullTableProperties_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        Assert.Throws<ArgumentNullException>(() =>
        {
            sdk.CreateStream(null!, "id", "secret");
        });
    }

    [Test]
    public async Task CreateStream_NullClientId_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        Assert.Throws<ArgumentNullException>(() =>
        {
            sdk.CreateStream(new TableProperties("test_table"), null!, "secret");
        });
    }

    [Test]
    public async Task CreateStream_NullClientSecret_ThrowsArgumentNullException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        Assert.Throws<ArgumentNullException>(() =>
        {
            sdk.CreateStream(new TableProperties("test_table"), "id", null!);
        });
    }

    // ── Stream Creation ───────────────────────────────────────────────

    [Test]
    public async Task SuccessfulStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        Assert.That(stream, Is.Not.Null);
    }

    [Test]
    public async Task TimeoutedStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1", delayMs: 300),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            RecoveryTimeoutMs = 100,
            Recovery = false,
        };

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });

        // Give background tasks a moment to realize the timeout occurred.
        await Task.Delay(100);
    }

    [Test]
    public async Task NonRetriableErrorDuringStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unauthenticated, "Non-retriable error"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = true,
        };

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });
    }

    [Test]
    public async Task RetriableErrorWithoutRecoveryDuringStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unavailable, "Retriable error"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecoveryTimeoutMs = 100,
            RecoveryBackoffMs = 100,
        };

        var sw = Stopwatch.StartNew();

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });

        sw.Stop();

        // Should fail reasonably quickly without retry. Allow buffer for test environment variability.
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(1000),
            $"Expected reasonable failure time, but took {sw.ElapsedMilliseconds}ms");
    }

    [Test]
    public async Task Builder_CreatesWorkingSdk()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_builder"),
        ]);

        // Builder path: UnityCatalogUrl omitted because we use a custom headers provider.
        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .ApplicationName("integration-test")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        using var stream = sdk.CreateStreamWithHeadersProvider(
            tableProps,
            new TestHeadersProvider(),
            StreamConfigurationOptions.Default with { Recovery = false });

        Assert.That(stream, Is.Not.Null);
    }

    // ── Close ─────────────────────────────────────────────────────────

    [Test]
    public async Task GracefulClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0, delayMs: 100),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = stream.IngestRecord(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        stream.Close();

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IdempotentClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        // First close should succeed.
        stream.Close();

        // Second close should also succeed (idempotent).
        stream.Close();
    }

    [Test]
    public async Task IngestAfterClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Close();

        // Ingesting after close should throw from the native closed-stream state.
        Assert.That(() => stream.IngestRecord("test record data"u8.ToArray()),
            Throws.InstanceOf<ZerobusException>());
    }

    // ── Single Record Ingestion ───────────────────────────────────────

    [Test]
    public async Task IngestSingleRecord()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for offset 0
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = stream.IngestRecord(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        // Give the server time to process.
        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    // ── Multiple Record Ingestion ─────────────────────────────────────

    [Test]
    public async Task IngestMultipleRecords()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for offset 0
            MockResponses.RecordAckResponse(1), // Ack for offset 1
            MockResponses.RecordAckResponse(2), // Ack for offset 2
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        const int numRecords = 3;
        for (var i = 0; i < numRecords; i++)
        {
            var testRecord = System.Text.Encoding.UTF8.GetBytes($"test record {i}");
            var offsetId = stream.IngestRecord(testRecord);

            Assert.That(offsetId, Is.EqualTo(i));
        }

        // Give the server time to process.
        await Task.Delay(100);

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

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

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

    // ── Batch Record Ingestion ────────────────────────────────────────

    [Test]
    public async Task IngestBatchRecords()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for the batch at offset 0
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

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

        // Give the server time to process.
        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)batch.Length));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    // ── Batch Ingest After Close ──────────────────────────────────────

    [Test]
    public async Task IngestRecordsAfterClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_batch_after_close"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Close();

        byte[][] batch = ["record 1"u8.ToArray(), "record 2"u8.ToArray()];

        Assert.That(() => stream.IngestRecords(batch),
            Throws.InstanceOf<Exception>());
    }

    [Test]
    public async Task RecreateStream_ClosedStream_RecreatedStreamUsable()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.CreateStreamResponse("test_stream_2"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        stream.Close();

        var unacked = stream.GetUnackedRecords();
        Assert.That(unacked, Is.Empty);

        using var recreatedStream = sdk.RecreateStream(stream);

        Assert.That(recreatedStream, Is.Not.Null);
        Assert.That(recreatedStream, Is.Not.SameAs(stream));

        Assert.That(() => stream.IngestRecord("old stream"u8.ToArray()),
            Throws.InstanceOf<ObjectDisposedException>());

        var offsetId = recreatedStream.IngestRecord("recreated stream"u8.ToArray());
        Assert.That(offsetId, Is.EqualTo(0));

        recreatedStream.WaitForOffset(offsetId);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task RecreateStream_ActiveStream_ThrowsZerobusException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint(fixture.ServerUrl)
            .UnityCatalogUrl("https://mock-uc.com")
            .DisableTls()
            .Build();

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var ex = Assert.Throws<ZerobusException>(() => sdk.RecreateStream(stream));
        Assert.That(ex!.Message, Does.Contain("active stream"));
    }

}
