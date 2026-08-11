using System.Collections.Concurrent;
using Grpc.Core;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class LifecycleRecoveryIntegrationTests : IntegrationTestBase
{
    [Test]
    public async Task GracefulClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0, delayMs: 100),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

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
    public async Task GracefulClose_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_close"),
            MockResponses.RecordAckResponse(0, delayMs: 100),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = await stream.IngestRecordAsync(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        await stream.CloseAsync();

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task AsyncDispose_GracefullyClosesStream()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_dispose"),
            MockResponses.RecordAckResponse(0, delayMs: 100),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        await using (var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options))
        {
            var testRecord = "test record data"u8.ToArray();
            var offsetId = await stream.IngestRecordAsync(testRecord);

            Assert.That(offsetId, Is.EqualTo(0));
        }

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

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Close();
        stream.Close();
    }

    [Test]
    public async Task IdempotentClose_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_idempotent_close"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        await stream.CloseAsync();
        await stream.CloseAsync();
    }

    [Test]
    public async Task IngestAfterClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        stream.Close();

        Assert.That(() => stream.IngestRecord("test record data"u8.ToArray()),
            Throws.InstanceOf<ZerobusException>());
    }

    [Test]
    public async Task IngestAfterClose_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_after_close"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);
        await stream.CloseAsync();

        Assert.ThrowsAsync<ZerobusException>(async () =>
        {
            await stream.IngestRecordAsync("test record data"u8.ToArray());
        });
    }

    [Test]
    public async Task ConcurrentIngestAndClose_DoesNotProduceUnexpectedExceptions()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        const int workerCount = 12;
        const int recordsPerWorker = 60;
        const int maxRecords = workerCount * recordsPerWorker;

        var responses = new List<MockResponse>
        {
            MockResponses.CreateStreamResponse("test_stream_concurrent_close"),
        };

        for (var i = 0; i < maxRecords; i++)
        {
            responses.Add(MockResponses.RecordAckResponse(i));
        }

        fixture.MockServer.InjectResponses(TestTableName, responses);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var startGate = new ManualResetEventSlim(false);
        var unexpected = new ConcurrentBag<Exception>();

        var workers = Enumerable.Range(0, workerCount)
            .Select(workerId => Task.Run(() =>
            {
                startGate.Wait();

                for (var i = 0; i < recordsPerWorker; i++)
                {
                    try
                    {
                        var payload = System.Text.Encoding.UTF8.GetBytes($"close-race-{workerId}-{i}");
                        _ = stream.IngestRecord(payload);
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (ZerobusException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        unexpected.Add(ex);
                        break;
                    }
                }
            }))
            .ToArray();

        var closer = Task.Run(async () =>
        {
            startGate.Wait();
            await Task.Delay(20);

            try
            {
                stream.Close();
            }
            catch (ZerobusException)
            {
                // Close races with active ingesters by design in this stress test.
            }
        });

        startGate.Set();
        await Task.WhenAll(workers.Concat([closer]));

        Assert.That(unexpected, Is.Empty);
    }

    [Test]
    public async Task ConcurrentIngestAndDispose_DoesNotProduceUnexpectedExceptions()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        const int workerCount = 12;
        const int recordsPerWorker = 60;
        const int maxRecords = workerCount * recordsPerWorker;

        var responses = new List<MockResponse>
        {
            MockResponses.CreateStreamResponse("test_stream_concurrent_dispose"),
        };

        for (var i = 0; i < maxRecords; i++)
        {
            responses.Add(MockResponses.RecordAckResponse(i));
        }

        fixture.MockServer.InjectResponses(TestTableName, responses);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var startGate = new ManualResetEventSlim(false);
        var unexpected = new ConcurrentBag<Exception>();

        var workers = Enumerable.Range(0, workerCount)
            .Select(workerId => Task.Run(() =>
            {
                startGate.Wait();

                for (var i = 0; i < recordsPerWorker; i++)
                {
                    try
                    {
                        var payload = System.Text.Encoding.UTF8.GetBytes($"dispose-race-{workerId}-{i}");
                        _ = stream.IngestRecord(payload);
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (ZerobusException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        unexpected.Add(ex);
                        break;
                    }
                }
            }))
            .ToArray();

        var disposer = Task.Run(async () =>
        {
            startGate.Wait();
            await Task.Delay(20);

            try
            {
                stream.Dispose();
            }
            catch (ZerobusException)
            {
                // Dispose may surface close failure when racing with active ingesters.
            }
        });

        startGate.Set();
        await Task.WhenAll(workers.Concat([disposer]));

        Assert.That(unexpected, Is.Empty);
    }

    [Test]
    public async Task ConcurrentIngestThenCloseAndRecreate_DoesNotProduceUnexpectedExceptions()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        var responses = new List<MockResponse>
        {
            MockResponses.CreateStreamResponse("test_stream_concurrent_recreate_1"),
        };

        for (var i = 0; i < 48; i++)
        {
            responses.Add(MockResponses.RecordAckResponse(i));
        }

        responses.Add(MockResponses.CreateStreamResponse("test_stream_concurrent_recreate_2"));
        responses.Add(MockResponses.RecordAckResponse(0));

        fixture.MockServer.InjectResponses(TestTableName, responses);

        using var sdk = CreateDefaultSdk(fixture);

        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var currentStream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        var sync = new object();
        var startGate = new ManualResetEventSlim(false);
        var unexpected = new ConcurrentBag<Exception>();

        const int workerCount = 4;
        const int iterationsPerWorker = 4;

        var workers = Enumerable.Range(0, workerCount)
            .Select(workerId => Task.Run(() =>
            {
                startGate.Wait();

                for (var i = 0; i < iterationsPerWorker; i++)
                {
                    ZerobusStream target;
                    lock (sync)
                    {
                        target = currentStream;
                    }

                    try
                    {
                        var payload = System.Text.Encoding.UTF8.GetBytes($"recreate-race-{workerId}-{i}");
                        _ = target.IngestRecord(payload);
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (ZerobusException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        unexpected.Add(ex);
                        break;
                    }
                }
            }))
            .ToArray();

        var recreater = Task.Run(async () =>
        {
            startGate.Wait();
            await Task.Delay(5);

            ZerobusStream oldStream;
            lock (sync)
            {
                oldStream = currentStream;
            }

            try
            {
                oldStream.Close();
            }
            catch (ZerobusException)
            {
                // Close can race with active ingest; stream recreation should still be valid.
            }

            using var recreated = sdk.RecreateStream(oldStream);
            var recreatedOffset = recreated.IngestRecord("post-recreate"u8.ToArray());
            recreated.WaitForOffset(recreatedOffset);
        });

        startGate.Set();
        await Task.WhenAll(workers.Concat([recreater]));

        Assert.That(unexpected, Is.Empty);
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

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

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
    public async Task RecreateStream_ClosedStream_RecreatedStreamUsable_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_async_recreate_1"),
            MockResponses.CreateStreamResponse("test_stream_async_recreate_2"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);
        await stream.CloseAsync();

        var unacked = await stream.GetUnackedRecordsAsync();
        Assert.That(unacked, Is.Empty);

        await using var recreatedStream = await sdk.RecreateStreamAsync(stream);

        Assert.That(recreatedStream, Is.Not.Null);
        Assert.That(recreatedStream, Is.Not.SameAs(stream));

        Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await stream.IngestRecordAsync("old stream"u8.ToArray());
        });

        var offsetId = await recreatedStream.IngestRecordAsync("recreated stream"u8.ToArray());
        Assert.That(offsetId, Is.EqualTo(0));

        await recreatedStream.WaitForOffsetAsync(offsetId);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task RecreateStream_JsonTypedStream_RecreatedStreamUsable()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_json_1"),
            MockResponses.CreateStreamResponse("test_stream_json_2"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions();

        var stream = sdk.CreateJsonStreamWithHeadersProvider(
            TestTableName,
            new TestHeadersProvider(),
            options);
        stream.Close();

        using var recreatedStream = sdk.RecreateStream(stream);

        var offsetId = recreatedStream.IngestRecord("{\"message\":\"recreated\"}");
        recreatedStream.WaitForOffset(offsetId);

        Assert.That(offsetId, Is.EqualTo(0));
    }

    [Test]
    public async Task RecreateStream_JsonTypedStream_RecreatedStreamUsable_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_json_async_1"),
            MockResponses.CreateStreamResponse("test_stream_json_async_2"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var options = CreateDefaultOptions();

        var stream = await sdk.CreateJsonStreamWithHeadersProviderAsync(
            TestTableName,
            new TestHeadersProvider(),
            options);
        await stream.CloseAsync();

        await using var recreatedStream = await sdk.RecreateStreamAsync(stream);

        var offsetId = await recreatedStream.IngestRecordAsync("{\"message\":\"recreated\"}");
        await recreatedStream.WaitForOffsetAsync(offsetId);

        Assert.That(offsetId, Is.EqualTo(0));
    }

    [Test]
    public async Task FlushFailure_GetUnackedRecords_ThenRecreateStream_Works()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_flush_failure_1"),
            MockResponses.ErrorResponse(StatusCode.Unavailable, "transient ack failure"),
            MockResponses.CreateStreamResponse("test_stream_flush_failure_2"),
            MockResponses.RecordAckResponse(1),
        ]);

        using var sdk = CreateDefaultSdk(fixture);

        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var failedPayload = "failed-before-flush"u8.ToArray();
        _ = stream.IngestRecord(failedPayload);

        Assert.That(() => stream.Flush(), Throws.InstanceOf<ZerobusException>());

        var unacked = stream.GetUnackedRecords();
        Assert.That(unacked, Has.Length.EqualTo(1));
        Assert.That(unacked[0].ToArray(), Is.EqualTo(failedPayload));

        using var recreated = sdk.RecreateStream(stream);

        var recoveredOffset = recreated.IngestRecord("recovered-record"u8.ToArray());
        Assert.That(recoveredOffset, Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public async Task RecreateStream_RetryableFailure_ThrowsRetryableException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_recreate_retryable_1"),
            MockResponses.ErrorResponse(StatusCode.Unavailable, "recreate unavailable"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);

        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        stream.Close();

        var ex = Assert.Throws<ZerobusException>(() => sdk.RecreateStream(stream));
        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.IsRetryable, Is.True);
    }

    [Test]
    public async Task RecreateStream_RetryableFailure_ThrowsRetryableException_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_recreate_retryable_async_1"),
            MockResponses.ErrorResponse(StatusCode.Unavailable, "recreate unavailable"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);

        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        await using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);
        await stream.CloseAsync();

        var ex = Assert.ThrowsAsync<ZerobusException>(async () => await sdk.RecreateStreamAsync(stream));
        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.IsRetryable, Is.True);
    }

    [Test]
    public async Task RecreateStream_ActiveStream_ThrowsZerobusException()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var ex = Assert.Throws<ZerobusException>(() => sdk.RecreateStream(stream));
        Assert.That(ex!.Message, Does.Contain("active stream"));
    }

    [Test]
    public async Task RecreateStream_ActiveStream_ThrowsZerobusException_AsyncApi()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_active_async_1"),
        ]);

        using var sdk = CreateDefaultSdk(fixture);
        var tableProps = CreateTableProperties();
        var options = CreateDefaultOptions();

        await using var stream = await sdk.CreateStreamWithHeadersProviderAsync(tableProps, new TestHeadersProvider(), options);

        var ex = Assert.ThrowsAsync<ZerobusException>(async () => await sdk.RecreateStreamAsync(stream));
        Assert.That(ex!.Message, Does.Contain("active stream"));
    }
}
