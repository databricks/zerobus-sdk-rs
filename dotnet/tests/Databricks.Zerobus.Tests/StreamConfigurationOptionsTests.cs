using Xunit;

namespace Databricks.Zerobus.Tests;

public class StreamConfigurationOptionsTests
{
    [Fact]
    public void Default_ReturnsOptionsWithSensibleDefaults()
    {
        var opts = StreamConfigurationOptions.Default;

        Assert.Equal(1_000_000, opts.MaxInflightRecords);
        Assert.True(opts.Recovery);
        Assert.Equal(15_000, opts.RecoveryTimeoutMs);
        Assert.Equal(2_000, opts.RecoveryBackoffMs);
        Assert.Equal(4, opts.RecoveryRetries);
        Assert.Equal(300_000, opts.FlushTimeoutMs);
        Assert.Equal(60_000, opts.ServerLackOfAckTimeoutMs);
        Assert.Null(opts.StreamPausedMaxWaitTimeMs);
        Assert.Equal(5_000, opts.CallbackMaxWaitTimeMs);
        Assert.Null(opts.OnAck);
        Assert.Null(opts.OnError);
    }

    [Fact]
    public void Builder_CanOverrideIndividualValues()
    {
        var opts = StreamConfigurationOptions.NewBuilder()
            .SetMaxInflightRecords(50_000)
            .SetRecovery(false)
            .SetRecoveryTimeoutMs(5000)
            .SetFlushTimeoutMs(60_000)
            .Build();

        Assert.Equal(50_000, opts.MaxInflightRecords);
        Assert.False(opts.Recovery);
        Assert.Equal(5000, opts.RecoveryTimeoutMs);
        Assert.Equal(60_000, opts.FlushTimeoutMs);
        // Unset values keep defaults
        Assert.Equal(2_000, opts.RecoveryBackoffMs);
    }

    [Fact]
    public void Builder_SetAckCallback_SetsBothDelegates()
    {
        AckOnAckDelegate? onAckCalled = null;
        AckOnErrorDelegate? onErrorCalled = null;

        void OnAck(long id) => onAckCalled = OnAck;
        void OnError(long id, string msg) => onErrorCalled = OnError;

        var opts = StreamConfigurationOptions.NewBuilder()
            .SetAckCallback(OnAck, OnError)
            .Build();

        Assert.NotNull(opts.OnAck);
        Assert.NotNull(opts.OnError);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Builder_SetMaxInflightRecords_RejectsNonPositive(int value)
    {
        Assert.Throws<ArgumentException>(() =>
            StreamConfigurationOptions.NewBuilder().SetMaxInflightRecords(value));
    }

    [Theory]
    [InlineData(-1)]
    public void Builder_SetRecoveryTimeoutMs_RejectsNegative(int value)
    {
        Assert.Throws<ArgumentException>(() =>
            StreamConfigurationOptions.NewBuilder().SetRecoveryTimeoutMs(value));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Builder_SetFlushTimeoutMs_RejectsNonPositive(int value)
    {
        Assert.Throws<ArgumentException>(() =>
            StreamConfigurationOptions.NewBuilder().SetFlushTimeoutMs(value));
    }

    [Fact]
    public void Builder_SetStreamPausedMaxWaitTimeMs_AcceptsNegative()
    {
        // Negative means "wait full server-specified duration" — allowed
        var opts = StreamConfigurationOptions.NewBuilder()
            .SetStreamPausedMaxWaitTimeMs(-1)
            .Build();

        Assert.Equal(-1, opts.StreamPausedMaxWaitTimeMs);
    }

    [Fact]
    public void Builder_SetStreamPausedMaxWaitTimeMs_NullClearsValue()
    {
        var opts = StreamConfigurationOptions.NewBuilder()
            .SetStreamPausedMaxWaitTimeMs(null)
            .Build();

        Assert.Null(opts.StreamPausedMaxWaitTimeMs);
    }
}
