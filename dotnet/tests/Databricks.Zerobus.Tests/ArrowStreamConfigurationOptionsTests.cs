using Xunit;

namespace Databricks.Zerobus.Tests;

public class ArrowStreamConfigurationOptionsTests
{
    [Fact]
    public void Default_ReturnsOptionsWithSensibleDefaults()
    {
        var opts = ArrowStreamConfigurationOptions.Default;

        Assert.Equal(10_000, opts.MaxInflightBatches);
        Assert.True(opts.Recovery);
        Assert.Equal(30_000, opts.ConnectionTimeoutMs);
        Assert.Equal(IPCCompressionType.None, opts.IpcCompression);
    }

    [Fact]
    public void Builder_CanOverrideIndividualValues()
    {
        var opts = ArrowStreamConfigurationOptions.NewBuilder()
            .SetMaxInflightBatches(5_000)
            .SetRecovery(false)
            .SetConnectionTimeoutMs(60_000)
            .SetIpcCompression(IPCCompressionType.Zstd)
            .SetStreamPausedMaxWaitTimeMs(-1)
            .Build();

        Assert.Equal(5_000, opts.MaxInflightBatches);
        Assert.False(opts.Recovery);
        Assert.Equal(60_000, opts.ConnectionTimeoutMs);
        Assert.Equal(IPCCompressionType.Zstd, opts.IpcCompression);
        Assert.Equal(-1, opts.StreamPausedMaxWaitTimeMs);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Builder_SetMaxInflightBatches_RejectsNonPositive(int value)
    {
        Assert.Throws<ArgumentException>(() =>
            ArrowStreamConfigurationOptions.NewBuilder().SetMaxInflightBatches(value));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Builder_SetConnectionTimeoutMs_RejectsNonPositive(int value)
    {
        Assert.Throws<ArgumentException>(() =>
            ArrowStreamConfigurationOptions.NewBuilder().SetConnectionTimeoutMs(value));
    }
}
