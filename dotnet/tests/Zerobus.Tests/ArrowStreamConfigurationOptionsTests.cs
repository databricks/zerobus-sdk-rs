using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ArrowStreamConfigurationOptionsTests
{
    [Test]
    public void Default_ReturnsExpectedValues()
    {
        var options = ArrowStreamConfigurationOptions.Default;

        Assert.That(options.MaxInflightBatches, Is.EqualTo(1_000U));
        Assert.That(options.Recovery, Is.True);
        Assert.That(options.IpcCompression, Is.EqualTo(IPCCompressionType.None));
        Assert.That(options.StreamPausedMaxWaitTimeMs, Is.EqualTo(-1));
        Assert.That(options.ConnectionTimeoutMs, Is.EqualTo(30_000UL));
    }

    [Test]
    public void WithExpression_OverridesSpecificFields()
    {
        var options = ArrowStreamConfigurationOptions.Default with
        {
            MaxInflightBatches = 5_000,
            IpcCompression = IPCCompressionType.Zstd,
            Recovery = false,
        };

        Assert.That(options.MaxInflightBatches, Is.EqualTo(5_000U));
        Assert.That(options.IpcCompression, Is.EqualTo(IPCCompressionType.Zstd));
        Assert.That(options.Recovery, Is.False);

        // Other fields remain default
        Assert.That(options.ConnectionTimeoutMs, Is.EqualTo(30_000UL));
        Assert.That(options.StreamPausedMaxWaitTimeMs, Is.EqualTo(-1));
    }

    [Test]
    public void WithExpression_CanSetNumericFieldsToNull()
    {
        var options = ArrowStreamConfigurationOptions.Default with
        {
            MaxInflightBatches = null,
            RecoveryTimeoutMs = null,
            RecoveryBackoffMs = null,
            RecoveryRetries = null,
            ConnectionTimeoutMs = null,
        };

        Assert.That(options.MaxInflightBatches, Is.Null);
        Assert.That(options.RecoveryTimeoutMs, Is.Null);
        Assert.That(options.RecoveryBackoffMs, Is.Null);
        Assert.That(options.RecoveryRetries, Is.Null);
        Assert.That(options.ConnectionTimeoutMs, Is.Null);
    }

    [Test]
    public void Record_SupportsEquality()
    {
        var a = ArrowStreamConfigurationOptions.Default;
        var b = ArrowStreamConfigurationOptions.Default;

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Record_DifferentValues_AreNotEqual()
    {
        var a = ArrowStreamConfigurationOptions.Default;
        var b = ArrowStreamConfigurationOptions.Default with { MaxInflightBatches = 500 };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void IpcCompressionType_Values()
    {
        Assert.That((int)IPCCompressionType.None, Is.EqualTo(-1));
        Assert.That((int)IPCCompressionType.Lz4Frame, Is.EqualTo(0));
        Assert.That((int)IPCCompressionType.Zstd, Is.EqualTo(1));
    }
}
