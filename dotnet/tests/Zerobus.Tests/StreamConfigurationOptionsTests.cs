using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class StreamConfigurationOptionsTests
{
    [Test]
    public void Default_ReturnsExpectedValues()
    {
        var options = StreamConfigurationOptions.Default;

        Assert.That(options.MaxInflightRequests, Is.EqualTo(1_000_000UL));
        Assert.That(options.Recovery, Is.True);
        Assert.That(options.RecoveryTimeoutMs, Is.EqualTo(15_000UL));
        Assert.That(options.RecoveryBackoffMs, Is.EqualTo(2_000UL));
        Assert.That(options.RecoveryRetries, Is.EqualTo(4U));
        Assert.That(options.ServerLackOfAckTimeoutMs, Is.EqualTo(60_000UL));
        Assert.That(options.FlushTimeoutMs, Is.EqualTo(300_000UL));
        Assert.That(options.RecordType, Is.EqualTo(RecordType.Proto));
        Assert.That(options.StreamPausedMaxWaitTimeMs, Is.Null);
    }

    [Test]
    public void WithExpression_OverridesSpecificFields()
    {
        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 50_000,
            RecordType = RecordType.Json,
        };

        Assert.That(options.MaxInflightRequests, Is.EqualTo(50_000UL));
        Assert.That(options.RecordType, Is.EqualTo(RecordType.Json));

        // Other fields remain default.
        Assert.That(options.Recovery, Is.True);
        Assert.That(options.RecoveryTimeoutMs, Is.EqualTo(15_000UL));
    }

    [Test]
    public void WithExpression_CanSetStreamPausedMaxWaitTime()
    {
        var options = StreamConfigurationOptions.Default with
        {
            StreamPausedMaxWaitTimeMs = 5_000,
        };

        Assert.That(options.StreamPausedMaxWaitTimeMs, Is.EqualTo(5_000UL));
    }

    [Test]
    public void WithExpression_CanSetStreamPausedMaxWaitTimeToZero()
    {
        var options = StreamConfigurationOptions.Default with
        {
            StreamPausedMaxWaitTimeMs = 0,
        };

        Assert.That(options.StreamPausedMaxWaitTimeMs, Is.Not.Null);
        Assert.That(options.StreamPausedMaxWaitTimeMs, Is.EqualTo(0UL));
    }

    [Test]
    public void Record_SupportsEquality()
    {
        var a = StreamConfigurationOptions.Default;
        var b = StreamConfigurationOptions.Default;

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Record_DifferentValues_AreNotEqual()
    {
        var a = StreamConfigurationOptions.Default;
        var b = StreamConfigurationOptions.Default with { RecordType = RecordType.Json };

        Assert.That(a, Is.Not.EqualTo(b));
    }
}
