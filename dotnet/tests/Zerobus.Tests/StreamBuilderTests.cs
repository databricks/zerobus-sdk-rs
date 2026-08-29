using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class StreamBuilderTests
{
    [Test]
    public void Table_RequiresNonBlankValue()
    {
        Assert.That(
            () => new StreamBuilder(null!).Table(""),
            Throws.ArgumentException.With.Message.Contains("must not be empty"));
    }

    [Test]
    public void OAuth_RejectsNullClientId()
    {
        Assert.That(
            () => new StreamBuilder(null!).OAuth(null!, "secret"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void OAuth_RejectsNullClientSecret()
    {
        Assert.That(
            () => new StreamBuilder(null!).OAuth("id", null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Json_WithoutTable_ThrowsInvalidOperationException()
    {
        var builder = new StreamBuilder(null!)
            .OAuth("id", "secret");

        Assert.That(
            () => builder.Json(),
            Throws.InvalidOperationException.With.Message.Contains("Table"));
    }

    [Test]
    public void Json_WithoutOAuth_ThrowsInvalidOperationException()
    {
        var builder = new StreamBuilder(null!)
            .Table("catalog.schema.table");

        Assert.That(
            () => builder.Json(),
            Throws.InvalidOperationException.With.Message.Contains("OAuth"));
    }

    [Test]
    public void Arrow_WithNullSchema_ThrowsArgumentNullException()
    {
        var builder = new StreamBuilder(null!)
            .Table("catalog.schema.table")
            .OAuth("id", "secret");

        Assert.That(
            () => builder.Arrow(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Arrow_WithEmptySchema_ThrowsArgumentException()
    {
        var builder = new StreamBuilder(null!)
            .Table("catalog.schema.table")
            .OAuth("id", "secret");

        Assert.That(
            () => builder.Arrow([]),
            Throws.ArgumentException.With.Message.Contains("must not be empty"));
    }

    [Test]
    public void Proto_WithNullDescriptor_ThrowsArgumentNullException()
    {
        var builder = new StreamBuilder(null!)
            .Table("catalog.schema.table")
            .OAuth("id", "secret");

        Assert.That(
            () => builder.Proto(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void FluentChain_AllMethodsReturnSelf()
    {
        var builder = new StreamBuilder(null!);

        Assert.That(builder.Table("tbl"), Is.SameAs(builder));
        Assert.That(builder.OAuth("id", "secret"), Is.SameAs(builder));
        Assert.That(builder.MaxInflightRequests(50000), Is.SameAs(builder));
        Assert.That(builder.Recovery(false), Is.SameAs(builder));
        Assert.That(builder.FlushTimeoutMs(30000), Is.SameAs(builder));
    }

    [Test]
    public void ArrowStreamBuilder_FluentChain_ReturnsSelf()
    {
        var arrowBuilder = new StreamBuilder(null!)
            .Table("tbl")
            .OAuth("id", "secret")
            .Arrow([0x01, 0x02, 0x03]);

        Assert.That(arrowBuilder.MaxInflightBatches(1000), Is.SameAs(arrowBuilder));
        Assert.That(arrowBuilder.IpcCompression(IPCCompressionType.Zstd), Is.SameAs(arrowBuilder));
        Assert.That(arrowBuilder.StreamPausedMaxWaitTimeMs(5000), Is.SameAs(arrowBuilder));
    }

    [Test]
    public void BuildOptions_AppliesAllOverrides()
    {
        var options = new StreamBuilder(null!)
            .MaxInflightRequests(50000)
            .Recovery(false)
            .RecoveryTimeoutMs(10000)
            .FlushTimeoutMs(60000)
            .BuildOptions();

        Assert.That(options.MaxInflightRequests, Is.EqualTo(50000UL));
        Assert.That(options.Recovery, Is.False);
        Assert.That(options.RecoveryTimeoutMs, Is.EqualTo(10000UL));
        Assert.That(options.FlushTimeoutMs, Is.EqualTo(60000UL));
    }

    [Test]
    public void BuildOptions_UnsetFieldsKeepDefaults()
    {
        var options = new StreamBuilder(null!).BuildOptions();

        Assert.That(options.MaxInflightRequests, Is.EqualTo(1_000_000UL));
        Assert.That(options.Recovery, Is.True);
        Assert.That(options.RecoveryRetries, Is.EqualTo(4U));
    }

    [Test]
    public void BuildArrowOptions_AppliesSharedConfig()
    {
        var options = new StreamBuilder(null!)
            .Recovery(false)
            .FlushTimeoutMs(120_000)
            .BuildArrowOptions();

        Assert.That(options.Recovery, Is.False);
        Assert.That(options.FlushTimeoutMs, Is.EqualTo(120_000UL));
        // Arrow-specific defaults
        Assert.That(options.MaxInflightBatches, Is.EqualTo(1_000U));
    }
}
