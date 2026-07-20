using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ZerobusExceptionTests
{
    [Test]
    public void Constructor_RetryableError_FormatsMessageCorrectly()
    {
        var ex = new ZerobusException("connection lost", isRetryable: true);

        Assert.That(ex.IsRetryable, Is.True);
        Assert.That(ex.RawMessage, Is.EqualTo("connection lost"));
        Assert.That(ex.Message, Does.Contain("retryable"));
        Assert.That(ex.Message, Does.Contain("connection lost"));
    }

    [Test]
    public void Constructor_NonRetryableError_FormatsMessageCorrectly()
    {
        var ex = new ZerobusException("invalid table name", isRetryable: false);

        Assert.That(ex.IsRetryable, Is.False);
        Assert.That(ex.RawMessage, Is.EqualTo("invalid table name"));
        Assert.That(ex.Message, Does.Not.Contain("retryable"));
        Assert.That(ex.Message, Does.Contain("invalid table name"));
    }

    [Test]
    public void Exception_IsStandardException()
    {
        var ex = new ZerobusException("test", isRetryable: false);

        Assert.That(ex, Is.InstanceOf<Exception>());
    }

    [Test]
    public void RetryableException_CanBeCaughtWithPattern()
    {
        var ex = new ZerobusException("timeout", isRetryable: true);

        try
        {
            throw ex;
        }
        catch (ZerobusException caught) when (caught.IsRetryable)
        {
            Assert.That(caught.IsRetryable, Is.True);
            return;
        }
        catch
        {
            Assert.Fail("Should have been caught by the retryable catch clause");
        }

        Assert.Fail("Exception was not thrown");
    }
}
