using Xunit;

namespace Databricks.Zerobus.Tests;

public class ExceptionTests
{
    [Fact]
    public void ZerobusException_StoresMessage()
    {
        var ex = new ZerobusException("test message");
        Assert.Equal("test message", ex.Message);
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void ZerobusException_StoresRetryableFlag()
    {
        var ex = new ZerobusException("retryable error", isRetryable: true);
        Assert.True(ex.IsRetryable);
    }

    [Fact]
    public void ZerobusException_StoresInnerException()
    {
        var inner = new InvalidOperationException("inner");
        var ex = new ZerobusException("outer", isRetryable: true, innerException: inner);
        Assert.Same(inner, ex.InnerException);
        Assert.True(ex.IsRetryable);
    }

    [Fact]
    public void NonRetriableException_AlwaysHasRetryableFalse()
    {
        var ex = new NonRetriableException("non-retryable");
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void NonRetriableException_InheritsFromZerobusException()
    {
        var ex = new NonRetriableException("test");
        Assert.IsAssignableFrom<ZerobusException>(ex);
    }
}
