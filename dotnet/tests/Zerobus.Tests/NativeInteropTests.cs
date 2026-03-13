using Databricks.Zerobus;
using Databricks.Zerobus.Native;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class NativeInteropTests
{
    [Test]
    public void ConvertConfig_CustomOptions_AppliesValues()
    {
        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 42,
            Recovery = false,
            RecoveryTimeoutMs = 100,
            RecoveryBackoffMs = 200,
            RecoveryRetries = 3,
            ServerLackOfAckTimeoutMs = 1000,
            FlushTimeoutMs = 2000,
            RecordType = RecordType.Json,
            StreamPausedMaxWaitTimeMs = 500,
        };

        var native = NativeInterop.ConvertConfig(options);

        Assert.That(native.MaxInflightRequests, Is.EqualTo((nuint)42));
        Assert.That(native.Recovery, Is.False);
        Assert.That(native.RecoveryTimeoutMs, Is.EqualTo(100UL));
        Assert.That(native.RecoveryBackoffMs, Is.EqualTo(200UL));
        Assert.That(native.RecoveryRetries, Is.EqualTo(3U));
        Assert.That(native.ServerLackOfAckTimeoutMs, Is.EqualTo(1000UL));
        Assert.That(native.FlushTimeoutMs, Is.EqualTo(2000UL));
        Assert.That(native.RecordType, Is.EqualTo((int)RecordType.Json));
        Assert.That(native.StreamPausedMaxWaitTimeMs, Is.EqualTo(500UL));
        Assert.That(native.HasStreamPausedMaxWaitTimeMs, Is.True);
    }

    [Test]
    public void ConvertConfig_NullStreamPausedWait_SetsFlagToFalse()
    {
        var options = StreamConfigurationOptions.Default;

        var native = NativeInterop.ConvertConfig(options);

        Assert.That(native.HasStreamPausedMaxWaitTimeMs, Is.False);
    }

    [Test]
    public void ToException_SuccessResult_ReturnsNull()
    {
        var result = new CResult { Success = true, ErrorMessage = IntPtr.Zero, IsRetryable = false };

        var ex = NativeInterop.ToException(ref result);

        Assert.That(ex, Is.Null);
    }

    [Test]
    public void ToException_FailureWithNullMessage_ReturnsUnknownError()
    {
        var result = new CResult { Success = false, ErrorMessage = IntPtr.Zero, IsRetryable = false };

        var ex = NativeInterop.ToException(ref result);

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.RawMessage, Is.EqualTo("unknown error"));
        Assert.That(ex.IsRetryable, Is.False);
    }

    [Test]
    public void ToException_RetryableFailure_SetsIsRetryable()
    {
        var result = new CResult { Success = false, ErrorMessage = IntPtr.Zero, IsRetryable = true };

        var ex = NativeInterop.ToException(ref result);

        Assert.That(ex, Is.Not.Null);
        Assert.That(ex!.IsRetryable, Is.True);
    }
}
