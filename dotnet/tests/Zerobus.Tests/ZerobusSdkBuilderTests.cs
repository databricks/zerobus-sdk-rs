using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ZerobusSdkBuilderTests
{
    // -------------------------------------------------------------------------
    // CreateBuilder / factory
    // -------------------------------------------------------------------------

    [Test]
    public void CreateBuilder_ReturnsNonNullBuilder()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        Assert.That(builder, Is.Not.Null);
    }

    // -------------------------------------------------------------------------
    // Fluent return-value contract (no native call needed)
    // -------------------------------------------------------------------------

    [Test]
    public void Endpoint_ReturnsSameBuilderInstance()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        var returned = builder.Endpoint("https://zerobus.databricks.com");

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void UnityCatalogUrl_ReturnsSameBuilderInstance()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        var returned = builder.UnityCatalogUrl("https://workspace.databricks.com");

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void ApplicationName_ReturnsSameBuilderInstance()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        var returned = builder.ApplicationName("my-app");

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void SdkIdentifier_ReturnsSameBuilderInstance()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        var returned = builder.SdkIdentifier("zerobus-sdk-dotnet/1.0.0");

        Assert.That(returned, Is.SameAs(builder));
    }

    [Test]
    public void DisableTls_ReturnsSameBuilderInstance()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        var returned = builder.DisableTls();

        Assert.That(returned, Is.SameAs(builder));
    }

    // -------------------------------------------------------------------------
    // Null argument guards
    // -------------------------------------------------------------------------

    [Test]
    public void Endpoint_NullValue_ThrowsArgumentNullException()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.Endpoint(null!));
    }

    [Test]
    public void UnityCatalogUrl_NullValue_ThrowsArgumentNullException()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.UnityCatalogUrl(null!));
    }

    [Test]
    public void ApplicationName_NullValue_ThrowsArgumentNullException()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.ApplicationName(null!));
    }

    [Test]
    public void SdkIdentifier_NullValue_ThrowsArgumentNullException()
    {
        using var builder = ZerobusSdk.CreateBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.SdkIdentifier(null!));
    }

    // -------------------------------------------------------------------------
    // ObjectDisposedException after Dispose()
    // -------------------------------------------------------------------------

    [Test]
    public void Endpoint_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.Endpoint("https://zerobus.databricks.com"));
    }

    [Test]
    public void UnityCatalogUrl_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.UnityCatalogUrl("https://workspace.databricks.com"));
    }

    [Test]
    public void ApplicationName_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.ApplicationName("my-app"));
    }

    [Test]
    public void SdkIdentifier_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.SdkIdentifier("zerobus-sdk-dotnet/1.0.0"));
    }

    [Test]
    public void DisableTls_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.DisableTls());
    }

    [Test]
    public void Build_AfterDispose_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.Build());
    }

    // -------------------------------------------------------------------------
    // Single-use contract: Build() consumes the builder
    // -------------------------------------------------------------------------

    [Test]
    public void Build_CalledTwice_ThrowsObjectDisposedExceptionOnSecondCall()
    {
        var builder = ZerobusSdk.CreateBuilder()
            .Endpoint("https://zerobus.databricks.com")
            .UnityCatalogUrl("https://workspace.databricks.com");

        using var sdk = builder.Build();

        Assert.Throws<ObjectDisposedException>(() => builder.Build());
    }

    [Test]
    public void Endpoint_AfterBuild_ThrowsObjectDisposedException()
    {
        var builder = ZerobusSdk.CreateBuilder()
            .Endpoint("https://zerobus.databricks.com")
            .UnityCatalogUrl("https://workspace.databricks.com");

        using var sdk = builder.Build();

        Assert.Throws<ObjectDisposedException>(() => builder.Endpoint("https://other.databricks.com"));
    }

    // -------------------------------------------------------------------------
    // Dispose is idempotent
    // -------------------------------------------------------------------------

    [Test]
    public void Dispose_CalledMultipleTimes_DoesNotThrow()
    {
        var builder = ZerobusSdk.CreateBuilder();

        Assert.DoesNotThrow(() =>
        {
            builder.Dispose();
            builder.Dispose();
            builder.Dispose();
        });
    }
}
