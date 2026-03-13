using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ZerobusSdkTests
{
    [Test]
    public void Constructor_NullEndpoint_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ZerobusSdk(null!, "https://workspace.databricks.com"));
    }

    [Test]
    public void Constructor_NullCatalogUrl_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ZerobusSdk("https://zerobus.databricks.com", null!));
    }
}
