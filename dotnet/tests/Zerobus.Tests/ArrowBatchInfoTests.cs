using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ArrowBatchInfoTests
{
    [Test]
    public void Constructor_StoresData()
    {
        var data = new byte[] { 0x01, 0x02, 0x03 };
        var batch = new ArrowBatchInfo(data);

        Assert.That(batch.Data, Is.SameAs(data));
    }

    [Test]
    public void Record_SupportsEquality()
    {
        var data = new byte[] { 0x01, 0x02 };
        var a = new ArrowBatchInfo(data);
        var b = new ArrowBatchInfo(data);

        // Records with reference-type properties compare by reference for the array
        // so two copies with different arrays are not equal
        Assert.That(a, Is.EqualTo(b));
    }
}
