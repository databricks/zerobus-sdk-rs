using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class TablePropertiesTests
{
    [Test]
    public void Constructor_JsonTable_DescriptorProtoIsNull()
    {
        var props = new TableProperties("catalog.schema.table");

        Assert.That(props.TableName, Is.EqualTo("catalog.schema.table"));
        Assert.That(props.DescriptorProto, Is.Null);
    }

    [Test]
    public void Constructor_ProtoTable_HasDescriptorProto()
    {
        byte[] descriptor = [0x0A, 0x03, 0x66, 0x6F, 0x6F];
        var props = new TableProperties("catalog.schema.table", descriptor);

        Assert.That(props.TableName, Is.EqualTo("catalog.schema.table"));
        Assert.That(props.DescriptorProto, Is.Not.Null);
        Assert.That(props.DescriptorProto, Is.EqualTo(descriptor));
    }

    [Test]
    public void Record_SupportsEquality()
    {
        var a = new TableProperties("catalog.schema.table");
        var b = new TableProperties("catalog.schema.table");

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Record_DifferentTableNames_AreNotEqual()
    {
        var a = new TableProperties("catalog.schema.table1");
        var b = new TableProperties("catalog.schema.table2");

        Assert.That(a, Is.Not.EqualTo(b));
    }
}
