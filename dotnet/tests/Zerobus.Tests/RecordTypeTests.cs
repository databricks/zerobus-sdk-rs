using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class RecordTypeTests
{
    [Test]
    public void EnumValues_MatchCBindings()
    {
        // These values must match the C enum used by the Rust FFI layer.
        Assert.That((int)RecordType.Unspecified, Is.EqualTo(0));
        Assert.That((int)RecordType.Proto, Is.EqualTo(1));
        Assert.That((int)RecordType.Json, Is.EqualTo(2));
    }
}
