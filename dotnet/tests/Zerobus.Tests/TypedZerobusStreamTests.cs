using System.Reflection;
using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class TypedZerobusStreamTests
{
    [Test]
    public void JsonStream_ExposesOnlyJsonIngestOverloads()
    {
        var singleRecord = typeof(JsonZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(JsonZerobusStream.IngestRecord))
            .ToArray();

        var batch = typeof(JsonZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(JsonZerobusStream.IngestRecords))
            .ToArray();

        Assert.That(singleRecord, Has.Length.EqualTo(1));
        Assert.That(singleRecord[0].GetParameters().Select(parameter => parameter.ParameterType),
            Is.EqualTo(new[] { typeof(string) }));

        Assert.That(batch, Has.Length.EqualTo(1));
        Assert.That(batch[0].GetParameters().Select(parameter => parameter.ParameterType),
            Is.EqualTo(new[] { typeof(string[]) }));
    }

    [Test]
    public void ProtoStream_ExposesOnlyProtoIngestOverloads()
    {
        var singleRecord = typeof(ProtoZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(ProtoZerobusStream.IngestRecord))
            .ToArray();

        var batch = typeof(ProtoZerobusStream)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(method => method.Name == nameof(ProtoZerobusStream.IngestRecords))
            .ToArray();

        Assert.That(singleRecord.Select(method => method.GetParameters().Single().ParameterType),
            Is.EquivalentTo(new[] { typeof(byte[]), typeof(ReadOnlySpan<byte>) }));

        Assert.That(batch, Has.Length.EqualTo(1));
        Assert.That(batch[0].GetParameters().Select(parameter => parameter.ParameterType),
            Is.EqualTo(new[] { typeof(byte[][]) }));
    }
}