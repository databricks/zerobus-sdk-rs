using Xunit;

namespace Databricks.Zerobus.Tests;

public class StreamBuilderTests
{
    [Fact]
    public void Table_RequiresNonBlankValue()
    {
        Assert.Throws<ArgumentException>(() => new StreamBuilder(null!).Table(""));
        Assert.Throws<ArgumentException>(() => new StreamBuilder(null!).Table("   "));
    }

    [Fact]
    public void OAuth_RequiresNonBlankValues()
    {
        Assert.Throws<ArgumentException>(() => new StreamBuilder(null!).OAuth("", "secret"));
        Assert.Throws<ArgumentException>(() => new StreamBuilder(null!).OAuth("id", ""));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxInflightRecords_RejectsNonPositive(int value)
    {
        Assert.Throws<ArgumentException>(() => new StreamBuilder(null!).MaxInflightRecords(value));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void FlushTimeoutMs_RejectsNonPositive(int value)
    {
        Assert.Throws<ArgumentException>(() => new StreamBuilder(null!).FlushTimeoutMs(value));
    }

    [Fact]
    public void TableProperties_StoresValues()
    {
        var props = new TableProperties(
            "catalog.schema.table",
            new List<ColumnDefinition>
            {
                new("id", "BIGINT", false, "Primary key"),
                new("name", "STRING", true)
            });

        Assert.Equal("catalog.schema.table", props.TableName);
        Assert.Equal(2, props.Columns.Count);
        Assert.Equal("id", props.Columns[0].Name);
        Assert.Equal("BIGINT", props.Columns[0].TypeName);
        Assert.False(props.Columns[0].Nullable);
        Assert.Equal("Primary key", props.Columns[0].Comment);
    }

    [Fact]
    public void EncodedBatch_StoresData()
    {
        var data = new byte[] { 1, 2, 3 };
        var lengths = new[] { 3 };
        var batch = new EncodedBatch(data, lengths);

        Assert.Same(data, batch.Data);
        Assert.Same(lengths, batch.Lengths);
    }

    [Fact]
    public void IPCCompressionType_Values()
    {
        Assert.Equal(-1, (int)IPCCompressionType.None);
        Assert.Equal(0, (int)IPCCompressionType.Lz4Frame);
        Assert.Equal(1, (int)IPCCompressionType.Zstd);
    }
}
