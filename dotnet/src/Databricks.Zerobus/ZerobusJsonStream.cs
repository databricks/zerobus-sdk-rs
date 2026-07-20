using Databricks.Zerobus.Native;
using System.Runtime.InteropServices;
using System.Text;

namespace Databricks.Zerobus;

/// <summary>
/// Stream for ingesting JSON records into a Unity Catalog Delta table.
/// Records are sent as JSON strings and are schema-free — no protobuf
/// compilation is required.
/// </summary>
/// <remarks>
/// JSON streams are ideal for quick starts or when the target schema is dynamic.
/// For production high-throughput workloads, prefer <see cref="ZerobusProtoStream{T}"/>.
/// </remarks>
public sealed class ZerobusJsonStream : BaseZerobusStream
{
    private readonly string _clientId;
    private readonly string _clientSecret;

    /// <summary>
    /// OAuth client ID used for stream authentication.
    /// </summary>
    public string ClientId => _clientId;

    /// <summary>
    /// OAuth client secret used for stream authentication.
    /// </summary>
    public string ClientSecret => _clientSecret;

    internal ZerobusJsonStream(
        IntPtr nativeHandle,
        string tableName,
        StreamConfigurationOptions options,
        string clientId,
        string clientSecret)
        : base(nativeHandle, tableName, options, isJsonMode: true)
    {
        _clientId = clientId;
        _clientSecret = clientSecret;
    }

    // ==================== Single Record Ingestion ====================

    /// <summary>
    /// Ingests a single JSON record string and returns its offset.
    /// </summary>
    /// <param name="json">The JSON record to ingest.</param>
    /// <returns>The offset of the queued record, or -1 on error.</returns>
    public long IngestRecord(string json)
    {
        if (json == null) throw new ArgumentNullException(nameof(json));
        EnsureOpen();

        var bytes = Encoding.UTF8.GetBytes(json);
        GCHandle handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            long offset = NativeMethods.zerobus_stream_ingest_json_record(
                NativeHandle,
                handle.AddrOfPinnedObject(),
                (UIntPtr)bytes.Length);

            if (offset < 0)
                throw new ZerobusException("Failed to ingest JSON record.", isRetryable: true);

            return offset;
        }
        finally
        {
            handle.Free();
        }
    }

    // ==================== Batch Ingestion ====================

    /// <summary>
    /// Ingests a batch of JSON record strings and returns the offset of the last record.
    /// Returns null if the input enumerable is empty.
    /// </summary>
    /// <param name="jsonRecords">The JSON records to ingest.</param>
    /// <returns>The offset of the last queued record, or null if empty.</returns>
    public long? IngestRecords(IReadOnlyList<string> jsonRecords)
    {
        if (jsonRecords == null) throw new ArgumentNullException(nameof(jsonRecords));
        if (jsonRecords.Count == 0) return null;
        EnsureOpen();

        var encodedRecords = new byte[jsonRecords.Count][];
        var recordPtrs = new IntPtr[jsonRecords.Count];
        var lengths = new IntPtr[jsonRecords.Count];
        var gcHandles = new GCHandle[jsonRecords.Count];

        try
        {
            for (int i = 0; i < jsonRecords.Count; i++)
            {
                if (jsonRecords[i] == null)
                    throw new ArgumentException($"JSON record at index {i} is null.", nameof(jsonRecords));

                encodedRecords[i] = Encoding.UTF8.GetBytes(jsonRecords[i]);
                gcHandles[i] = GCHandle.Alloc(encodedRecords[i], GCHandleType.Pinned);
                recordPtrs[i] = gcHandles[i].AddrOfPinnedObject();
                lengths[i] = (IntPtr)encodedRecords[i].Length;
            }

            long offset = NativeMethods.zerobus_stream_ingest_json_records(
                NativeHandle,
                recordPtrs,
                lengths,
                (UIntPtr)jsonRecords.Count);

            if (offset == -1)
                throw new ZerobusException("Failed to ingest JSON records batch.", isRetryable: true);
            if (offset == -2)
                return null;

            return offset;
        }
        finally
        {
            foreach (var h in gcHandles) h.Free();
        }
    }

    /// <summary>
    /// Ingests a batch of JSON record strings from an enumerable.
    /// </summary>
    public long? IngestRecords(IEnumerable<string> jsonRecords)
    {
        if (jsonRecords == null) throw new ArgumentNullException(nameof(jsonRecords));
        return IngestRecords(jsonRecords as IReadOnlyList<string> ?? jsonRecords.ToArray());
    }

    // ==================== Unacknowledged Records ====================

    /// <summary>
    /// Returns unacknowledged records as JSON strings.
    /// After the stream is closed, returns cached data.
    /// </summary>
    public IReadOnlyList<string> GetUnackedRecords()
    {
        IReadOnlyList<byte[]> raw;
        if (_disposed != 0 || IsClosed)
            raw = GetCachedUnackedRecords();
        else
            raw = GetNativeUnackedRecords();

        var result = new string[raw.Count];
        for (int i = 0; i < raw.Count; i++)
        {
            result[i] = Encoding.UTF8.GetString(raw[i]);
        }
        return result;
    }

    /// <summary>
    /// Returns unacknowledged records as raw byte arrays.
    /// </summary>
    public IReadOnlyList<byte[]> GetUnackedRecordBytes()
    {
        if (_disposed != 0 || IsClosed)
            return GetCachedUnackedRecords();

        return GetNativeUnackedRecords();
    }
}
