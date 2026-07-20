using Databricks.Zerobus.Native;
using Google.Protobuf;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus;

/// <summary>
/// Stream for ingesting Protocol Buffer records into a Unity Catalog Delta table.
/// Supports both typed proto messages and pre-serialized byte arrays.
/// </summary>
/// <typeparam name="T">The protobuf message type. Must implement <see cref="IMessage{T}"/>.</typeparam>
/// <remarks>
/// <para>For single-record ingestion, use <see cref="IngestRecord(T)"/>.
/// For batch ingestion, use <see cref="IngestRecords(IEnumerable{T})"/>.</para>
/// </remarks>
public sealed class ZerobusProtoStream<T> : BaseZerobusStream where T : IMessage<T>
{
    private readonly byte[] _descriptorProtoBytes;
    private readonly string _clientId;
    private readonly string _clientSecret;

    /// <summary>
    /// The compiled descriptor proto bytes used to create this stream.
    /// </summary>
    public byte[] DescriptorProtoBytes => _descriptorProtoBytes;

    /// <summary>
    /// OAuth client ID used for stream authentication.
    /// </summary>
    public string ClientId => _clientId;

    /// <summary>
    /// OAuth client secret used for stream authentication.
    /// </summary>
    public string ClientSecret => _clientSecret;

    internal ZerobusProtoStream(
        IntPtr nativeHandle,
        string tableName,
        StreamConfigurationOptions options,
        byte[] descriptorProtoBytes,
        string clientId,
        string clientSecret)
        : base(nativeHandle, tableName, options, isJsonMode: false)
    {
        _descriptorProtoBytes = descriptorProtoBytes;
        _clientId = clientId;
        _clientSecret = clientSecret;
    }

    // ==================== Single Record Ingestion ====================

    /// <summary>
    /// Ingests a single protobuf record and returns its offset for acknowledgment tracking.
    /// The record is serialized and queued; the call returns immediately.
    /// </summary>
    /// <param name="record">The protobuf message to ingest.</param>
    /// <returns>The offset of the queued record, or -1 on error.</returns>
    /// <exception cref="ZerobusException">Thrown if the stream is closed.</exception>
    public long IngestRecord(T record)
    {
        if (record == null) throw new ArgumentNullException(nameof(record));
        EnsureOpen();
        byte[] bytes = record.ToByteArray();
        return IngestBytes(bytes);
    }

    /// <summary>
    /// Ingests a pre-serialized protobuf record and returns its offset.
    /// </summary>
    /// <param name="encodedBytes">The pre-serialized protobuf message bytes.</param>
    /// <returns>The offset of the queued record, or -1 on error.</returns>
    public long IngestRecord(byte[] encodedBytes)
    {
        if (encodedBytes == null) throw new ArgumentNullException(nameof(encodedBytes));
        EnsureOpen();
        return IngestBytes(encodedBytes);
    }

    private long IngestBytes(byte[] bytes)
    {
        GCHandle handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            long offset = NativeMethods.zerobus_stream_ingest_proto_record(
                NativeHandle,
                handle.AddrOfPinnedObject(),
                (UIntPtr)bytes.Length);

            if (offset < 0)
                throw new ZerobusException("Failed to ingest proto record.", isRetryable: true);

            return offset;
        }
        finally
        {
            handle.Free();
        }
    }

    // ==================== Batch Ingestion ====================

    /// <summary>
    /// Ingests a batch of protobuf records and returns the offset of the last record.
    /// Returns null if the input enumerable is empty.
    /// </summary>
    /// <param name="records">The protobuf messages to ingest.</param>
    /// <returns>The offset of the last queued record, or null if empty.</returns>
    public long? IngestRecords(IEnumerable<T> records)
    {
        if (records == null) throw new ArgumentNullException(nameof(records));
        EnsureOpen();

        var encodedRecords = new List<byte[]>();
        foreach (var record in records)
        {
            encodedRecords.Add(record.ToByteArray());
        }

        return IngestRecords(encodedRecords);
    }

    /// <summary>
    /// Ingests a batch of pre-serialized protobuf records.
    /// Returns null if the input list is empty.
    /// </summary>
    /// <param name="encodedRecords">The pre-serialized protobuf message bytes.</param>
    /// <returns>The offset of the last queued record, or null if empty.</returns>
    public long? IngestRecords(IReadOnlyList<byte[]> encodedRecords)
    {
        if (encodedRecords == null) throw new ArgumentNullException(nameof(encodedRecords));
        if (encodedRecords.Count == 0) return null;
        EnsureOpen();

        var recordPtrs = new IntPtr[encodedRecords.Count];
        var lengths = new IntPtr[encodedRecords.Count];
        var gcHandles = new GCHandle[encodedRecords.Count];

        try
        {
            for (int i = 0; i < encodedRecords.Count; i++)
            {
                gcHandles[i] = GCHandle.Alloc(encodedRecords[i], GCHandleType.Pinned);
                recordPtrs[i] = gcHandles[i].AddrOfPinnedObject();
                lengths[i] = (IntPtr)encodedRecords[i].Length;
            }

            long offset = NativeMethods.zerobus_stream_ingest_proto_records(
                NativeHandle,
                recordPtrs,
                lengths,
                (UIntPtr)encodedRecords.Count);

            if (offset == -1)
                throw new ZerobusException("Failed to ingest proto records batch.", isRetryable: true);
            if (offset == -2)
                return null;

            return offset;
        }
        finally
        {
            foreach (var h in gcHandles) h.Free();
        }
    }

    // ==================== Unacknowledged Records ====================

    /// <summary>
    /// Returns unacknowledged records. After the stream is closed, returns cached data.
    /// </summary>
    public IReadOnlyList<byte[]> GetUnackedRecords()
    {
        if (_disposed != 0 || IsClosed)
            return GetCachedUnackedRecords();

        return GetNativeUnackedRecords();
    }

    /// <summary>
    /// Returns unacknowledged records as typed protobuf messages.
    /// Requires a message parser for deserialization.
    /// </summary>
    public IReadOnlyList<T> GetUnackedRecords(MessageParser<T> parser)
    {
        if (parser == null) throw new ArgumentNullException(nameof(parser));

        var raw = GetUnackedRecords();
        var result = new List<T>(raw.Count);
        foreach (var bytes in raw)
        {
            try
            {
                result.Add(parser.ParseFrom(bytes));
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new ZerobusException("Failed to parse unacked record.", isRetryable: false, ex);
            }
        }
        return result;
    }
}
