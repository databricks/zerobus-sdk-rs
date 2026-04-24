package com.databricks.zerobus;

/**
 * Compression type for Arrow IPC messages.
 *
 * <p>When set on {@link ArrowStreamConfigurationOptions}, the SDK compresses each Arrow record
 * batch before transmitting it over the Arrow Flight connection. This can significantly reduce
 * network bandwidth at the cost of additional CPU usage.
 *
 * <p>Supported codecs:
 *
 * <ul>
 *   <li>{@link #NONE} - No compression (default)
 *   <li>{@link #LZ4_FRAME} - LZ4 frame compression (fast, moderate ratio)
 *   <li>{@link #ZSTD} - Zstandard compression (slower, better ratio)
 * </ul>
 *
 * @see ArrowStreamConfigurationOptions.ArrowStreamConfigurationOptionsBuilder#setIpcCompression
 */
public enum IPCCompressionType {
  /** No compression. This is the default. */
  NONE,

  /** LZ4 frame compression. Offers fast compression with a moderate compression ratio. */
  LZ4_FRAME,

  /** Zstandard compression. Offers slower compression with a better compression ratio. */
  ZSTD
}
