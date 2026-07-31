package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import java.util.Collections;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link StreamBuilder}.
 *
 * <p>Most tests verify required-field validation and configuration accumulation without requiring
 * the native library. They exercise the builder up to (but not including) {@code build()}, which is
 * the only operation that touches native code, so a {@code null} SDK reference is sufficient.
 *
 * <p>The routing tests ({@code *RoutesTo*}) additionally verify that each terminal {@code build()}
 * dispatches to the correct {@code ZerobusSdk.create*StreamInternal} method with the right table,
 * credentials, and options. They use a mocked SDK and are skipped when the native library is
 * unavailable, because instantiating (or mocking) {@link ZerobusSdk} triggers its static
 * initializer, which loads the native library.
 */
public class StreamBuilderTest {

  private static StreamBuilder builder() {
    return new StreamBuilder(null);
  }

  /**
   * Skips a test unless the native library is loadable. Mocking {@link ZerobusSdk} initializes the
   * class, which calls {@code NativeLoader.ensureLoaded()} in its static block.
   */
  private static void assumeNativeLibrary() {
    boolean available;
    try {
      NativeLoader.ensureLoaded();
      available = true;
    } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
      available = false;
    }
    assumeTrue(available, "Native library required to mock ZerobusSdk");
  }

  // ==================== Validation ====================

  @Test
  void validateRequiredThrowsWithoutTable() {
    StreamBuilder b = builder().oauth("client-id", "client-secret");
    IllegalStateException ex = assertThrows(IllegalStateException.class, b::validateRequired);
    assertTrue(ex.getMessage().contains("table name is required"));
  }

  @Test
  void validateRequiredThrowsWithoutAuth() {
    StreamBuilder b = builder().table("catalog.schema.table");
    IllegalStateException ex = assertThrows(IllegalStateException.class, b::validateRequired);
    assertTrue(ex.getMessage().contains("authentication is required"));
  }

  @Test
  void validateRequiredPassesWithTableAndOauth() {
    StreamBuilder b = builder().table("catalog.schema.table").oauth("client-id", "client-secret");
    assertDoesNotThrow(b::validateRequired);
  }

  @Test
  void tableRejectsNullAndBlankNames() {
    assertThrows(NullPointerException.class, () -> builder().table(null));
    assertThrows(IllegalArgumentException.class, () -> builder().table(""));
    assertThrows(IllegalArgumentException.class, () -> builder().table("   "));
  }

  @Test
  void oauthRejectsNullAndBlankCredentials() {
    assertThrows(NullPointerException.class, () -> builder().oauth(null, "client-secret"));
    assertThrows(NullPointerException.class, () -> builder().oauth("client-id", null));
    assertThrows(IllegalArgumentException.class, () -> builder().oauth("", "client-secret"));
    assertThrows(IllegalArgumentException.class, () -> builder().oauth("client-id", "   "));
  }

  @Test
  void compiledProtoRejectsNullDescriptor() {
    assertThrows(NullPointerException.class, () -> builder().compiledProto(null));
  }

  @Test
  void arrowRejectsNullSchema() {
    assertThrows(NullPointerException.class, () -> builder().arrow(null));
  }

  @Test
  void ackCallbackRejectsNullCallback() {
    assertThrows(NullPointerException.class, () -> builder().ackCallback(null));
  }

  // ==================== Numeric setter validation ====================
  //
  // These values are cast to unsigned Rust integers across JNI, so a negative would silently
  // become a huge positive value. Limits and timeouts must be positive; retry counts and backoff
  // delays must be non-negative; the Arrow streamPausedMaxWaitTimeMs negative is intentional.

  @Test
  void maxInflightRecordsRejectsNonPositive() {
    assertThrows(IllegalArgumentException.class, () -> builder().maxInflightRecords(0));
    assertThrows(IllegalArgumentException.class, () -> builder().maxInflightRecords(-1));
  }

  @Test
  void timeoutSettersRejectNonPositive() {
    assertThrows(IllegalArgumentException.class, () -> builder().recoveryTimeoutMs(0));
    assertThrows(IllegalArgumentException.class, () -> builder().flushTimeoutMs(-5));
    assertThrows(IllegalArgumentException.class, () -> builder().serverLackOfAckTimeoutMs(0));
  }

  @Test
  void retryAndBackoffSettersRejectNegativeButAllowZero() {
    assertThrows(IllegalArgumentException.class, () -> builder().recoveryRetries(-1));
    assertThrows(IllegalArgumentException.class, () -> builder().recoveryBackoffMs(-1));
    // Zero is meaningful: no retries / no backoff delay.
    assertDoesNotThrow(() -> builder().recoveryRetries(0).recoveryBackoffMs(0));
  }

  @Test
  void arrowSettersRejectNonPositiveExceptStreamPausedWait() {
    assertThrows(
        IllegalArgumentException.class, () -> builder().arrow(emptySchema()).maxInflightBatches(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> builder().arrow(emptySchema()).connectionTimeoutMs(-1L));
    // A negative streamPausedMaxWaitTimeMs means "wait the full server-specified duration".
    assertDoesNotThrow(() -> builder().arrow(emptySchema()).streamPausedMaxWaitTimeMs(-1L));
  }

  // ==================== gRPC option accumulation ====================

  @Test
  void streamOptionsDefaultToGrpcDefaults() {
    StreamConfigurationOptions options = builder().toStreamOptions();
    StreamConfigurationOptions defaults = StreamConfigurationOptions.getDefault();

    assertEquals(defaults.maxInflightRecords(), options.maxInflightRecords());
    assertEquals(defaults.recovery(), options.recovery());
    assertEquals(defaults.recoveryTimeoutMs(), options.recoveryTimeoutMs());
    assertEquals(defaults.recoveryBackoffMs(), options.recoveryBackoffMs());
    assertEquals(defaults.recoveryRetries(), options.recoveryRetries());
    assertEquals(defaults.flushTimeoutMs(), options.flushTimeoutMs());
    assertEquals(defaults.serverLackOfAckTimeoutMs(), options.serverLackOfAckTimeoutMs());
  }

  @Test
  void streamOptionsReflectSetters() {
    StreamConfigurationOptions options =
        builder()
            .maxInflightRecords(123)
            .recovery(false)
            .recoveryTimeoutMs(11)
            .recoveryBackoffMs(22)
            .recoveryRetries(7)
            .serverLackOfAckTimeoutMs(33)
            .flushTimeoutMs(44)
            .toStreamOptions();

    assertEquals(123, options.maxInflightRecords());
    assertFalse(options.recovery());
    assertEquals(11, options.recoveryTimeoutMs());
    assertEquals(22, options.recoveryBackoffMs());
    assertEquals(7, options.recoveryRetries());
    assertEquals(33, options.serverLackOfAckTimeoutMs());
    assertEquals(44, options.flushTimeoutMs());
  }

  @Test
  void ackCallbackIsCarriedThrough() {
    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {}

          @Override
          public void onError(long offsetId, String errorMessage) {}
        };
    StreamConfigurationOptions options = builder().ackCallback(callback).toStreamOptions();
    assertTrue(options.getNewAckCallback().isPresent());
    assertSame(callback, options.getNewAckCallback().get());
  }

  // ==================== Arrow option accumulation ====================

  private static Schema emptySchema() {
    return new Schema(Collections.emptyList());
  }

  @Test
  void arrowOptionsPreserveArrowDefaults() {
    // Crucially, unset shared values must fall back to Arrow's own defaults (e.g. 4 recovery
    // retries) rather than the gRPC defaults (3).
    ArrowStreamConfigurationOptions options = builder().arrow(emptySchema()).buildOptions();
    ArrowStreamConfigurationOptions defaults = ArrowStreamConfigurationOptions.getDefault();

    assertEquals(defaults.maxInflightBatches(), options.maxInflightBatches());
    assertEquals(defaults.recovery(), options.recovery());
    assertEquals(defaults.recoveryTimeoutMs(), options.recoveryTimeoutMs());
    assertEquals(defaults.recoveryBackoffMs(), options.recoveryBackoffMs());
    assertEquals(defaults.recoveryRetries(), options.recoveryRetries());
    assertEquals(defaults.serverLackOfAckTimeoutMs(), options.serverLackOfAckTimeoutMs());
    assertEquals(defaults.flushTimeoutMs(), options.flushTimeoutMs());
    assertEquals(defaults.connectionTimeoutMs(), options.connectionTimeoutMs());
    assertEquals(defaults.ipcCompression(), options.ipcCompression());
    assertEquals(defaults.streamPausedMaxWaitTimeMs(), options.streamPausedMaxWaitTimeMs());
  }

  @Test
  void arrowOptionsApplySharedAndArrowSpecificSetters() {
    ArrowStreamConfigurationOptions options =
        builder()
            .recovery(false)
            .recoveryRetries(9)
            .flushTimeoutMs(55)
            .arrow(emptySchema())
            .maxInflightBatches(7)
            .connectionTimeoutMs(12345L)
            .ipcCompression(IPCCompressionType.ZSTD)
            .streamPausedMaxWaitTimeMs(678L)
            .buildOptions();

    // Shared values set on the base builder.
    assertFalse(options.recovery());
    assertEquals(9, options.recoveryRetries());
    assertEquals(55, options.flushTimeoutMs());

    // Arrow-specific values.
    assertEquals(7, options.maxInflightBatches());
    assertEquals(12345L, options.connectionTimeoutMs());
    assertEquals(IPCCompressionType.ZSTD, options.ipcCompression());
    assertEquals(678L, options.streamPausedMaxWaitTimeMs());
  }

  @Test
  void maxInflightRecordsDoesNotAffectArrowOptions() {
    // maxInflightRecords is a gRPC-only knob; Arrow uses maxInflightBatches.
    ArrowStreamConfigurationOptions options =
        builder().maxInflightRecords(42).arrow(emptySchema()).buildOptions();
    assertEquals(
        ArrowStreamConfigurationOptions.getDefault().maxInflightBatches(),
        options.maxInflightBatches());
  }

  // ==================== Terminal build() routing ====================
  //
  // Verify that each sub-builder's build() dispatches to the matching create*StreamInternal method
  // with the configured table, credentials, and options — and never to the other two.

  @Test
  void jsonBuildRoutesToCreateJsonStreamInternal() {
    assumeNativeLibrary();
    ZerobusSdk sdk = mock(ZerobusSdk.class);
    HeadersProvider provider = () -> Collections.singletonMap("authorization", "Bearer token");

    new StreamBuilder(sdk)
        .table("cat.sch.json")
        .headersProvider(provider)
        .oauth("json-id", "json-secret")
        .maxInflightRecords(4242)
        .json()
        .build();

    ArgumentCaptor<StreamConfigurationOptions> opts =
        ArgumentCaptor.forClass(StreamConfigurationOptions.class);
    verify(sdk)
        .createJsonStreamInternal(
            eq("cat.sch.json"), eq("json-id"), eq("json-secret"), eq(null), opts.capture());
    assertEquals(4242, opts.getValue().maxInflightRecords());
    verify(sdk, never()).createProtoStreamInternal(any(), any(), any(), any(), any(), any());
    verify(sdk, never()).createArrowStreamInternal(any(), any(), any(), any(), any(), any());
  }

  @Test
  void headersProviderRoutesToCreateJsonStreamInternal() throws Exception {
    assumeNativeLibrary();
    ZerobusSdk sdk = mock(ZerobusSdk.class);
    HeadersProvider provider = () -> Collections.singletonMap("authorization", "Bearer token");

    StreamBuilder builder =
        new StreamBuilder(sdk)
            .table("cat.sch.json")
            .oauth("unused-id", "unused-secret")
            .headersProvider(provider);
    assertDoesNotThrow(builder::validateRequired);
    builder.json().build();

    verify(sdk)
        .createJsonStreamInternal(eq("cat.sch.json"), eq(null), eq(null), eq(provider), any());
  }

  @Test
  void compiledProtoBuildRoutesToCreateProtoStreamInternal() {
    assumeNativeLibrary();
    ZerobusSdk sdk = mock(ZerobusSdk.class);
    DescriptorProto descriptor = DescriptorProto.newBuilder().setName("Rec").build();

    new StreamBuilder(sdk)
        .table("cat.sch.proto")
        .oauth("proto-id", "proto-secret")
        .recoveryRetries(9)
        .compiledProto(descriptor)
        .build();

    ArgumentCaptor<StreamConfigurationOptions> opts =
        ArgumentCaptor.forClass(StreamConfigurationOptions.class);
    verify(sdk)
        .createProtoStreamInternal(
            eq("cat.sch.proto"),
            eq(descriptor),
            eq("proto-id"),
            eq("proto-secret"),
            eq(null),
            opts.capture());
    assertEquals(9, opts.getValue().recoveryRetries());
    verify(sdk, never()).createJsonStreamInternal(any(), any(), any(), any(), any());
    verify(sdk, never()).createArrowStreamInternal(any(), any(), any(), any(), any(), any());
  }

  @Test
  void arrowBuildRoutesToCreateArrowStreamInternal() {
    assumeNativeLibrary();
    ZerobusSdk sdk = mock(ZerobusSdk.class);
    Schema schema = emptySchema();

    new StreamBuilder(sdk)
        .table("cat.sch.arrow")
        .oauth("arrow-id", "arrow-secret")
        .recovery(false)
        .arrow(schema)
        .maxInflightBatches(11)
        .build();

    ArgumentCaptor<ArrowStreamConfigurationOptions> opts =
        ArgumentCaptor.forClass(ArrowStreamConfigurationOptions.class);
    verify(sdk)
        .createArrowStreamInternal(
            eq("cat.sch.arrow"),
            eq(schema),
            eq("arrow-id"),
            eq("arrow-secret"),
            eq(null),
            opts.capture());
    assertEquals(11, opts.getValue().maxInflightBatches());
    assertFalse(opts.getValue().recovery());
    verify(sdk, never()).createJsonStreamInternal(any(), any(), any(), any(), any());
    verify(sdk, never()).createProtoStreamInternal(any(), any(), any(), any(), any(), any());
  }
}
