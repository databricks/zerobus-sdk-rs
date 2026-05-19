package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the fire-and-forget ingestion API surface. */
public class NoWaitApiTest {

  @Test
  void protoStreamExposesNoWaitOverloads() throws Exception {
    assertVoidMethod(ZerobusProtoStream.class.getMethod("ingestRecordNoWait", Message.class));
    assertVoidMethod(ZerobusProtoStream.class.getMethod("ingestRecordNoWait", byte[].class));
    assertVoidMethod(ZerobusProtoStream.class.getMethod("ingestRecordsNoWait", Iterable.class));
    assertVoidMethod(ZerobusProtoStream.class.getMethod("ingestRecordsNoWait", List.class));
  }

  @Test
  void jsonStreamExposesNoWaitOverloads() throws Exception {
    assertVoidMethod(
        ZerobusJsonStream.class.getMethod(
            "ingestRecordNoWait", Object.class, ZerobusJsonStream.JsonSerializer.class));
    assertVoidMethod(ZerobusJsonStream.class.getMethod("ingestRecordNoWait", String.class));
    assertVoidMethod(
        ZerobusJsonStream.class.getMethod(
            "ingestRecordsNoWait", Iterable.class, ZerobusJsonStream.JsonSerializer.class));
    assertVoidMethod(ZerobusJsonStream.class.getMethod("ingestRecordsNoWait", Iterable.class));
  }

  @Test
  void baseStreamDeclaresNativeNoWaitBridge() throws Exception {
    Method method =
        BaseZerobusStream.class.getDeclaredMethod(
            "nativeIngestRecordNoWait", long.class, byte[].class, boolean.class);

    assertVoidMethod(method);
    assertTrue(Modifier.isNative(method.getModifiers()));
    assertTrue(Modifier.isProtected(method.getModifiers()));
  }

  private static void assertVoidMethod(Method method) {
    assertEquals(void.class, method.getReturnType());
  }
}
