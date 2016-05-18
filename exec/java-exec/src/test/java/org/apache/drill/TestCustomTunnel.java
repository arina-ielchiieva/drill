/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.internal.ThreadLocalRandom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.UserRpcException;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.control.ControlTunnel.CustomFuture;
import org.apache.drill.exec.rpc.control.ControlTunnel.CustomTunnel;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.Controller.CustomMessageHandler;
import org.apache.drill.exec.rpc.control.Controller.CustomResponse;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.Test;

public class TestCustomTunnel extends BaseTestQuery {

  public static final Controller.CustomSerDe<String> REQUEST_SERDE = new Controller.CustomSerDe<String>() {
    @Override
    public byte[] serializeToSend(String send) {
      return send.getBytes();
    }

    @Override
    public String deserializeReceived(byte[] bytes) throws Exception {
      return new String(bytes);
    }
  };

  public static final Controller.CustomSerDe<List<String>> RESPONSE_SERDE = new Controller.CustomSerDe<List<String>>() {
    @Override
    public byte[] serializeToSend(List<String> send) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      for (String element : send) {
        try {
          out.writeUTF(element);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return baos.toByteArray();
    }

    @Override
    public List<String> deserializeReceived(byte[] bytes) throws Exception {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      DataInputStream in = new DataInputStream(bais);
      List<String> list = Lists.newLinkedList();
      while (in.available() > 0) {
        String element = in.readUTF();
        list.add(element);
      }
      return list;
    }
  };

  private final QueryId expectedId = QueryId
      .newBuilder()
      .setPart1(ThreadLocalRandom.current().nextLong())
      .setPart2(ThreadLocalRandom.current().nextLong())
      .build();

  private final ByteBuf buf1;
  private final byte[] expected;

  public TestCustomTunnel() throws IOException {
    buf1 = UnpooledByteBufAllocator.DEFAULT.buffer(1024);
    Random r = new Random();
    this.expected = new byte[1024];
    r.nextBytes(expected);
    buf1.writeBytes(expected);
  }

  @Test
  public void ensureRoundTrip() throws Exception {

    final DrillbitContext context = getDrillbitContext();
    final TestCustomMessageHandler handler = new TestCustomMessageHandler(context.getEndpoint(), false);
    context.getController().registerCustomHandler(1001, handler, DrillbitEndpoint.PARSER);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<DrillbitEndpoint, QueryId> tunnel = loopbackTunnel.getCustomTunnel(1001, DrillbitEndpoint.class,
        QueryId.PARSER);
    CustomFuture<QueryId> future = tunnel.send(context.getEndpoint());
    assertEquals(expectedId, future.get());
  }

  @Test
  public void getLogs() throws Exception {
    final DrillbitContext context = getDrillbitContext();
    LogMessageHandler logMessageHandler = new LogMessageHandler();
    context.getController().registerCustomHandler(10, logMessageHandler, REQUEST_SERDE, RESPONSE_SERDE);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<String, List<String>> tunnel = loopbackTunnel.getCustomTunnel(10, REQUEST_SERDE, RESPONSE_SERDE);
    CustomFuture<List<String>> future = tunnel.send("Get logs");
    System.out.println(future.get());
  }

  @Test
  public void copyJarsToTmpFolder() throws Exception {
    // crete ByteBuf with bytes from our jar
    // File jar = new File("/home/osboxes/projects/DrillUDF/target/DrillUDF-1.0.jar");
    Path jar = Paths.get("/home/osboxes/git_repo/drillUDF/target/DrillUDF-1.0.jar");
    byte[] bytes = Files.readAllBytes(jar);
    ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.length);
    byteBuf.writeBytes(bytes);
    byteBuf.retain();

    CopyJarTransferHandler copyJarTransferHandler = new CopyJarTransferHandler(getDrillbitContext());
    final DrillbitContext context = getDrillbitContext();
    context.getController().registerCustomHandler(1, copyJarTransferHandler, REQUEST_SERDE, RESPONSE_SERDE);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<String, List<String>> tunnel = loopbackTunnel.getCustomTunnel(1, REQUEST_SERDE, RESPONSE_SERDE);
    CustomFuture<List<String>> future = tunnel.send("DrillUDF-1.0.jar", byteBuf);
    System.out.println(future.get());
  }

  public static class CopyJarTransferHandler implements CustomMessageHandler<String, List<String>> {

    private final DrillbitContext context;

    public CopyJarTransferHandler(DrillbitContext context) {
      this.context = context;
    }

    @Override
    public CustomResponse<List<String>> onMessage(String pBody, DrillBuf dBody) throws UserRpcException {
      // get data from context
        System.out.println(context.getEndpoint().toString());
      //
        System.out.println("File name to create - " + pBody);
        // convert incoming byte buffer to byte array
        byte[] bytes = new byte[dBody.capacity()];
        dBody.getBytes(0, bytes);
        // create file with incoming content
        Path destination = Paths.get("/home/osboxes/files/" + pBody);
        try {
          Files.write(destination, bytes);
        } catch (IOException e) {
          e.printStackTrace();
        }

      // return list of files in temp directory

      File folder = new File("/home/osboxes/files");
      File[] files = folder.listFiles(new FileFilter() {
        public boolean accept(File file) {
          return file.isFile();
        }
      });
      final List<String> fileNames = Lists.newLinkedList();
      for (File file : files) {
        fileNames.add(file.getName());
      }
      return new CustomResponse<List<String>>() {
        @Override
        public List<String> getMessage() {
          return fileNames;
        }

        @Override
        public ByteBuf[] getBodies() {
          return null;
        }
      };
    }
  }

  public static class LogMessageHandler implements CustomMessageHandler<String, List<String>> {

    @Override
    public CustomResponse<List<String>> onMessage(String pBody, DrillBuf dBody) throws UserRpcException {
      System.out.println(pBody);
      File folder = new File("/home/osboxes/projects/DrillUDF/target");
      File[] files = folder.listFiles(new FileFilter() {
        public boolean accept(File file) {
          return file.isFile();
        }
      });
      final List<String> fileNames = Lists.newLinkedList();
      for (File file : files) {
        fileNames.add(file.getName());
      }
      return new CustomResponse<List<String>>() {
        @Override
        public List<String> getMessage() {
          return fileNames;
        }

        @Override
        public ByteBuf[] getBodies() {
          return null;
        }
      };
    }
  }

  @Test
  public void ensureRoundTripBytes() throws Exception {
    final DrillbitContext context = getDrillbitContext();
    final TestCustomMessageHandler handler = new TestCustomMessageHandler(context.getEndpoint(), true);
    context.getController().registerCustomHandler(1002, handler, DrillbitEndpoint.PARSER);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<DrillbitEndpoint, QueryId> tunnel = loopbackTunnel.getCustomTunnel(1002, DrillbitEndpoint.class,
        QueryId.PARSER);
    buf1.retain();
    CustomFuture<QueryId> future = tunnel.send(context.getEndpoint(), buf1);
    assertEquals(expectedId, future.get());
    byte[] actual = new byte[1024];
    future.getBuffer().getBytes(0, actual);
    future.getBuffer().release();
    assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void ensureRoundTripBytesForJar() throws Exception {
    final DrillbitContext context = getDrillbitContext();
    final TestCustomMessageHandlerForJar handler = new TestCustomMessageHandlerForJar(context.getEndpoint(), true);
    context.getController().registerCustomHandler(1002, handler, DrillbitEndpoint.PARSER);
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<DrillbitEndpoint, QueryId> tunnel = loopbackTunnel.getCustomTunnel(1002, DrillbitEndpoint.class,
        QueryId.PARSER);
    //buf1.retain();
    Path jar = Paths.get("/home/osboxes/projects/DrillUDF/target/DrillUDF-1.0.jar");
/*    byte[] bytes = Files.readAllBytes(jar);
    ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.length);
    byteBuf.writeBytes(byteBuf);
    byteBuf.retain();*/

    Random r = new Random();
    byte[] bytes = new byte[1024];
    r.nextBytes(bytes);
    ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(bytes.length);
    byteBuf.writeBytes(byteBuf);
    byteBuf.retain();
    //CustomFuture<QueryId> future = tunnel.send(context.getEndpoint(), buf1);
    CustomFuture<QueryId> future = tunnel.send(context.getEndpoint(), byteBuf);
    assertEquals(expectedId, future.get());
/*    byte[] actual = new byte[1024];
    future.getBuffer().getBytes(0, actual);
    future.getBuffer().release();
    assertTrue(Arrays.equals(expected, actual));*/
  }



  private class TestCustomMessageHandler implements CustomMessageHandler<DrillbitEndpoint, QueryId> {
    private DrillbitEndpoint expectedValue;
    private final boolean returnBytes;

    public TestCustomMessageHandler(DrillbitEndpoint expectedValue, boolean returnBytes) {
      super();
      this.expectedValue = expectedValue;
      this.returnBytes = returnBytes;
    }

    @Override
    public CustomResponse<QueryId> onMessage(DrillbitEndpoint pBody, DrillBuf dBody) throws UserRpcException {

      if (!expectedValue.equals(pBody)) {
        throw new UserRpcException(expectedValue, "Invalid expected downstream value.", new IllegalStateException());
      }

      if (returnBytes) {
        byte[] actual = new byte[1024];
        dBody.getBytes(0, actual);
        if (!Arrays.equals(expected, actual)) {
          throw new UserRpcException(expectedValue, "Invalid expected downstream value.", new IllegalStateException());
        }
      }

      return new CustomResponse<QueryId>() {

        @Override
        public QueryId getMessage() {
          return expectedId;
        }

        @Override
        public ByteBuf[] getBodies() {
          if (returnBytes) {
            buf1.retain();
            return new ByteBuf[] { buf1 };
          } else {
            return null;
          }
        }

      };
    }
  }

  private class TestCustomMessageHandlerForJar implements CustomMessageHandler<DrillbitEndpoint, QueryId> {
    private DrillbitEndpoint expectedValue;
    private final boolean returnBytes;

    public TestCustomMessageHandlerForJar(DrillbitEndpoint expectedValue, boolean returnBytes) {
      super();
      this.expectedValue = expectedValue;
      this.returnBytes = returnBytes;
    }

    @Override
    public CustomResponse<QueryId> onMessage(DrillbitEndpoint pBody, DrillBuf dBody) throws UserRpcException {

      byte[] bytes = new byte[dBody.capacity()];
      dBody.getBytes(0, bytes);
      // create file with incoming content
      Path destination = Paths.get("/home/osboxes/files/" + pBody);
      try {
        Files.write(destination, bytes);
      } catch (IOException e) {
        e.printStackTrace();
      }


/*      if (!expectedValue.equals(pBody)) {
        throw new UserRpcException(expectedValue, "Invalid expected downstream value.", new IllegalStateException());
      }

      if (returnBytes) {
        byte[] actual = new byte[1024];
        dBody.getBytes(0, actual);
        if (!Arrays.equals(expected, actual)) {
          throw new UserRpcException(expectedValue, "Invalid expected downstream value.", new IllegalStateException());
        }
      }*/

      return new CustomResponse<QueryId>() {

        @Override
        public QueryId getMessage() {
          return expectedId;
        }

        @Override
        public ByteBuf[] getBodies() {
          if (returnBytes) {
            buf1.retain();
            return new ByteBuf[] { buf1 };
          } else {
            return null;
          }
        }

      };
    }
  }

  @Test
  public void ensureRoundTripJackson() throws Exception {
    final DrillbitContext context = getDrillbitContext();
    final MesgA mesgA = new MesgA();
    mesgA.fieldA = "123";
    mesgA.fieldB = "okra";

    final TestCustomMessageHandlerJackson handler = new TestCustomMessageHandlerJackson(mesgA);
    context.getController().registerCustomHandler(1003, handler,
        new ControlTunnel.JacksonSerDe<MesgA>(MesgA.class),
        new ControlTunnel.JacksonSerDe<MesgB>(MesgB.class));
    final ControlTunnel loopbackTunnel = context.getController().getTunnel(context.getEndpoint());
    final CustomTunnel<MesgA, MesgB> tunnel = loopbackTunnel.getCustomTunnel(
        1003,
        new ControlTunnel.JacksonSerDe<MesgA>(MesgA.class),
        new ControlTunnel.JacksonSerDe<MesgB>(MesgB.class));
    CustomFuture<MesgB> future = tunnel.send(mesgA);
    assertEquals(expectedB, future.get());
  }

  private MesgB expectedB = new MesgB().set("hello", "bye", "friend");

  public static class MesgA {
    public String fieldA;
    public String fieldB;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((fieldA == null) ? 0 : fieldA.hashCode());
      result = prime * result + ((fieldB == null) ? 0 : fieldB.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MesgA other = (MesgA) obj;
      if (fieldA == null) {
        if (other.fieldA != null) {
          return false;
        }
      } else if (!fieldA.equals(other.fieldA)) {
        return false;
      }
      if (fieldB == null) {
        if (other.fieldB != null) {
          return false;
        }
      } else if (!fieldB.equals(other.fieldB)) {
        return false;
      }
      return true;
    }

  }

  public static class MesgB {
    public String fieldA;
    public String fieldB;
    public String fieldC;

    public MesgB set(String a, String b, String c) {
      fieldA = a;
      fieldB = b;
      fieldC = c;
      return this;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((fieldA == null) ? 0 : fieldA.hashCode());
      result = prime * result + ((fieldB == null) ? 0 : fieldB.hashCode());
      result = prime * result + ((fieldC == null) ? 0 : fieldC.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MesgB other = (MesgB) obj;
      if (fieldA == null) {
        if (other.fieldA != null) {
          return false;
        }
      } else if (!fieldA.equals(other.fieldA)) {
        return false;
      }
      if (fieldB == null) {
        if (other.fieldB != null) {
          return false;
        }
      } else if (!fieldB.equals(other.fieldB)) {
        return false;
      }
      if (fieldC == null) {
        if (other.fieldC != null) {
          return false;
        }
      } else if (!fieldC.equals(other.fieldC)) {
        return false;
      }
      return true;
    }

  }

  private class TestCustomMessageHandlerJackson implements CustomMessageHandler<MesgA, MesgB> {
    private MesgA expectedValue;

    public TestCustomMessageHandlerJackson(MesgA expectedValue) {
      super();
      this.expectedValue = expectedValue;
    }

    @Override
    public CustomResponse<MesgB> onMessage(MesgA pBody, DrillBuf dBody) throws UserRpcException {

      if (!expectedValue.equals(pBody)) {
        throw new UserRpcException(DrillbitEndpoint.getDefaultInstance(),
            "Invalid expected downstream value.", new IllegalStateException());
      }

      return new CustomResponse<MesgB>() {

        @Override
        public MesgB getMessage() {
          return expectedB;
        }

        @Override
        public ByteBuf[] getBodies() {
          return null;
        }

      };
    }
  }
}
