/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.coder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.nemo.common.DirectByteArrayOutputStream;
import org.apache.nemo.common.punctuation.WatermarkWithIndex;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link DecoderFactory} which is used for an array of bytes.
 */
public final class BytesDecoderFactory implements DecoderFactory<byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(BytesDecoderFactory.class.getName());

  private static final BytesDecoderFactory BYTES_DECODER_FACTORY = new BytesDecoderFactory();

  /**
   * A private constructor.
   */
  private BytesDecoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the decoder.
   * @return the initializer.
   */
  public static BytesDecoderFactory of() {
    return BYTES_DECODER_FACTORY;
  }

  @Override
  public Decoder<byte[]> create(final InputStream inputStream) {
    return new BytesDecoder(inputStream);
  }

  @Override
  public String toString() {
    return "BytesDecoderFactory{}";
  }

  public Object decode(final ByteBuf byteBuf) throws Exception {
    final ByteBufInputStream inputStream = new ByteBufInputStream(byteBuf);

    final byte isWatermark = (byte) inputStream.read();
    if (isWatermark == -1) {
      // end of the input stream
      throw new EOFException();
    }

    final DataInputStream dis = new DataInputStream(inputStream);

    if (isWatermark == 0x00) {
      // this is not a watermark
      final long timestamp = dis.readLong();
      //LOG.info("Decode {}", value);
      return new TimestampAndValue<>(timestamp, byteBuf);
    } else if (isWatermark == 0x01) {
      // this is a watermark
      final WatermarkWithIndex r = WatermarkWithIndex.decode(dis);
      byteBuf.release();
      return r;
    } else if (isWatermark == 0x02) {
      final Watermark w = Watermark.decode(dis);
      byteBuf.release();
      return w;
    } else {
      throw new RuntimeException("Watermark decoding failure: " + isWatermark);
    }
  }

  /**
   * BytesDecoder.
   */
  private final class BytesDecoder implements Decoder<byte[]> {

    private final InputStream inputStream;
    private boolean returnedArray;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
     */
    private BytesDecoder(final InputStream inputStream) {
      this.inputStream = inputStream;
      this.returnedArray = false;
    }

    @Override
    public byte[] decode() throws IOException {
      // We cannot use inputStream.available() to know the length of bytes to read.
      // The available method only returns the number of bytes can be read without blocking.
      final DirectByteArrayOutputStream byteOutputStream = new DirectByteArrayOutputStream();
      int b = inputStream.read();
      while (b != -1) {
        byteOutputStream.write(b);
        b = inputStream.read();
      }

      final int lengthToRead = byteOutputStream.getCount();
      if (lengthToRead == 0) {
        if (!returnedArray) {
          returnedArray = true;
          return new byte[0];
        } else {
          throw new EOFException("EoF (empty partition)!"); // TODO #120: use EOF exception instead of IOException.
        }
      }
      final byte[] resultBytes = new byte[lengthToRead]; // Read the size of this byte array.
      System.arraycopy(byteOutputStream.getBufDirectly(), 0, resultBytes, 0, lengthToRead);

      returnedArray = true;
      return resultBytes;
    }
  }
}
