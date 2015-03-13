package org.study.stu.file;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

public class NullCodec extends Codec {
  
  public static final NullCodec INSTANCE = new NullCodec();

  static class Option extends CodecFactory {
    @Override
    public Codec createInstance() {
      return INSTANCE;
    }
  }

  /** No options available for NullCodec. */
  public static final CodecFactory OPTION = new Option();

  @Override
  public String getName() {
    return DataFileConstants.NULL_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer buffer) throws IOException {
    return buffer;
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return data;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    return (this.getClass() == other.getClass());
  }

  @Override
  public int hashCode() {
    return 2;
  }
}
