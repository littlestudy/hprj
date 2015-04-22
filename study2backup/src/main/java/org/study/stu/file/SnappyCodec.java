package org.study.stu.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.study.stu.utils.DataFileConstants;
import org.xerial.snappy.Snappy;

/** * Implements Snappy compression and decompression. */
public class SnappyCodec extends Codec {
  private CRC32 crc32 = new CRC32();

  public static final SnappyCodec INSTANCE = new SnappyCodec();
  
  public static class Option extends CodecFactory {
    @Override
    protected Codec createInstance() {
      return new SnappyCodec();
    }
  }

  private SnappyCodec() {}

  @Override public String getName() { return DataFileConstants.SNAPPY_CODEC; }

  @Override
  public ByteBuffer compress(ByteBuffer in) throws IOException {
    ByteBuffer out =
      ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining())+4);
    int size = Snappy.compress(in.array(), in.position(), in.remaining(),
                               out.array(), 0);
    crc32.reset();
    crc32.update(in.array(), in.position(), in.remaining());
    out.putInt(size, (int)crc32.getValue());

    out.limit(size+4);

    return out;
  }

  @Override
  public ByteBuffer decompress(ByteBuffer in) throws IOException {
    ByteBuffer out = ByteBuffer.allocate
      (Snappy.uncompressedLength(in.array(),in.position(),in.remaining()-4));
    int size = Snappy.uncompress(in.array(),in.position(),in.remaining()-4,
                                 out.array(), 0);
    out.limit(size);
    
    crc32.reset();
    crc32.update(out.array(), 0, size);
    if (in.getInt(in.limit()-4) != (int)crc32.getValue())
      throw new IOException("Checksum failure");
    
    return out;
  }
  
  @Override public int hashCode() { return getName().hashCode(); }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    return true;
  }

}
