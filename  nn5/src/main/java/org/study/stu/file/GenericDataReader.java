package org.study.stu.file;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.file.Codec;
import org.apache.avro.file.SeekableInput;
import org.study.stu.StuRuntimeException;
import org.study.stu.common.DataBlock;
import org.study.stu.utils.DataFileConstants;

public class GenericDataReader implements Iterator<DataBlock>, Iterable<DataBlock>, Closeable{
	
	private SeekableInputStream sin;
	private DataInput dataInput;
	private long blockStart;
	private DataBlock dataBlock;	
	private Header header;
	private Codec codec;
	byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];
	private boolean isTaken = true;
	
	public static final class Header {
		Map<String,byte[]> meta = new HashMap<String,byte[]>();    
		byte[] sync = new byte[DataFileConstants.SYNC_SIZE];
		private Header() {}
	}
	
	public static GenericDataReader openReader(SeekableInputStream in) throws IOException{
		if (in.length() <DataFileConstants.MAGIC.length)
			throw new IOException("Not an Avro data file");
		byte[] magic = new byte[DataFileConstants.MAGIC.length];
		in.seek(0);
		for (int c = 0; c < magic.length; c = in.read(magic, c, magic.length-c)) {}    
		in.seek(0);
		
		if (Arrays.equals(DataFileConstants.MAGIC, magic))             
			return new GenericDataReader(in);
		
		throw new IOException("Not an Avro data file");
	}
	
	public GenericDataReader(SeekableInputStream sin) throws IOException{
		this.sin = new SeekableInputStream(sin);
		init(sin);
		blockStart = sin.tell();
	}

	private void init(SeekableInputStream sin) throws IOException,
			UnsupportedEncodingException {
		this.header = new Header();
		byte[] magic = new byte[DataFileConstants.MAGIC.length];
		sin.read(magic);
		
		if (!Arrays.equals(DataFileConstants.MAGIC, magic))
			throw new IOException("Not a data file.");
		
		dataInput = new DataInputStream(sin);
		int mapSize = dataInput.readInt();
		for (int i = 0; i < mapSize; i++){
			String key = dataInput.readUTF();
			int len = dataInput.readInt();
			byte[] value = new byte[len];
			dataInput.readFully(value);
			header.meta.put(key, value);
		}
		dataInput.readFully(header.sync);	
		
		String codecStr = new String(header.meta.get(DataFileConstants.CODEC), "UTF-8");
		if (codecStr.equals(DataFileConstants.NULL_CODEC)) {
			this.codec = NullCodec.INSTANCE;
		} else {
			this.codec = SnappyCodec.INSTANCE;
		}
	}
	
	public void seek(long position) throws IOException {
		sin.seek(position);
		blockStart = position;
	}
	
	public void sync(long position) throws IOException {
		seek(position);
		
		if (position == 0) {
			init(sin);                            // re-init to skip header
			return;
		}
		try {
			int i=0, b;
			sin.read(syncBuffer);
			do {
				int j = 0;
				for (; j < DataFileConstants.SYNC_SIZE; j++) {
					if (header.sync[j] != syncBuffer[(i+j)%DataFileConstants.SYNC_SIZE])
						break;
				}
				if (j == DataFileConstants.SYNC_SIZE) {                       // matched a complete sync
					blockStart = position + i + DataFileConstants.SYNC_SIZE;
					return;
				}
				b = sin.read();
				syncBuffer[i++%DataFileConstants.SYNC_SIZE] = (byte)b;
			} while (b != -1);
		} catch (EOFException e) {
      // fall through
		}
    // if no match or EOF set start to the end position
		blockStart = sin.tell();
    //System.out.println("block start location after EOF: " + blockStart );
		return;
	}	

	public boolean pastSync(long position) throws IOException {
		return ((blockStart >= position+DataFileConstants.SYNC_SIZE)||(blockStart >= sin.length()));
	}

	@Override
	public Iterator<DataBlock> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		if (!isTaken)
			return dataBlock.isAvailable();
		try {
			dataBlock = new DataBlock();
			dataBlock.readFields(dataInput);
			dataInput.readFully(syncBuffer);
			if (!Arrays.equals(syncBuffer, header.sync))
				throw new IOException("Invalid sync!");
			isTaken = false;
			if (!dataBlock.isAvailable())
				throw new IOException("DataBlock is broken!");
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public DataBlock next() {
		if (!isTaken){
			isTaken = true;
			try {
				dataBlock.decompressUsing(codec);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return dataBlock;	
		} else 
			throw new StuRuntimeException("Call hasNext() first.");
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();		
	}
	
		
	@Override
	public void close() throws IOException {
		sin.close();
	}
	
	static class SeekableInputStream extends InputStream implements SeekableInput {
    
		private final byte[] oneByte = new byte[1];
		private SeekableInput in;

	   SeekableInputStream(SeekableInput in) throws IOException {
		   this.in = in;
	   }
    
	   @Override
	   public void seek(long p) throws IOException {
		   if (p < 0)
			   throw new IOException("Illegal seek: " + p);
		   in.seek(p);
	   }

	   @Override
	   public long tell() throws IOException {
		   return in.tell();
	   }

	   @Override
	   public long length() throws IOException {
		   return in.length();
	   }

	   @Override
	   public int read(byte[] b) throws IOException {
		   return in.read(b, 0, b.length);
	   }
    
	   @Override
	   public int read(byte[] b, int off, int len) throws IOException {
		   return in.read(b, off, len);
	   }

	   @Override
	   public int read() throws IOException {
		   int n = read(oneByte, 0, 1);
		   if (n == 1) {
			   return oneByte[0] & 0xff;
		   } else {
			   return n;
		   }
	   }

	   @Override
	   public long skip(long skip) throws IOException {
		   long position = in.tell();
		   long length = in.length();
		   long remaining = length - position;
		   if (remaining > skip) {
			   in.seek(skip);
			   return in.tell() - position;
		   } else {
			   in.seek(remaining);
			   return in.tell() - position;
		   }
	   }

	   @Override
	   public void close() throws IOException {
		   in.close();
		   super.close();
	   }

	   @Override
	   public int available() throws IOException {
		   long remaining = (in.length() - in.tell());
		   return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE
				   : (int) remaining;
	   }
	}

}
