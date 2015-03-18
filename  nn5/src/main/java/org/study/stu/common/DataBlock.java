package org.study.stu.common;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.file.Codec;
import org.apache.hadoop.io.Writable;
import org.study.stu.common.dict.CDictionaryBundle;

public class DataBlock implements Writable{

	public static final DataBlock EndBlock = new DataBlock(-1);
	
	private byte[] data;
	private int blockSize;
	private final int offset = 0;
	
	public DataBlock(String block) throws UnsupportedEncodingException{
		data = block.getBytes("UTF-8");
		blockSize = data.length;
	}
	
	public DataBlock(){		
	}
	
	public DataBlock(int size){
		if (size < 0)
			this.blockSize = size;
	}
		
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(blockSize);
		if (blockSize >= 0)
			out.write(data, offset, blockSize);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		blockSize = in.readInt();
		if (blockSize < 0)
			return;
		data = new byte[blockSize];
		in.readFully(data);		
	}
	
	public boolean isAvailable(){
		return blockSize >= 0;
	}
	
	public void compressUsing(Codec c) throws IOException {
		if (blockSize < 0)
			return ;
		ByteBuffer result = c.compress(getAsByteBuffer());
		data = result.array();
		blockSize = result.remaining();
    }
    
	public void decompressUsing(Codec c) throws IOException {
		if (blockSize < 0)
			return ;
		ByteBuffer result = c.decompress(getAsByteBuffer());
		data = result.array();
		blockSize = result.remaining();
	}
	
	ByteBuffer getAsByteBuffer() {
		return ByteBuffer.wrap(data, offset, blockSize);
    }
	
	@Override
	public String toString() {
		return new String(data);
	}
	
	public int getBlockSize(){
		return this.blockSize;
	}
	
	/*
	private byte[] data;
	private int blockSize;
	private final int offset = 0;
	
	public DataBlock(CDictionaryBundle dictionaryBundle, List<TreeRecord> records) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);
		dictionaryBundle.writeAllDicts(dataOutput);
		
		dataOutput.writeInt(records.size());
		for (TreeRecord treeRecord : records){
			byte[] treeBytes = treeRecord.getTree().getBytes("UTF-8");
			dataOutput.write(treeBytes, 0, treeBytes.length);
		}
		data = baos.toByteArray();
		blockSize = data.length;
	}
	
	public DataBlock(ByteBuffer block) {
		this.blockSize = block.remaining();
		this.data = new byte[blockSize];
		block.get(data, offset, blockSize);
    }
	
	ByteBuffer getAsByteBuffer() {
		return ByteBuffer.wrap(data, offset, blockSize);
    }
	
	public void compressUsing(Codec c) throws IOException {
		ByteBuffer result = c.compress(getAsByteBuffer());
		data = result.array();
		blockSize = result.remaining();
    }
    
	public void decompressUsing(Codec c) throws IOException {
		ByteBuffer result = c.decompress(getAsByteBuffer());
		data = result.array();
		blockSize = result.remaining();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.blockSize);
		out.write(data, offset, blockSize);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}   
	*/
}
