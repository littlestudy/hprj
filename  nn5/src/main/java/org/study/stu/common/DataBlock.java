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

	private byte[] data;
	private int blockSize;
	private final int offset = 0;
	
	public DataBlock(String block) throws UnsupportedEncodingException{
		data = block.getBytes("UTF-8");
		blockSize = data.length;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(blockSize);
		out.write(data, offset, blockSize);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		
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
	
	ByteBuffer getAsByteBuffer() {
		return ByteBuffer.wrap(data, offset, blockSize);
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
