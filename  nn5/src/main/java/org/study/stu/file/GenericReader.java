package org.study.stu.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.file.Codec;
import org.apache.avro.file.SeekableInput;
import org.study.stu.common.DataBlock;
import org.study.stu.utils.DataFileConstants;

public class GenericReader implements Iterator<DataBlock>, Iterable<DataBlock>, Closeable{
	private GenericDataReader reader;
	
	private ByteBuffer blockBuffer;
	private byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];
	private Codec codec;	
	
	public GenericReader(InputStream in){
		
	}
	
	@Override
	public Iterator<DataBlock> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DataBlock next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	

}
