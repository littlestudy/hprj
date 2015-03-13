package org.study.stu.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.io.IOUtils;

public class TreeRecordRead implements Iterator<Object>, Iterable<Object>{
	
	private DataFileStream<Object> reader;
	
	public TreeRecordRead(InputStream is) throws IOException{
		reader = new DataFileStream<Object>(is, new GenericDatumReader<Object>());
	}
	
	@Override
	public Iterator<Object> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return reader.hasNext();
	}

	@Override
	public Object next() {
		return reader.next();
	}

	@Override
	public void remove() {
		reader.remove();		
	}
	
	public void cleanup(){
		IOUtils.cleanup(null, reader);
	}
}
