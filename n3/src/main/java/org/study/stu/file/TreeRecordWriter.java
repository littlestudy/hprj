package org.study.stu.file;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.io.IOUtils;

public class TreeRecordWriter {
	private DataFileWriter<Object> writer;
	
	public TreeRecordWriter(){
		this.writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
	}
	
	public TreeRecordWriter setSyncInterval(int syncInterval){
		writer.setSyncInterval(syncInterval);
		return this;
	}
	
	public TreeRecordWriter setCodec(CodecFactory c){
		writer.setCodec(c);
		return this;
	}
	
	public TreeRecordWriter create(Schema schema, OutputStream outs) throws IOException{
		writer.create(schema, outs);
		return this;
	}
	
	public void append(Object obj) throws IOException{
		writer.append(obj);
	}
	
	public void cleanup(){
		IOUtils.cleanup(null, writer);
		writer = null;
	}
}
