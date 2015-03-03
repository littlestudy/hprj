package org.study.stu.file;

import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.study.stu.stu.DataBlock;
import org.study.stu.stu.GroupBundle;
import org.study.stu.utils.MergeInMemoryWithDict;
import org.study.stu.utils.dataconvert.BaseDataConvert;
import org.study.stu.utils.dataconvert.impl.JsonToCsvConvert;

public abstract class AvroFileConverterBase {
	private DataFileWriter<Object> writer;
	private int dataInterval;
	private GroupBundle groupBundle;
	
	public AvroFileConverterBase(GroupBundle groupBundle){	
		this(1000, 3, groupBundle);
	}
	
	@SuppressWarnings("resource")
	public AvroFileConverterBase(int syncInterval, int dataInterval, GroupBundle groupBundle){
		writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>()).setSyncInterval(syncInterval);
		this.dataInterval = dataInterval;
		this.groupBundle = groupBundle;
	}
	
	public abstract String createRecordJsonSchema();
	
	public abstract GenericRecord convertBlockToAvroRecord(DataBlock dataBlock);
	
	public AvroFileConverterBase setCodec(CodecFactory c){
		writer.setCodec(c);
		return this;
	}
	
	public void writeToAvroFile(String inputFile, OutputStream outputStream) throws IOException{
		Schema schema = new Schema.Parser().parse(createRecordJsonSchema());
		writer.create(schema, outputStream);

		MergeInMemoryWithDict mimw = new MergeInMemoryWithDict(groupBundle);
		BaseDataConvert dataCovert = new JsonToCsvConvert(groupBundle);		
		LineIterator iter = IOUtils.lineIterator(new FileReader(inputFile));
		List<String> datas = null;
		DataBlock dataBlock = null;
		while(iter.hasNext()){
			datas = new ArrayList<String>(dataInterval);
			int i = 0;
			for (i = 0; i < dataInterval && iter.hasNext(); i++){
				datas.add(dataCovert.dataFormat(iter.next()));
			}
			dataBlock = mimw.mergeInMemory(datas.iterator(), dataInterval);
			writer.append(convertBlockToAvroRecord(dataBlock));	
			
			org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			org.apache.hadoop.io.IOUtils.cleanup(null, outputStream);
		}
	}	
}
