package org.study.stu.common;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TreeRecordRead extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		args = new String[] {
				"/home/hadoop/tmp/test.avro"
		};
		int res = ToolRunner.run(new Configuration(), new TreeRecordRead(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(getConf());
		
		System.out.println("fs: " + hdfs.getClass());
		
		Path destFile = new Path(args[0]);
		
		InputStream is = hdfs.open(destFile);
		readFromAvro(is);
		
		return 0;
	}
	
	public static void readFromAvro(InputStream is) throws IOException{
		DataFileStream<Object> reader = new DataFileStream<Object>(is, new GenericDatumReader<Object>());
		for (Object o : reader){
			GenericRecord r = (GenericRecord) o;
			
			ByteBuffer groupBundleBytes = (ByteBuffer)r.get(DataBlock.GROUP_BUNDLE);
						
			RestoreInMemoryWithDict rimw = new RestoreInMemoryWithDict(groupBundleBytes);
			
			int dictAmount = (int) r.get(DataBlock.DICTIONARY_AMOUNT);
			System.out.println(DataBlock.DICTIONARY_AMOUNT + ": " + dictAmount);			
			for (int i = 0; i < dictAmount; i++){
				rimw.setDictFromByteBuffer(i, (ByteBuffer)r.get(DataBlock.DICTIONARY + i));				
			}			
			
			int recordAmount = (int) r.get(DataBlock.RECORD_AMOUNT);
			ByteBuffer records = (ByteBuffer)r.get(DataBlock.RECORDS);
			byte[] recordsBytes = new byte[records.remaining()];
			records.get(recordsBytes, 0, records.remaining());
			ByteArrayInputStream bais = new ByteArrayInputStream(recordsBytes);
			DataInput dataInput = new DataInputStream(bais);
			for (int k = 0; k < recordAmount; k++){
				String tree = dataInput.readUTF();
				System.out.println(tree);
				for (String str : rimw.restoreInMemory(tree))
					System.out.println(str);
			}
				
			System.out.println("---------------------------------------------");
		}
		IOUtils.cleanup(null, is);
		IOUtils.cleanup(null, reader);
	}
}
