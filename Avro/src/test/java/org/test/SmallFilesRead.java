package org.test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.stu.utils.Base;

public class SmallFilesRead extends Base{
	
	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://master:9000/tmp/files.avro"
		};
		int res = ToolRunner.run(new Configuration(), new SmallFilesRead(), args);
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

	private static final String FIELD_FILENAME = "filename";
	private static final String FIELD_CONTENTS = "contents";
	
	public static void readFromAvro(InputStream is) throws IOException{
		DataFileStream<Object> reader = 
				new DataFileStream<Object>(is, new GenericDatumReader<Object>());
		for (Object o : reader){
			GenericRecord r = (GenericRecord) o;
			ByteBuffer byteBuffer = (ByteBuffer)r.get(FIELD_CONTENTS);
			byte[] data = new byte[byteBuffer.remaining()];
			byteBuffer.get(data, 0, byteBuffer.remaining());
			System.out.println(
					r.get(FIELD_FILENAME)
					+ ": "
					+ DigestUtils.md5Hex(data)
					);
		}
		IOUtils.cleanup(null, is);
		IOUtils.cleanup(null, reader);
	}
}
