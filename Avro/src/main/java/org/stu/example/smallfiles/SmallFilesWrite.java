package org.stu.example.smallfiles;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.stu.utils.Base;

public class SmallFilesWrite extends Base{

	public static void main(String[] args) throws Exception {
		args = new String [] {
				"/tmp/hadoop",
				"/tmp/files.avro"
		};
		int res = ToolRunner.run(new Configuration(), new SmallFilesWrite(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(getConf());
		
		File sourceDir = new File(args[0]);
		Path destFile = new Path(args[1]);
		
		OutputStream os = hdfs.create(destFile);
		writeToAvro(sourceDir, os);
		
		return 0;
	}
	
	public static final String FIELD_FILENAME = "filename";
	public static final String FIELD_CONTENTS = "contents";
	private static final String SCHEMA_JSON =
			"{\"type\": \"record\", \"name\": \"SmallFilesTest\", "
          + "\"fields\": ["
          + "{\"name\":\"" + FIELD_FILENAME
          + "\", \"type\":\"string\"},"
          + "{\"name\":\"" + FIELD_CONTENTS
          + "\", \"type\":\"bytes\"}]}";
	@SuppressWarnings("deprecation")
	public static final Schema SCHEMA = Schema.parse(SCHEMA_JSON);
	
	@SuppressWarnings("resource")
	public static void writeToAvro(File srcPath, OutputStream outputStream) throws IOException{
		DataFileWriter<Object> writer
				= new DataFileWriter<Object>(new GenericDatumWriter<Object>()).setSyncInterval(100);
		writer.setCodec(CodecFactory.snappyCodec());
		writer.create(SCHEMA, outputStream);
		for (Object obj : FileUtils.listFiles(srcPath, null, false)){
			File file = (File) obj;
			String filename = file.getAbsolutePath();
			byte[] content = FileUtils.readFileToByteArray(file);
			GenericRecord record = new GenericData.Record(SCHEMA);
			record.put(FIELD_FILENAME, filename);
			record.put(FIELD_CONTENTS, ByteBuffer.wrap(content));
			writer.append(record);
			System.out.println(
					file.getAbsolutePath()
					+ ": " 
					+ DigestUtils.md5Hex(content));
		}
		IOUtils.cleanup(null, writer);
		IOUtils.cleanup(null, outputStream);
	}	
}
