package org.stu.example.smallfiles;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.stu.utils.Base;

public class SmallFilesMapReduce extends Base{
	
	public static void main(String[] args) throws Exception {
		args = new String[] {
			"/tmp/files.avro",
			"/tmp/fileinfo"
		};
		int res = ToolRunner.run(new Configuration(), new SmallFilesMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputPath = args[0];
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf);
		job.setJarByClass(SmallFilesMapReduce.class);
		
		job.set(AvroJob.INPUT_SCHEMA, SmallFilesWrite.SCHEMA.toString());
		
		job.setInputFormat(AvroInputFormat.class);
		
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setMapperClass(MapperClass.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(0);
		
		return JobClient.runJob(job).isSuccessful() ? 0 : 1;
	}
	
	public static class MapperClass 
		implements Mapper<AvroWrapper<GenericRecord>, NullWritable, Text, Text>{

		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		public void map(AvroWrapper<GenericRecord> key,
						 NullWritable value,
						 OutputCollector<Text, Text> output, 
						 Reporter reporter) throws IOException {
			GenericRecord r = key.datum();
			outKey.set(r.get(SmallFilesWrite.FIELD_FILENAME).toString());
			ByteBuffer byteBuffer = (ByteBuffer)r.get(SmallFilesWrite.FIELD_CONTENTS);
			byte[] data = new byte[byteBuffer.remaining()];
			byteBuffer.get(data, 0, byteBuffer.remaining());
			outValue.set(DigestUtils.md5Hex(data));
			output.collect(outKey, outValue);
		}
		
		@Override
		public void configure(JobConf job) {			
		}

		@Override
		public void close() throws IOException {			
		}		
	}
}
