package org.study.stu.example.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.study.stu.common.DataBlock;
import org.study.stu.common.RestoreInMemoryWithDict;

public class AvroMixedMapReduce2 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String [] {
				"/home/ym/ytmp/output/testcsvGroup/part-00000.avro",
				"/home/ym/ytmp/output/testcsvGroup2"
		};
		int res = ToolRunner.run(new Configuration(),  new AvroMixedMapReduce2(), args);
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf);
		job.setJarByClass(AvroMixedMapReduce2.class);
		
		job.set(AvroJob.INPUT_SCHEMA, DataBlock.getRecordSchema(6).toString());
				
		job.setInputFormat(AvroInputFormat.class);
				
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return JobClient.runJob(job).isSuccessful() ? 0 : 1;
	}
	
	public static class MapperClass 
		extends MapReduceBase implements Mapper<AvroWrapper<GenericRecord>, NullWritable, Text, NullWritable>{
			
		@Override
		public void map(AvroWrapper<GenericRecord> key,
						 NullWritable value,
						 OutputCollector<Text, NullWritable> output, 
						 Reporter reporter)
				throws IOException {
			GenericRecord r = key.datum();
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
				for (String str : rimw.restoreInMemory(tree)){
					System.out.println(str);
					output.collect(new Text(str), NullWritable.get());
				}
			}
			
			//System.out.println(outputKey.get() + ": " + v);
			//output.collect(outputKey, new Text(v));
		}		
	}

	public static class ReducerClass 
		extends MapReduceBase implements Reducer<Text, NullWritable, Text, NullWritable>{

		@Override
		public void reduce(Text key, 
				Iterator<NullWritable> values,
				OutputCollector<Text, NullWritable> output,
				Reporter reporter) throws IOException {
			output.collect(key, NullWritable.get());
					
		}		
	}
}
