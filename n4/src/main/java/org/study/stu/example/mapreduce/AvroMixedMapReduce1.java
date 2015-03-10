package org.study.stu.example.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.study.stu.common.DataBlock;
import org.study.stu.common.GroupBundle;
import org.study.stu.common.MergeInMemoryWithDict;
import org.study.stu.utils.BaseDataConvert;
import org.study.stu.utils.Constant;
import org.study.stu.utils.JsonToCsvConvert;

public class AvroMixedMapReduce1 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String [] {
				"/home/ym/data/1mRsample",
				"/home/ym/ytmp/output/nelog"
		};
		int res = ToolRunner.run(new Configuration(),  new AvroMixedMapReduce1(), args);
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf);
		job.setJarByClass(AvroMixedMapReduce1.class);
		
		job.set(AvroJob.OUTPUT_SCHEMA, DataBlock.getRecordSchema(26).toString());
		job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(AvroOutputFormat.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return JobClient.runJob(job).isSuccessful() ? 0 : 1;
	}
	
	public static class MapperClass 
		extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

		private static BaseDataConvert dataConvet 
			= new JsonToCsvConvert(new GroupBundle(Constant.TEST_EN_GROUP_BUNDLE, Constant.DEFAULT_SEPARATOR)); 
		
		private IntWritable outputKey = new IntWritable();
			
		@Override
		public void map(LongWritable key,
						 Text value,
						 OutputCollector<IntWritable, Text> output, 
						 Reporter reporter)
				throws IOException {			
			String v = dataConvet.dataFormat(value.toString());
			outputKey.set(v.toString().substring(0, Math.min(3, v.length())).hashCode());
			
			
			output.collect(outputKey, new Text(v));
		}		
	}

	public static class ReducerClass 
		extends MapReduceBase implements Reducer<IntWritable, Text, AvroWrapper<GenericRecord>, NullWritable>{

		@Override
		public void reduce(IntWritable key, 
				Iterator<Text> values,
				OutputCollector<AvroWrapper<GenericRecord>, NullWritable> output,
				Reporter reporter) throws IOException {
			System.out.println("reduce!!");
			
			MergeInMemoryWithDict md 
				= new MergeInMemoryWithDict(Constant.TEST_EN_GROUP_BUNDLE, Constant.DEFAULT_SEPARATOR);
			
			while (values.hasNext()){
			//	System.out.println(key.get() + ": " + values.next());
				DataBlock dataBlock = md.mergeInMemory(values, 2000);
				//dataBlock.showRecords();
				//dataBlock.showDictionaryBundle();
				
				//System.out.println("&&&&&&&&&&&&&&");
				
				md.clear();		
				GenericRecord r = dataBlock.toRecord();			
				output.collect(new AvroWrapper<GenericRecord>(r), NullWritable.get());
				/*
				System.out.println(DataBlock.GROUP_BUNDLE + ": " + r.get(DataBlock.GROUP_BUNDLE));
			
				int dictAmount = (int) r.get(DataBlock.DICTIONARY_AMOUNT);
				System.out.println(DataBlock.DICTIONARY_AMOUNT + ": " + dictAmount);			
				CDictionaryBundle bundle = new CDictionaryBundle((int)r.get(DataBlock.DICTIONARY_AMOUNT));
				for (int j = 0; j < dictAmount; j++){
					bundle.setDictFromByteBuffer(j, (ByteBuffer)r.get(DataBlock.DICTIONARY + j));				
				}
				bundle.showDictionaries();
			
				int recordAmount = (int) r.get(DataBlock.RECORD_AMOUNT);				
				ByteBuffer records = (ByteBuffer)r.get(DataBlock.RECORDS);
				byte[] recordsBytes = new byte[records.remaining()];
				records.get(recordsBytes, 0, records.remaining());
				ByteArrayInputStream bais = new ByteArrayInputStream(recordsBytes);
				DataInput dataInput = new DataInputStream(bais);
				for (int k = 0; k < recordAmount; k++)
					System.out.println(dataInput.readUTF());
			
				System.out.println("-------------------------------------------------------");
				*/
			
			}			
		}		
	}
}
