package org.study.stu.statistics;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.study.stu.common.DataBlock;
import org.study.stu.common.GroupBundle;
import org.study.stu.common.MergeInMemoryWithDict;
import org.study.stu.io.input.BaseDataInputFormat;
import org.study.stu.io.output.StuOutputFormat;
import org.study.stu.utils.Constant;
import org.study.stu.utils.DataFileConstants;
import org.study.stu.utils.JsonToCsvConvert;

public class JsonToCsvMapReduce2 extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {	
		args = new String[] {
				//"hdfs://master:9000/user/hadoop/data/o100R",
				//Constant.DEFAULT_RESOURCES_DIR + "/data/jsondata.txt",
				"/home/ym/data/1mO",
				"/home/ym/data/1mOc"
		};
		int res = ToolRunner.run(new Configuration(),  new JsonToCsvMapReduce2(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		
		
		Configuration conf = new Configuration();
		long size = inputPath.getFileSystem(conf).getFileStatus(inputPath).getBlockSize();
		System.out.println(size);
		Job job = Job.getInstance(conf);
		job.setJarByClass(JsonToCsvMapReduce2.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReducerClass2.class);

		BaseDataInputFormat.setDataConvert(new JsonToCsvConvert(
								new GroupBundle(Constant.TEST_NE_GROUP_BUNDLE_STR, Constant.DEFAULT_SEPARATOR)));				
		job.setInputFormatClass(BaseDataInputFormat.class);
		
		job.getConfiguration().set(DataFileConstants.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
		job.getConfiguration().set(DataFileConstants.GROUP_BUNDLE, Constant.TEST_NE_GROUP_BUNDLE_STR);
		job.getConfiguration().set(DataFileConstants.GROUP_BUNDLE_SEPARATOR, Constant.DEFAULT_SEPARATOR);
		
		job.setOutputFormatClass(StuOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		return 0;
	}
	
	public static class MappClass extends Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable outputKey = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			outputKey.set(value.toString().substring(0, Math.min(1, value.getLength())).hashCode());
			context.write(outputKey, value);
		}
	}
	
	public static class ReducerClass2 extends Reducer<IntWritable, Text, DataBlock, NullWritable>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
						
			MergeInMemoryWithDict md 
				= new MergeInMemoryWithDict(Constant.TEST_NE_GROUP_BUNDLE_STR, Constant.DEFAULT_SEPARATOR);
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()){
				DataBlock dataBlock = md.mergeInMemory(iter,10000);
				md.clear();				
				context.write(dataBlock, NullWritable.get());
			}
		}
	}
}
