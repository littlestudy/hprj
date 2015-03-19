package org.study.stu.others;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.study.stu.io.output.StuOutputFormat;
import org.study.stu.utils.Constant;
import org.study.stu.utils.DataFileConstants;

public class TestOutput extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TestOutput(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path("/home/ym/ytmp/testfile");	
		Path outputPath = new Path("/home/ym/ytmp/asdg22");
		long size = inputPath.getFileSystem(getConf()).getFileStatus(inputPath).getBlockSize();
		FileSystem fs = inputPath.getFileSystem(getConf());
		fs.open(inputPath);
		System.out.println(fs.getClass().toString());
		System.out.println(size);
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Text.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setOutputFormatClass(StuOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(DataBlock.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.getConfiguration().set(DataFileConstants.GROUP_BUNDLE, "1");
		job.getConfiguration().set(DataFileConstants.GROUP_BUNDLE_SEPARATOR, "0");
		job.getConfiguration().set(DataFileConstants.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, NullWritable>{
				
		private Text outputKey = new Text();
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(" ------------------> map is called.");
			
		}		
		
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Context context) throws IOException, InterruptedException {
			outputKey.set(value.toString());
			context.write(outputKey, NullWritable.get());
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	public static class ReducerClass extends Reducer<Text, NullWritable, DataBlock, NullWritable>{
		@Override
		protected void reduce(
				Text key,
				Iterable<NullWritable> values,
				Context context)
				throws IOException, InterruptedException {
			DataBlock block = null;
			block = new DataBlock(key.toString());
			context.write(block, NullWritable.get());
		}
	}
}
