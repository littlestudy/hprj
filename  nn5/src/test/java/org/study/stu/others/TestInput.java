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
import org.study.stu.io.input.StuInputFormat;
import org.study.stu.io.output.StuOutputFormat;
import org.study.stu.utils.Constant;
import org.study.stu.utils.DataFileConstants;

public class TestInput extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TestInput(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path("/home/ym/ytmp/asdg/part-r-00000.stu");	
		Path outputPath = new Path("/home/ym/ytmp/asdg2");
		long size = inputPath.getFileSystem(getConf()).getFileStatus(inputPath).getBlockSize();
		FileSystem fs = inputPath.getFileSystem(getConf());
		fs.open(inputPath);
		System.out.println(fs.getClass().toString());
		System.out.println(size);
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Text.class);
		
		job.setMapperClass(MapperClass.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(StuInputFormat.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class MapperClass extends Mapper<DataBlock, NullWritable, NullWritable, NullWritable>{
				
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
			System.out.println(" ------------------> map is called.");
			
		}		
		
		@Override
		protected void map(
				DataBlock key,
				NullWritable value,
				Context context) throws IOException, InterruptedException {
			System.out.println(key.toString());			
		}
		
		@Override
		protected void cleanup(Context context)
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
