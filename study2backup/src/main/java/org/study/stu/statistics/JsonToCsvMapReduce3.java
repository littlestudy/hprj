package org.study.stu.statistics;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.study.stu.common.DataBlock;
import org.study.stu.common.RestoreInMemoryWithDict;
import org.study.stu.io.input.StuInputFormat;

public class JsonToCsvMapReduce3 extends Configured implements Tool{

	//private static final Log LOG = LogFactory.getLog(JsonToCsvMapReduce.class);
	
	public static void main(String[] args) throws Exception {	
		args = new String[] {
				//"hdfs://master:9000/user/hadoop/data/o100R",
				"/home/ym/data/1mOc",
				"/home/ym/data/1mOu"
		};
		int res = ToolRunner.run(new Configuration(),  new JsonToCsvMapReduce3(), args);
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
		job.setJarByClass(JsonToCsvMapReduce3.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReducerClass.class);
		//job.setNumReduceTasks(0);
		
		job.setInputFormatClass(StuInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		return 0;
	}
	
	public static class MappClass extends Mapper<DataBlock, NullWritable, Text, NullWritable> {

		private Text outKey = new Text();
		
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
		}
		
		@Override
		protected void map(DataBlock key, NullWritable value, Context context)
				throws IOException, InterruptedException {
			System.out.println("---------- map function -------------");		
			RestoreInMemoryWithDict rimw = new RestoreInMemoryWithDict(key);
			for (String record : rimw.restore()){
				//System.out.println(record);
				outKey.set(record);
				context.write(outKey, NullWritable.get());
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
				context.write(key, NullWritable.get());
		}
	}
}
