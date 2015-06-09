package org.study.stu.statistics;

import java.io.IOException;

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
import org.study.stu.common.GroupBundle;
import org.study.stu.io.input.BaseDataInputFormat;
import org.study.stu.utils.Constant;
import org.study.stu.utils.JsonToCsvConvert;

public class JsonToCsvMapReduce extends Configured implements Tool{

	//private static final Log LOG = LogFactory.getLog(JsonToCsvMapReduce.class);
	
	public static void main(String[] args) throws Exception {	
		args = new String[] {
				//"hdfs://master:9000/user/hadoop/data/o100R",
				//Constant.DEFAULT_RESOURCES_DIR + "/data/jsondata.txt",
				"/home/ym/data/4m",
				"/home/ym/data/4m-csv7"
		};
		int res = ToolRunner.run(new Configuration(),  new JsonToCsvMapReduce(), args);
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
		job.setJarByClass(JsonToCsvMapReduce.class);

		job.setMapperClass(MappClass.class);
		job.setReducerClass(ReducerClass.class);
		
		BaseDataInputFormat.setDataConvert(new JsonToCsvConvert(
								new GroupBundle(Constant.TEST_NE_GROUP_BUNDLE_STR2, Constant.DEFAULT_SEPARATOR)));
		job.setInputFormatClass(BaseDataInputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		return 0;
	}
	
	public static class MappClass extends Mapper<LongWritable, Text, IntWritable, Text> {		
		
		IntWritable outkey = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			outkey.set(value.toString().hashCode());
			context.write(outkey, value);
		}
	}
	
	public static class ReducerClass extends Reducer<IntWritable, Text, Text, NullWritable>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			for (Text t : values)
				context.write(t, NullWritable.get());
		}
	}
}
