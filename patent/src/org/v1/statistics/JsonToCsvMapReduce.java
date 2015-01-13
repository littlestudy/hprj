package org.v1.statistics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.v1.ioformat.JsonToCsvInputForamt;
import org.v1.utils.Constant;

public class JsonToCsvMapReduce extends Configured implements Tool{
	public static void main(String[] args) throws Exception {	
		args = new String[] {
				"hdfs://master:9000/user/hadoop/data/o100R",
				"/home/htmp/output/testcsv"
		};
		int res = ToolRunner.run(new Configuration(),  new JsonToCsvMapReduce(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(JsonToCsvMapReduce.class);

		job.setMapperClass(MappClass.class);
		job.setNumReduceTasks(0);

		JsonToCsvInputForamt.setGroups(Constant.getTargetFileds());
		JsonToCsvInputForamt.setGroupSeparator(",");
		job.setInputFormatClass(JsonToCsvInputForamt.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		job.waitForCompletion(true);
		return 0;
	}
	
	
	public static class MappClass extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//System.out.println("value: " + value.toString());
			context.write(NullWritable.get(), value);
		}
	}
}
