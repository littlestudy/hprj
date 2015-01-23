package org.v2.statistics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.v2.ioformat.JsonToCsvInputForamt;
import org.v2.utils.Constant;
import org.v2.common.BaseMapReduce;

public class JsonToCsvMapReduce extends BaseMapReduce{
	public JsonToCsvMapReduce() {
		super(JsonToCsvMapReduce.class);
	}

	public static void main(String[] args) throws Exception {	
		args = new String[] {
				//"hdfs://master:9000/user/hadoop/data/o100R",
				Constant.DEFAULT_RESOURCES_DIR + "/data/jsondata.txt",
				"/home/htmp/output/testcsvGroup"
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

		JsonToCsvInputForamt.setGroups(Constant.getTestGroups());
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
			BaseMapper<LongWritable, Text, NullWritable, Text> {
		
		public MappClass() {
			super(MappClass.class);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//System.out.println("value: " + value.toString());
			context.write(NullWritable.get(), value);
		}
	}
}
