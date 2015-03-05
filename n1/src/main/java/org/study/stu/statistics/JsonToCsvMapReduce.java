package org.study.stu.statistics;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.study.stu.common.dict.CDictionaryBundle;
import org.study.stu.io.input.BaseDataInputFormat;
import org.study.stu.utils.Constant;
import org.study.stu.utils.JsonToCsvConvert;

public class JsonToCsvMapReduce extends Configured implements Tool{

	private static final Log LOG = LogFactory.getLog(JsonToCsvMapReduce.class);
	
	public static void main(String[] args) throws Exception {	
		args = new String[] {
				//"hdfs://master:9000/user/hadoop/data/o100R",
				Constant.DEFAULT_RESOURCES_DIR + "/data/jsondata.txt",
				"/home/hadoop/tmp/output/testcsvGroup"
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
		job.setReducerClass(ReducerClass.class);

		BaseDataInputFormat.setDataConvert(new JsonToCsvConvert(new GroupBundle(Constant.getTestGroups(), "##")));
		job.setInputFormatClass(BaseDataInputFormat.class);

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
	
	public static class ReducerClass extends Reducer<IntWritable, Text, Text, Text>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			MergeInMemoryWithDict md = new MergeInMemoryWithDict(Constant.getTestGroups());
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()){
				DataBlock dataBlock = md.mergeInMemory(iter, 6);
				dataBlock.showRecords();
				dataBlock.showDictionaryBundle();
				
				System.out.println("&&&&&&&&&&&&&&");
				
				md.clear();		
				System.out.println(dataBlock.createRecordJson());
			GenericRecord r = dataBlock.toRecord();			
			
			System.out.println(DataBlock.GROUP_BUNDLE + ": " + r.get(DataBlock.GROUP_BUNDLE));
			
			int dictAmount = (int) r.get(DataBlock.DICTIONARY_AMOUNT);
			System.out.println(DataBlock.DICTIONARY_AMOUNT + ": " + dictAmount);			
			CDictionaryBundle bundle = new CDictionaryBundle((int)r.get(DataBlock.DICTIONARY_AMOUNT));
			for (int j = 0; j < dictAmount; j++){
				bundle.setRowDictionary(j, (Map<String, Integer>)r.get(DataBlock.DICTIONARY + j));				
			}
			bundle.showDictionaries();
			
			int recordAmount = (int) r.get(DataBlock.RECORD_AMOUNT);
			byte[] records = (byte[])r.get(DataBlock.RECORDS);
			ByteArrayInputStream bais = new ByteArrayInputStream(records);
			DataInput dataInput = new DataInputStream(bais);
			for (int k = 0; k < recordAmount; k++)
				System.out.println(dataInput.readUTF());
			
			System.out.println("-------------------------------------------------------");
			}
			///context.write(new Text(key), new Text());
		}
	}
}
