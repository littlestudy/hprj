package org.v1.ioformat;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.v1.utils.JsonToCsv;

public class JsonToCsvInputForamt extends FileInputFormat<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(JsonToCsvInputForamt.class);
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new JsonToCsvRecordReader();
	}

	public static class JsonToCsvRecordReader extends RecordReader<LongWritable, Text> {			
		//private static final Log LOG = LogFactory.getLog(JsonToCsvRecordReader.class);
		public static List<String[]> groups;

		private LineRecordReader reader = new LineRecordReader();

		private final Text value_ = new Text();
		private static String groupSeparator = "##";
		private static JsonToCsv jsonToCsv = new JsonToCsv();
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
			jsonToCsv.setGroupSeparator(groupSeparator);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()) {
				String value = jsonToCsv.jsonStringToCsv(reader.getCurrentValue().toString(), groups);
				System.out.println("value: " + value);
				if (value != null) {
					value_.set(value);
					return true;
				}
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value_;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}
	
	public static void setGroups(List<String[]> groups) {		
		LOG.info("set group for JsonToCsvInputForamt.JsonToCsvRecordReader");
		JsonToCsvInputForamt.JsonToCsvRecordReader.groups = groups;
	}
	
	public static void setGroupSeparator(String groupSeparator){
		LOG.info("set group separator for JsonToCsvInputForamt.groupSeparator");
		JsonToCsvInputForamt.JsonToCsvRecordReader.groupSeparator = groupSeparator;
	}
}