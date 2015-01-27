package org.v2.ioformat.input.jsontocsv;

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
import org.v2.common.GroupBundle;

public class JsonToCsvInputForamt extends FileInputFormat<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(JsonToCsvInputForamt.class);	
	private static GroupBundle groupBundle;
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new JsonToCsvRecordReader(groupBundle);
	}

	public static class JsonToCsvRecordReader extends RecordReader<LongWritable, Text> {			
		//private static final Log LOG = LogFactory.getLog(JsonToCsvRecordReader.class);

		private LineRecordReader reader = new LineRecordReader();

		private final Text value_ = new Text();		
		private JsonToCsv jsonToCsv;
				
		public JsonToCsvRecordReader(GroupBundle groupBundle) {
			jsonToCsv = new JsonToCsv(groupBundle);
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()) {
				String value = jsonToCsv.jsonStringToCsv(reader.getCurrentValue().toString());
				//System.out.println("value: " + value);
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
		LOG.info("set GroupBundle");
		groupBundle = new GroupBundle(groups);
	}
}