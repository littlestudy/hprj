package org.v2.ioformat.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class BaseDataInputFormat extends FileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return null;
	}
	
	public static class BaseDataRecordReader extends RecordReader<LongWritable, Text>{

		private LineRecordReader reader = new LineRecordReader();
	
		private final Text value_ = new Text();		
		private BaseDataConvert dataFormat;
	
		public BaseDataRecordReader(BaseDataConvert dataFormat){
			this.dataFormat = dataFormat;
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);		
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()){
				String value = dataFormat.dataFormat(reader.getCurrentValue().toString());
				if (value != null){
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
			return reader.getCurrentValue();
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


}
