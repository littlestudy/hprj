package org.study.stu.io.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.study.stu.utils.DataConvertBase;

public class BaseDataInputFormat extends FileInputFormat<LongWritable, Text>{

	private static DataConvertBase dataConvert;
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new BaseDataRecordReader(dataConvert);
	}
	
	public static class BaseDataRecordReader extends RecordReader<LongWritable, Text>{

		private LineRecordReader reader = new LineRecordReader();
	
		private final Text value_ = new Text();		
		private DataConvertBase dataCovert;
	
		public BaseDataRecordReader(DataConvertBase dataConvert){
			this.dataCovert = dataConvert;
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);		
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()){
				String value = dataCovert.dataFormat(reader.getCurrentValue().toString());
				//System.out.println("value: " + value);
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
	
	public static void setDataConvert(DataConvertBase baseDataConvert){
		dataConvert = baseDataConvert;
	}
}
