package org.study.stu.io.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.study.stu.common.DataBlock;
import org.study.stu.file.StuRecordReader;

public class StuInputFormat extends FileInputFormat<DataBlock, NullWritable>{	

	@Override
	public RecordReader<DataBlock, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new StuRecordReader();
	}

}
