package org.study.stu.file;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.study.stu.common.DataBlock;
import org.study.stu.io.FsInput;

public class StuRecordReader extends RecordReader<DataBlock, NullWritable>{
	private GenericDataReader reader;
	private long start;
	private long end;
	
	public StuRecordReader(){		
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();		
		reader = GenericDataReader.openReader(new FsInput(file, job));		
		reader.sync(split.getStart());
		this.start = reader.tell();
		this.end = split.getStart() + split.getLength();
		System.out.println("split start: " + split.getStart() + ", split length: " + split.getLength() + ", split end:" + this.end);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {		
		if (!reader.hasNext() || reader.pastSync(end))
			return false;
		return true;
	}

	@Override
	public DataBlock getCurrentKey() throws IOException, InterruptedException {
		return reader.next();
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (end == start) {
	    	return 0.0f;
	    } else {
	    	return Math.min(1.0f, (getPos() - start) / (float)(end - start));
	    }
	}

	public long getPos() throws IOException {
		return reader.tell();
	}
	  
	@Override
	public void close() throws IOException {
		reader.close();		
	}
}
