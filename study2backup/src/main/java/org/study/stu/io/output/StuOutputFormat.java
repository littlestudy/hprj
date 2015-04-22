package org.study.stu.io.output;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.Codec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.study.stu.common.DataBlock;
import org.study.stu.file.GenericWriter;
import org.study.stu.file.NullCodec;
import org.study.stu.file.SnappyCodec;
import org.study.stu.utils.DataFileConstants;

public class StuOutputFormat extends FileOutputFormat<DataBlock, NullWritable>{

	public static class StuWriter extends RecordWriter<DataBlock, NullWritable>{

		private GenericWriter writer;
		
		public StuWriter(OutputStream out, Codec c, String group, String separator) throws IOException{
			writer = new GenericWriter();
			writer.setGroupBundle(group, separator);
			writer.setCodec(c);
			writer.create(out);
		}
		
		@Override
		public void write(DataBlock key, NullWritable value)
				throws IOException, InterruptedException {
			writer.append(key);			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			writer.close();			
			writer = null;
		}		
	}
	
	@Override
	public RecordWriter<DataBlock, NullWritable> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		
		Codec c = null;
		String codecName = job.getConfiguration().get(DataFileConstants.CONF_OUTPUT_CODEC);
		
		if (codecName == null) {
			c = NullCodec.INSTANCE;
		} else {
			c = SnappyCodec.INSTANCE;
		}
		
		String groupBundle = job.getConfiguration().get(DataFileConstants.GROUP_BUNDLE);
		String separator = job.getConfiguration().get(DataFileConstants.GROUP_BUNDLE_SEPARATOR);
		
		Path file = getDefaultWorkFile(job, ".stu");
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		return new StuWriter(fs.create(file), c, groupBundle, separator);
	}
	
}