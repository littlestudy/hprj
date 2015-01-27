package org.v3.common;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.v2.utils.LogReader;
import org.v2.utils.LogReader.JobState;
import org.v2.utils.LogReader.ShowLogType;


public abstract class BaseMapReduce extends Configured implements Tool{
	
	private final Log LOG; 
		
	public static String jobId_;
	public static ShowLogType show_;
	public static JobState jobState_;
	
	public BaseMapReduce(Class<?> cls){
		LOG = LogFactory.getLog(cls);
	}
	
	public void info(String info){		
		LOG.info(LogReader.LOG_IDENTITY + info);
	}
	
	public static void exec(Tool tool, String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), tool, args);
		saveJobId(jobId_, jobState_, show_);
		System.exit(res);
	}
	
	public void setJobIdAndShowLog(Job job, JobState jobState, ShowLogType show){		
		jobId_ = job.getJobID().toString();
		jobState_ = jobState;
		show_ = show;
	}
	
	public void removeHdfsFile(Path path) throws IOException{
		path.getFileSystem(getConf()).delete(path, true);
	}
	
	public static void saveJobId(String jobId, JobState jobState, ShowLogType show){
		PrintStream ps = null;
		try {
			ps = new PrintStream(LogReader.LOCAL_JOBID_FILE);
			ps.println(LogReader.getJobIdInfo(jobId, jobState, show));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(ps);
		}		
	}	
	
	public static class BaseMapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
			extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		private final Log LOG;
		
		public BaseMapper(Class<?> cls){
			LOG = LogFactory.getLog(cls);
		}
		
		public void info(String info){		
			LOG.info(LogReader.LOG_IDENTITY + info);
		}
	}
	
	public static class BaseReduce<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
			extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		private final Log LOG;
		
		public BaseReduce(Class<?> cls){
			LOG = LogFactory.getLog(cls);
		}
		
		public void info(String info){		
			LOG.info(LogReader.LOG_IDENTITY + info);
		}
	}
}
