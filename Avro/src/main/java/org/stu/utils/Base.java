package org.stu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public abstract class Base extends Configured implements Tool{
	
	public static void exec(Tool tool, String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), tool, args);
		System.exit(res);
	}
}
