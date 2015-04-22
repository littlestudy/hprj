package org.study.stu.utils.io;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;
import org.study.stu.common.GroupBundle;
import org.study.stu.utils.Constant;
import org.study.stu.utils.BaseDataConvert;
import org.study.stu.utils.JsonToCsvConvert;

public class IOUtilsTest {

	@Test
	public void testJsonToCsvIterator() throws FileNotFoundException {
		BaseDataConvert dataConvert = 
				new JsonToCsvConvert(new GroupBundle(Constant.TEST_GROUPS_STR, ","));
		JsonToCsvIterator iter = IOUtils.jsonToCsvIterator(
				new FileReader(Constant.DEFAULT_RESOURCES_DIR + "/data/jsondata.txt"), dataConvert);
		while(iter.hasNext())
			System.out.println(iter.next());
		//TextInputFormat
		LocalFileSystem l = null;
		TextOutputFormat t = null;
	}

}
