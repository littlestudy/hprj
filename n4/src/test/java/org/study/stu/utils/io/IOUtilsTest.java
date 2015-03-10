package org.study.stu.utils.io;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;

import org.junit.Test;
import org.study.stu.common.GroupBundle;
import org.study.stu.utils.Constant;
import org.study.stu.utils.BaseDataConvert;
import org.study.stu.utils.JsonToCsvConvert;

public class IOUtilsTest {

	@Test
	public void testJsonToCsvIterator() throws FileNotFoundException {
		BaseDataConvert dataConvert = 
				new JsonToCsvConvert(new GroupBundle(Constant.TEST_EN_GROUP_BUNDLE2, ","));
		JsonToCsvIterator iter = IOUtils.jsonToCsvIterator(
				new FileReader("/home/ym/data/500kR"), dataConvert);
				
		PrintStream ps = new PrintStream("/home/ym/data/500kR-H");
		while(iter.hasNext())
			ps.println(iter.next());
		ps.close();
		
	}

}
