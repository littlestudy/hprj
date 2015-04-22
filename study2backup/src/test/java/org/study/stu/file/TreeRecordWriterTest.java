package org.study.stu.file;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.junit.Test;
import org.study.stu.common.DataBlock;
import org.study.stu.common.GroupBundle;
import org.study.stu.common.MergeInMemoryWithDict;
import org.study.stu.utils.Constant;
import org.study.stu.utils.JsonToCsvConvert;
import org.study.stu.utils.io.IOUtils;
import org.study.stu.utils.io.JsonToCsvTextIterator;

public class TreeRecordWriterTest {

	@Test
	public void testAppend() throws IOException {
		TreeRecordWriter writer = new TreeRecordWriter();
		OutputStream os = new FileOutputStream("/home/hadoop/tmp/test.avro");
		writer.setSyncInterval(100);
		writer.setCodec(CodecFactory.snappyCodec());
		writer.create(DataBlock.getRecordSchema(6), os);
		
		MergeInMemoryWithDict md =
				new MergeInMemoryWithDict(Constant.TEST_GROUP_BUNDLE_STR, Constant.DEFAULT_SEPARATOR);
		JsonToCsvTextIterator iter = IOUtils.jsonToCsvTextIterator(
					new FileReader(Constant.DEFAULT_RESOURCES_DIR + "/data/jsondata.txt"), 
					new JsonToCsvConvert(new GroupBundle(Constant.TEST_GROUP_BUNDLE_STR, Constant.DEFAULT_SEPARATOR)));
		DataBlock dataBlock = null;	
		while (iter.hasNext()){
			dataBlock = md.mergeInMemory(iter, 6);
			writer.append(dataBlock.toRecord());
			md.clear();
		}
		writer.cleanup();
		os.close();
	}

}
