package org.study.stu.example.file;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.study.stu.common.DataBlock;
import org.study.stu.common.GroupBundle;
import org.study.stu.common.MergeInMemoryWithDict;
import org.study.stu.file.TreeRecordWriter;
import org.study.stu.utils.Constant;
import org.study.stu.utils.JsonToCsvConvert;
import org.study.stu.utils.io.IOUtils;
import org.study.stu.utils.io.JsonToCsvTextIterator;

public class TreeRecordWriterExample {

	public static void main(String[] args) throws IOException {		
		TreeRecordWriter writer = new TreeRecordWriter();
		OutputStream os = new FileOutputStream("/home/ym/ytmp/output/test");
		writer.setSyncInterval(1000);
		writer.setCodec(CodecFactory.snappyCodec());
		writer.create(DataBlock.getRecordSchema(26), os);
		
		MergeInMemoryWithDict md =
				new MergeInMemoryWithDict(Constant.TEST_GROUP_BUNDLE_STR2, Constant.DEFAULT_SEPARATOR);
		JsonToCsvTextIterator iter = IOUtils.jsonToCsvTextIterator(
					new FileReader("/home/ym/ytmp/experiment/data/1mRsample"), 
					new JsonToCsvConvert(new GroupBundle(Constant.TEST_GROUP_BUNDLE_STR2, Constant.DEFAULT_SEPARATOR)));
		DataBlock dataBlock = null;
		while (iter.hasNext()){
			dataBlock = md.mergeInMemory(iter, 2000);
			writer.append(dataBlock.toRecord());
			//System.out.println(iter.next());
			//String str = iter.next().toString();
			//ps.println(str);
			md.clear();
		}
		writer.cleanup();
		os.close();
	}

}
