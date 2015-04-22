package org.study.stu.common;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.study.stu.common.dict.CDictionaryBundle;
import org.study.stu.utils.Constant;

public class TreeRecordWrite extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		args = new String[] {
				System.getProperty("user.dir") + "/src/test/resources/data/TreeRecordTestData.txt",
				"/home/hadoop/tmp/test.avro"
		};
		int res = ToolRunner.run(new Configuration(), new TreeRecordWrite(), args);
		System.exit(res);
	}
	
	@SuppressWarnings("resource")
	@Override
	public int run(String[] args) throws Exception {
		List<Text> values = new ArrayList<Text>();
		LineIterator iter = IOUtils.lineIterator(new FileReader(args[0]));
		while (iter.hasNext())
			values.add(new Text(iter.next()));		
				
		OutputStream os = new FileOutputStream(args[1]);
		DataFileWriter<Object> writer
				= new DataFileWriter<Object>(new GenericDatumWriter<Object>()).setSyncInterval(100);
		writer.setCodec(CodecFactory.snappyCodec());
		writer.create(DataBlock.getRecordSchema(6), os);
		
		DataBlock dataBlock = null;		
		int i = 0;
		MergeInMemoryWithDict md =
				new MergeInMemoryWithDict(Constant.TEST_GROUP_BUNDLE_STR, Constant.DEFAULT_SEPARATOR);
		Iterator<Text> it = values.iterator();
		while (true){			
			dataBlock = md.mergeInMemory(it, 6);
			dataBlock.showRecords();
			dataBlock.showDictionaryBundle();
			dataBlock.save("/home/hadoop/tmp/output" + i++);
			md.clear();
			System.out.println("&&&&&&&&&&&&&&");
			
			GenericRecord r = dataBlock.toRecord();			
			writer.append(r);
			System.out.println(DataBlock.GROUP_BUNDLE + ": " + r.get(DataBlock.GROUP_BUNDLE));
			
			int dictAmount = (int) r.get(DataBlock.DICTIONARY_AMOUNT);
			System.out.println(DataBlock.DICTIONARY_AMOUNT + ": " + dictAmount);			
			CDictionaryBundle bundle = new CDictionaryBundle((int)r.get(DataBlock.DICTIONARY_AMOUNT));
			for (int j = 0; j < dictAmount; j++){
				bundle.setDictFromByteBuffer(j, (ByteBuffer)r.get(DataBlock.DICTIONARY + j));				
			}
			bundle.showDictionaries();
			
			int recordAmount = (int) r.get(DataBlock.RECORD_AMOUNT);
			ByteBuffer records = (ByteBuffer)r.get(DataBlock.RECORDS);
			byte[] recordsBytes = new byte[records.remaining()];
			records.get(recordsBytes, 0, records.remaining());
			ByteArrayInputStream bais = new ByteArrayInputStream(recordsBytes);
			DataInput dataInput = new DataInputStream(bais);
			for (int k = 0; k < recordAmount; k++)
				System.out.println(dataInput.readUTF());
			
			System.out.println("-------------------------------------------------------");
			if (!it.hasNext())
				break;
		}
		org.apache.hadoop.io.IOUtils.cleanup(null, writer);
		org.apache.hadoop.io.IOUtils.cleanup(null, os);		
		
		return 0;
	}

}
