package org.study.stu.example.file;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericRecord;
import org.study.stu.common.DataBlock;
import org.study.stu.common.RestoreInMemoryWithDict;
import org.study.stu.file.TreeRecordRead;

public class TreeRecordReadExample {

	public static void main(String[] args) throws IOException {
		InputStream inputStream = new FileInputStream("/home/hadoop/tmp/test.avro");
		TreeRecordRead reader = new TreeRecordRead(inputStream);
		for (Object o : reader){
			GenericRecord r = (GenericRecord) o;
			
			ByteBuffer groupBundleBytes = (ByteBuffer)r.get(DataBlock.GROUP_BUNDLE);
						
			RestoreInMemoryWithDict rimw = new RestoreInMemoryWithDict(groupBundleBytes);
			
			int dictAmount = (int) r.get(DataBlock.DICTIONARY_AMOUNT);
			System.out.println(DataBlock.DICTIONARY_AMOUNT + ": " + dictAmount);			
			for (int i = 0; i < dictAmount; i++){
				rimw.setDictFromByteBuffer(i, (ByteBuffer)r.get(DataBlock.DICTIONARY + i));				
			}			
			
			int recordAmount = (int) r.get(DataBlock.RECORD_AMOUNT);
			ByteBuffer records = (ByteBuffer)r.get(DataBlock.RECORDS);
			byte[] recordsBytes = new byte[records.remaining()];
			records.get(recordsBytes, 0, records.remaining());
			ByteArrayInputStream bais = new ByteArrayInputStream(recordsBytes);
			DataInput dataInput = new DataInputStream(bais);
			for (int k = 0; k < recordAmount; k++){
				String tree = dataInput.readUTF();
				System.out.println(tree);
				for (String str : rimw.restoreInMemory(tree))
					System.out.println(str);
			}
			System.out.println("----------------------");
		}		
		
		inputStream.close();
		reader.cleanup();
	}

}
