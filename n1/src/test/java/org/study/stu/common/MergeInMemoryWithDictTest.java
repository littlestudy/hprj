package org.study.stu.common;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.study.stu.common.dict.CDictionaryBundle;

public class MergeInMemoryWithDictTest {
	List<String> values;
	List<String[]> groupList;

	@Before
	public void setUp() throws Exception {
		String dataPath = System.getProperty("user.dir") + "/src/test/resources/data/TreeRecordTestData.txt";
		//String dataPath = "/home/htmp/output/testcsvGroup5/part-m-00000";		
		values = new ArrayList<String>();
		LineIterator iter = IOUtils.lineIterator(new FileReader(dataPath));
		while (iter.hasNext())
			values.add(iter.next());
		
		String [] array1 = new String [] {"aa", "bb"};
		String [] array2 = new String [] {"cc", "dd", "ee"};
		String [] array3 = new String [] {"ff"};
		
		List<String[]> list = new ArrayList<String[]>();
		list.add(array1);
		list.add(array2);
		list.add(array3);
		
		groupList = list;
	}

	@Test
	public void testMergeInMemory() throws Exception {		
		DataBlock dataBlock = null;
		Iterator<String> iter = values.iterator();
		List<Text> list = new ArrayList<Text>();
		while (iter.hasNext()){
			list.add(new Text(iter.next()));
		}
		System.out.println(list.size());
		int i = 0;
		MergeInMemoryWithDict md = new MergeInMemoryWithDict(groupList);
		Iterator<Text> it = list.iterator();
		while (true){			
			dataBlock = md.mergeInMemory(it, 6);
			dataBlock.showRecords();
			dataBlock.showDictionaryBundle();
			dataBlock.save("/home/hadoop/tmp/output" + i++);
			md.clear();
			System.out.println("&&&&&&&&&&&&&&");
			
			System.out.println(dataBlock.createRecordJson());			
			
			GenericRecord r = dataBlock.toRecord();			
			
			System.out.println(DataBlock.GROUP_BUNDLE + ": " + r.get(DataBlock.GROUP_BUNDLE));
			
			int dictAmount = (int) r.get(DataBlock.DICTIONARY_AMOUNT);
			System.out.println(DataBlock.DICTIONARY_AMOUNT + ": " + dictAmount);			
			CDictionaryBundle bundle = new CDictionaryBundle((int)r.get(DataBlock.DICTIONARY_AMOUNT));
			for (int j = 0; j < dictAmount; j++){
				bundle.setRowDictionary(j, (Map<String, Integer>)r.get(DataBlock.DICTIONARY + j));				
			}
			bundle.showDictionaries();
			
			int recordAmount = (int) r.get(DataBlock.RECORD_AMOUNT);
			byte[] records = (byte[])r.get(DataBlock.RECORDS);
			ByteArrayInputStream bais = new ByteArrayInputStream(records);
			DataInput dataInput = new DataInputStream(bais);
			for (int k = 0; k < recordAmount; k++)
				System.out.println(dataInput.readUTF());
			
			System.out.println("-------------------------------------------------------");
			if (!it.hasNext())
				break;
		}

	}
}
