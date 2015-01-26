package org.v2.utils.im.md;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Before;
import org.junit.Test;
import org.v2.common.DataBlock;

public class MergeInMemoryWithDictTest {
	List<String> values;
	List<String[]> groupList;

	@Before
	public void setUp() throws Exception {
		//String dataPath = System.getProperty("user.dir") + "/src/test/resources/data/TreeRecordTestData.txt";
		String dataPath = "/home/htmp/output/testcsvGroup/part-m-00000";		
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
		
		while (true){
			MergeInMemoryWithDict md = new MergeInMemoryWithDict(groupList);
			dataBlock = md.mergeInMemory(iter, 10);
			dataBlock.showRecords();
			dataBlock.showDictionaryBundle();
			dataBlock.save("/home/htmp/output/ccc/all");
			System.out.println("----------------------------");
			if (!iter.hasNext())
				break;
		}

	}

}
