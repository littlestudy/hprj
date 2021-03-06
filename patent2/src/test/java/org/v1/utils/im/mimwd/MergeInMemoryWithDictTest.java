package org.v1.utils.im.mimwd;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Before;
import org.junit.Test;
import org.v1.utils.im.mimwd.DataBlock;
import org.v1.utils.im.mimwd.MergeInMemoryWithDict;

public class MergeInMemoryWithDictTest {
	List<String> values;
	List<String[]> groupList;
	
	@Before
	public void setUp() throws Exception {
		String dataPath = System.getProperty("user.dir") + "/src/test/resources/data/TreeRecordTestData.txt";		
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
	public void testMergeInMemory() {
		MergeInMemoryWithDict mimwd = new MergeInMemoryWithDict(groupList);
		DataBlock dataBlock = mimwd.mergeInMemory(values);
		dataBlock.showRecords();
		dataBlock.showDictionaryBundle();
	}

}
