package org.v1.utils.im.mimwd;


import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Before;
import org.junit.Test;
import org.v1.utils.im.mimwd.DataBlock;
import org.v1.utils.im.mimwd.CDictionaryBundle;
import org.v1.utils.im.mimwd.GroupBundle;
import org.v1.utils.im.mimwd.TreeRecord;

public class DataBlockTest {

	List<String> values;
	
	@Before
	public void setUp() throws Exception {
		String dataPath = System.getProperty("user.dir") + "/src/test/resources/data/TreeRecordTestData.txt";		
		values = new ArrayList<String>();
		LineIterator iter = IOUtils.lineIterator(new FileReader(dataPath));
		while (iter.hasNext())
			values.add(iter.next());
	}

	@Test
	public void testGetRecords() {
		List<TreeRecord> list = new ArrayList<TreeRecord>();
		for (String str : values){
			list.add(TreeRecord.fromOriginalString(str));
		}
						
		merge(list);				
		merge(list);
		
		DataBlock dataBlock = new DataBlock(null, null, list);
		
		for (String str : dataBlock.getRecords())
			System.out.println(str);
	}

	private void merge(final List<TreeRecord> list){
		HashMap<String, List<TreeRecord>> mergeMap = new HashMap<String, List<TreeRecord>>();
		for (TreeRecord treeRecord : list){
			String key = treeRecord.getKey();
			List<TreeRecord> result = mergeMap.get(key);
			if (result == null){
				result = new ArrayList<TreeRecord>();
				mergeMap.put(key, result);				
			}
			result.add(treeRecord);			
		}
		
		list.clear();
		Iterator<Entry<String, List<TreeRecord>>> iter = mergeMap.entrySet().iterator();
		while (iter.hasNext()){
			Entry<String, List<TreeRecord>> entry = iter.next();
			list.add(TreeRecord.generateTreeRecodeFromTreeList(entry.getKey(), entry.getValue()));
		}
	}
	
		@Test
	public void testSave() {
		List<String[]> groups = new ArrayList<String[]>();
		groups.add(new String[] {"field4", "field5"});
		groups.add(new String[] {"field1", "field2", "field3"});
		groups.add(new String[] {"field0"});
		GroupBundle groupBundle = new GroupBundle(groups);
			
		CDictionaryBundle dictionaryBundle = new CDictionaryBundle(6);		
		dictionaryBundle.find(0, "0p");
		dictionaryBundle.find(0, "0a");
		dictionaryBundle.find(1, "1v");
		dictionaryBundle.find(1, "1b");		
		dictionaryBundle.find(2, "");
		dictionaryBundle.find(2, "2c");
		dictionaryBundle.find(2, "2x");
		dictionaryBundle.find(3, "3e");
		dictionaryBundle.find(3, "3d");
		dictionaryBundle.find(4, "4e");
		dictionaryBundle.find(4, "4x");
		dictionaryBundle.find(4, "4t");		
		dictionaryBundle.find(5, "5d");
		dictionaryBundle.find(5, "5k");
				
		TreeRecord treeRecord1 = new TreeRecord(null, null, null);
		treeRecord1.setKey(null);
		treeRecord1.setRoot("0");
		treeRecord1.setSubTrees("(1,2,1)[(2,0)(0,0)]"); // (0)[(1,2,1)[(2,0)(0,0)]]
		TreeRecord treeRecord2 = new TreeRecord(null, null, null);
		treeRecord2.setKey(null);
		treeRecord2.setRoot("1");
		treeRecord2.setSubTrees("(0,0,0)[(1,1)](1,1,1)[(0,0)(0,1)]"); // (1)[(0,0,0)[(1,1)](1,1,1)[(0,0)(0,1)]]
		List<TreeRecord> treeRecords = new ArrayList<TreeRecord>();
		treeRecords.add(treeRecord1);
		treeRecords.add(treeRecord2);
		DataBlock db = new DataBlock(groupBundle, dictionaryBundle, treeRecords);		
		
		try {
			db.save("dbtest.txt");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
