package org.v1.utils.mimwd;

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

public class TreeRecordTest {
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
	public void testFromOriginalStringString() {	
		List<TreeRecord> list = new ArrayList<TreeRecord>();
		for (String str : values){
			list.add(TreeRecord.fromOriginalString(str));
		}
		
		for (TreeRecord treeRecord : list)
			System.out.println(treeRecord.toString());
	}

	@Test
	public void testGenerateTreeRecodeFromTreeListStringListOfTreeRecord() {
		List<TreeRecord> list = new ArrayList<TreeRecord>();
		for (String str : values){
			list.add(TreeRecord.fromOriginalString(str));
		}
		
		System.out.println("init");
		showTreeRecordList(list);
		
		merge(list);
		System.out.println("merge 1");
		showTreeRecordList(list);
		
		merge(list);
		System.out.println("merge 2");
		showTreeRecordList(list);
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
	
	private void showTreeRecordList(List<TreeRecord> list){
		for (TreeRecord treeRecord : list)
			System.out.println(treeRecord.toString());
	}

}
