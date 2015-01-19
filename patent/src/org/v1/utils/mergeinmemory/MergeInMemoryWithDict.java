package org.v1.utils.mergeinmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeInMemoryWithDict {
	
	private List<TreeRecord> records;
	private boolean isFinish = false;			
	private List<Map<String, Integer>> dictionary;
	private int code;
	private List<Integer> mGroup;
	
	public MergeInMemoryWithDict(List<String[]> group){
		mGroup = new ArrayList<Integer>(group.size());
		int fieldNumberBase = 0;
		
		for (String[] strings : group){		
			mGroup.add(fieldNumberBase);	
			fieldNumberBase += strings.length;			
		}
		
		dictionary = new ArrayList<Map<String,Integer>>(mGroup.size());
	}
	
	public DataBlock mergeInMemory(List<String> values){
		initRecords(values);		
		
		for (int i = 0; i < mGroup.size(); i++){
			int fieldNumberBase = mGroup.get(mGroup.size() - i - 1);
			merge(fieldNumberBase);
		}
		
		return null;
	}
	
	private void merge(int fieldNumberBase){	
		Map<String, List<TreeRecord>> map = new HashMap<String, List<TreeRecord>>();
		
		code = 0;
		for (TreeRecord treeRecord : records)	{
			String key = treeRecord.getKey();	
			String root = treeRecord.getRoot();
			
			generateDictItems(root, fieldNumberBase);
			
			if(!map.containsKey(key)){
				List<TreeRecord> treeList = new ArrayList<TreeRecord>();
				treeList.add(treeRecord);
				map.put(key, treeList);
				continue;
			}
			
			List<TreeRecord> treeList = map.get(key);
			treeList.add(treeRecord);
		}
		
		records.clear();
		for (String k : map.keySet())
			records.add(TreeRecord.generateTreeRecodeFromTreeList(k, map.get(k)));
		
		map = null;
	}
	
	private void generateDictItems(String root, int fieldNumberBase) {
		String parts[] = root.split(",", -1);
		int groupItemLen = parts.length;
		for (int i = 0; i < groupItemLen; i++){
			Map<String,Integer> map = dictionary.get(i + fieldNumberBase);
			map = new HashMap<String, Integer>();
		}
		
	}

	private void initRecords(List<String> values){
		records = new ArrayList<TreeRecord>();
		for (String str : values)
			records.add(TreeRecord.fromOriginalString(str));		
	}
	
	public void showRecords(){
		for (TreeRecord record : records){
			System.out.println("Key  === " + record.getKey());
			System.out.println("tree --- " + record.getTree());
		}		
	}
}
