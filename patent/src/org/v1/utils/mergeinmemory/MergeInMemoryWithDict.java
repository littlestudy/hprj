package org.v1.utils.mergeinmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeInMemoryWithDict {
	
	private List<TreeRecord> records;		
	private List<Integer> mGroup;
	private List<Map<String, Integer>> dictionary;	
	private List<Integer> dictionaryCodeTop;
	
	public MergeInMemoryWithDict(List<String[]> group){
		mGroup = new ArrayList<Integer>(group.size());
		
		int fieldNumberBase = 0;
		for (int i = 0; i < group.size(); i++)	{
			mGroup.add(fieldNumberBase);	
			String [] strings = group.get(i);			
			fieldNumberBase += strings.length;			
		}
		
		dictionary = new ArrayList<Map<String,Integer>>(fieldNumberBase);
		dictionaryCodeTop = new ArrayList<Integer>(fieldNumberBase);
		for (int i = 0; i < fieldNumberBase; i++){
			dictionary.add(new HashMap<String, Integer>());
			dictionaryCodeTop.add(0);
		}		
	}
	
	public DataBlock mergeInMemory(List<String> values){
		initRecords(values);		
		
		for (int i = 0; i < mGroup.size(); i++){
			int fieldNumberBase = mGroup.get(mGroup.size() - i - 1);
			merge(fieldNumberBase);
		}
		
		return new DataBlock(dictionary, records);
	}
	
	private void merge(int fieldNumberBase){	
		Map<String, List<TreeRecord>> map = new HashMap<String, List<TreeRecord>>();
		
		for (TreeRecord treeRecord : records)	{
			String key = treeRecord.getKey();	
			
			generateDictItems(treeRecord, fieldNumberBase); // 对当前的treeRecord的root进行编码
			
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
	
	private void generateDictItems(final TreeRecord treeRecord, int fieldNumberBase) {
		String parts[] = treeRecord.getRoot().split(",", -1);
		int groupItemLen = parts.length;
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < groupItemLen; i++){
			Map<String,Integer> map = dictionary.get(i + fieldNumberBase);	
			int code = 0;
			if (!map.containsKey(parts[i])){
				code = dictionaryCodeTop.get(i + fieldNumberBase);
				map.put(parts[i], code);
				dictionaryCodeTop.set(i + fieldNumberBase, code + 1);
			} else {
				code = map.get(parts[i]);
			}
			sb.append(",").append(String.valueOf(code));
		}
		treeRecord.setRoot(sb.substring(1));
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
