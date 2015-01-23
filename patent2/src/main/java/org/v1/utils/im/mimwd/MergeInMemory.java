package org.v1.utils.im.mimwd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.v1.utils.im.mimwd.TreeRecord;

public class MergeInMemory {
	
	private List<TreeRecord> records;	
	private CDictionaryBundle dictionaryBundle;
	private GroupBundle groupBundle;
	
	/*
	 * group中的顺序是字段组归并的顺序
	 * 如group {array1, array2, array3}，array1先被归并，然后是array2，最后是array3
	 * 一般重复率高的归并顺序靠后
	 */
	public MergeInMemory(List<String[]> group){		
		groupBundle = new GroupBundle(group);		
		dictionaryBundle = new CDictionaryBundle(groupBundle.getFieldAmount());
	}
	
	public DataBlock mergeInMemory(List<String> values){
		initRecords(values);		
		
		for (int i = 0; i < groupBundle.getGroupAmount() - 1; i++){
			int fieldNumberBase = groupBundle.getGroupIndexBase(i);
			merge(fieldNumberBase);
		}
		//codeLastRoot(0);
		return new DataBlock(groupBundle, dictionaryBundle, records);
	}
	
	private void merge(int fieldNumberBase){	
		Map<String, List<TreeRecord>> map = new HashMap<String, List<TreeRecord>>();		
		for (TreeRecord treeRecord : records)	{
			String key = treeRecord.getKey();	
			
			//generateDictItems(treeRecord, fieldNumberBase); // 对当前的treeRecord的root进行编码
			
			List<TreeRecord> treeList = map.get(key);			
			if(treeList == null){
				treeList = new ArrayList<TreeRecord>();
				map.put(key, treeList);
			}			
			
			treeList.add(treeRecord);
		}
		
		records.clear();
		for (String k : map.keySet())
			records.add(TreeRecord.generateTreeRecodeFromTreeList(k, map.get(k)));
		
		map = null;
	}
	
	/*
	 * 虽然多循环一次，但总的编字典的hashmap数量没变，因为都是每个字段都只hashmap一次
	 */
	@SuppressWarnings("unused")
	private void codeLastRoot(int fieldNumberBase){ // 只是对第一字段组编码
		for (TreeRecord treeRecord : records)	{			
			generateDictItems(treeRecord, fieldNumberBase); // 对当前的treeRecord的root进行编码
		}
	}
	
	private void generateDictItems(final TreeRecord treeRecord, int fieldNumberBase) {
		String parts[] = treeRecord.getRoot().split(",", -1);
		int groupItemLen = parts.length;
		StringBuilder sb = new StringBuilder();		
		for (int i = 0; i < groupItemLen; i++){
			int fieldNumber = i + fieldNumberBase;			
			int code = dictionaryBundle.find(fieldNumber, parts[i]);			
			sb.append(",").append(String.valueOf(code));
		}
		treeRecord.setRoot(sb.substring(1));
	}

	private void initRecords(List<String> values){
		records = new ArrayList<TreeRecord>();
		for (String str : values)
			records.add(TreeRecord.fromOriginalString(str));		
	}	
}
