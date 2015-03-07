package org.study.stu.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.study.stu.common.dict.CDictionaryBundle;

public class MergeInMemoryWithDict {
	
	private List<TreeRecord> records;	
	private CDictionaryBundle dictionaryBundle;
	private GroupBundle groupBundle;
	
	public MergeInMemoryWithDict(String groupBundleStr, String groupSeparator){		
		groupBundle = new GroupBundle(groupBundleStr, groupSeparator);		
		dictionaryBundle = new CDictionaryBundle(groupBundle.getFieldAmount());
	}
	
	public DataBlock mergeInMemory(Iterator<Text> iter, int recordNumber){
		initRecords(iter, recordNumber);		
		
		for (int i = groupBundle.getGroupAmount(); i > 1; i--){ // 从后向前归并，最后group中第一个字段组不用归并
			int fieldNumberBase = groupBundle.getGroupIndexBase(i - 1);
			merge(fieldNumberBase);
		}
		codeLastRoot(0); //第一个字段组不用归并，只编码
		return new DataBlock(groupBundle, dictionaryBundle, records);
	}
	
	private void merge(int fieldNumberBase){		
		Map<String, List<TreeRecord>> map = new HashMap<String, List<TreeRecord>>();		
		for (TreeRecord treeRecord : records)	{
			String key = treeRecord.getKey();	
		
			generateDictItems(treeRecord, fieldNumberBase); // 对当前的treeRecord的root进行编码
			
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

	private void initRecords(Iterator<Text> iter, int recordNumber){
		if (records == null)
			records = new ArrayList<TreeRecord>();
		
		for (int i = 0; i < recordNumber; i++){
			if (iter.hasNext())
				records.add(TreeRecord.fromOriginalString(iter.next().toString()));
			else
				break;
		}					
	}	
	
	public void clear(){
		if (records != null)
			records.clear();
		dictionaryBundle = null;
		dictionaryBundle =  new CDictionaryBundle(groupBundle.getFieldAmount());
	}
}
