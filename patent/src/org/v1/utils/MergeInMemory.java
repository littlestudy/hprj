package org.v1.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class TreeRecord{
	private String key;
	private String value;
	
	private String nextKey;
	private String nextValue;
	
	private static final String separator = "##";
	
	public TreeRecord(String key, String value) {
		this.key = key;
		this.value = value;
		setNextKeyAndNextValue();
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getValue() {
		return value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return key + value;
	}	
	
	public static TreeRecord fromString(String str){
		int index = str.indexOf("[");
		return new TreeRecord(str.substring(0, index), str.substring(index));
	}
	
	public String getNextKey(){
		return nextKey;
	}
	
	public String getNextValue(){
		return nextValue;
	}
	
	private void setNextKeyAndNextValue(){
		if (!key.contains(separator)) { // 对于第一个字段组的情况
			nextKey = "(" + key + ")" + value;
			nextValue = null;
		}
		int lastindex = key.lastIndexOf(separator);
		nextKey = key.substring(0, lastindex);
		nextValue = "(" + key.substring(lastindex + separator.length()) + ")"; 
	}
}

public class MergeInMemory {
	
	private static List<TreeRecord> records;
	private static boolean isFinish = false;
	private static List<String> list = null;
	
	public static List<String> mergeInMemory(List<String> values){
		initRecords(values);		
		
		while (!isFinish)
			merge();
		
		return list;
	}
	
	private static void merge(){		
		if (records.get(0).getNextValue() == null){
			mergeLastTime();
			return;
		}
		
		Map<String, String> map = new HashMap<String, String>();		
		for (TreeRecord originTreeRecord : records)	{
			String newKey = originTreeRecord.getNextKey();	
			String newValue = originTreeRecord.getNextValue();
			if(map.containsKey(newKey)){
				String setValue = map.get(newKey);
				setValue = newValue + setValue;
			} else 
				map.put(newKey, newValue);
		}
		
		records.clear();
		for (String k : map.keySet())
			records.add(new TreeRecord(k, "[" + map.get(k) + "]"));	
		
		map = null;
	}

	private static void mergeLastTime(){
		HashSet<String> hashSet = new HashSet<String>();
		for (TreeRecord record : records){
			hashSet.add(record.getNextKey());
		}
		records = null;
		Iterator<String> it = hashSet.iterator();
		list = new ArrayList<String>();
		while (it.hasNext()){
			list.add(it.next());
		}
		isFinish = true;
		
		hashSet = null;
	}
	
	private static void initRecords(List<String> values){
		for (String str : values)
			records.add(TreeRecord.fromString(str));		
	}
}
