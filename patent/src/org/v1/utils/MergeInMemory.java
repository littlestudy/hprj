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
	private int index; // the nextKey is from 0 to index in this.key.
	
	private static final String separator = "##";
	
	public TreeRecord(String key, String value) {
		this.key = key;
		this.value = value;
		setIndex();
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
		if (index == -1)
			return new TreeRecord(str, "");
		
		return new TreeRecord(str.substring(0, index), str.substring(index));
	}
	
	public String getNextKey(){
		String nextKey = null;
		if (index == -1) // 对于第一个字段组的情况
			nextKey = "(" + key + ")" + value;
		else 
			nextKey = key.substring(0, index);
		
		return nextKey;
	}
	
	public String getNextValue(){
		String nextValue = null;
		if (index == -1)  // 对于第一个字段组的情况
			nextValue = null;
		else 		
			nextValue = "(" + key.substring(index + separator.length()) + ")" + value; 
		
		return nextValue;
	}
	
	private void setIndex(){
		if (!key.contains(separator)) // 对于第一个字段组的情况
			index = -1;
		else
			index = key.lastIndexOf(separator);
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
		
		//merge();
		//showRecords();
		
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
				newValue = newValue + setValue;				
			} 
			
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
		records = new ArrayList<TreeRecord>();
		for (String str : values)
			records.add(TreeRecord.fromString(str));		
	}
	
	public static void showRecords(){
		for (TreeRecord record : records){
			System.out.println(record.getKey() + " === " + record.getValue());
			System.out.println(record.getNextKey() + " --- " + record.getNextValue());
		}		
	}
}
