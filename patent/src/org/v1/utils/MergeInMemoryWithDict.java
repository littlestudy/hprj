package org.v1.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class MergeInMemoryWithDict {
	
	private static List<TreeRecord> records;
	private static boolean isFinish = false;
	private static List<String> list = null;
		
	//private static List<Map<String, Integer>> dictionary = new ArrayList<Map<String,Integer>>();
	
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
