package org.v1.test.mergeinmemory.v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.v1.utils.mergeinmemory_v2.DictionaryBundle;
import org.v1.utils.mergeinmemory_v2.TreeRecord;

public class TreeRecordTest {
	
	private static List<TreeRecord> records;
	private static DictionaryBundle dictionaryBundle = new DictionaryBundle(6);
	
	public static void main(String[] args) {
		String str1 = "0a,1b##2c,3d##4e,5d";		
		String str2 = "0a,1b##2c,3d##4f,5f";	
		String str3 = "0c,1b##2c,3e##4f,5g";	
		List<String> values = new ArrayList<String>();
		values.add(str1);
		values.add(str2);
		values.add(str3);
		
		initRecords(values);
		
		merge(4);
		showRecords();
		
		System.out.println("-----------------------------------------------------------");
		merge(2);
		showRecords();
		
		System.out.println("-----------------------------------------------------------");
		codeLastRoot(0);
		showRecords();
		
		System.out.println("--- DictionaryBundle --------------------------------------");
		//dictionaryBundle.showDictionaries();
	}
	
	private static void initRecords(List<String> values){
		records = new ArrayList<TreeRecord>();
		for (String str : values)
			records.add(TreeRecord.fromOriginalString(str));		
	}
	
	private static void merge(int fieldNumberBase){
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
			//System.out.println(treeList.size());
		}
		
		records.clear();
		for (String k : map.keySet())
			records.add(TreeRecord.generateTreeRecodeFromTreeList(k, map.get(k)));
		
		map = null;
	}
	
	private static void generateDictItems(final TreeRecord treeRecord, int fieldNumberBase) {
		String parts[] = treeRecord.getRoot().split(",", -1);
		int groupItemLen = parts.length;
		StringBuilder sb = new StringBuilder();	
		//System.out.println("root: " + treeRecord.getRoot());
		for (int i = 0; i < groupItemLen; i++){
			int fieldNumber = i + fieldNumberBase;			
			int code = dictionaryBundle.find(fieldNumber, parts[i]);			
			sb.append(",").append(String.valueOf(code));
			//System.out.println("fieldNumber: " + fieldNumber + " field: " + parts[i] + " code: " + code);
		}
		treeRecord.setRoot(sb.substring(1));
		sb = null;
	}
	
	private static void codeLastRoot(int fieldNumberBase){ // 只是对第一字段组编码
		for (TreeRecord treeRecord : records)	{			
			generateDictItems(treeRecord, fieldNumberBase); // 对当前的treeRecord的root进行编码
		}
	}
	
	public static void showRecords(){
		for (TreeRecord record : records){
			System.out.println(record.toString());
		}		
	}
}
