package org.v3.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CDictionaryBundle {
	
	/*
	 * key和code是1对1关系
	 */	
	private class Dictionary{
		private int codeTop;
		private HashMap<String, Integer> map;
		
		public Dictionary(){
			codeTop = 0;
			map = new HashMap<String, Integer>();
		}
		
		public Integer find(String key){
			Integer code = map.get(key);
			if (code == null) {
				map.put(key, codeTop);
				code = codeTop;
				codeTop++;
			}
			return code;
		}
		
		public Integer search(String key){
			Integer code = map.get(key);
			if (code == null)
				return -1;
			
			return code;
		}
		
		public void showDictionary(){
			Iterator<Entry<String, Integer>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				System.out.println("Item: " + entry.getKey() + ", Code: " + entry.getValue());
			}
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			Iterator<Entry<String, Integer>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				sb.append(",").append(entry.getKey()).append(",").append(String.valueOf(entry.getValue()));	
			}
			
			return sb.substring(1);
		}		
	}
	
	private List<Dictionary> bundle;
	
	public CDictionaryBundle(int dictAmount){
		bundle = new ArrayList<CDictionaryBundle.Dictionary>(dictAmount);
		for (int i = 0; i < dictAmount; i++)
			bundle.add(new Dictionary());
	}
	
	public CDictionaryBundle(List<String> dictionarys){ // 从字典数据中构造字典
	}
	
	public Integer find(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.find(key);
	}
	
	public Integer search(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.search(key);
	}
	
	public int getDictionaryAmount(){
		return bundle.size();
	}
	
	public String dictionaryToString(int dicNum){
		return bundle.get(dicNum).toString();
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + i);
			bundle.get(i).showDictionary();
		}			
	}
}
