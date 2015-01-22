package org.v1.utils.im.rimwd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RDictionaryBundle {
	
	/*
	 * field和code是1对1关系
	 */	
	private class Dictionary{
		private HashMap<Integer, String> map;
		
		public Dictionary(){
			map = new HashMap<Integer, String>();
		}
		
		public String search(int code){
			return map.get(code);
		}
		
		public void addItem(int code, String field){
			map.put(code, field);
		}
		
		public void showDictionary(){
			Iterator<Entry<Integer, String>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<Integer, String> entry = (Map.Entry<Integer, String>) iter.next();
				System.out.println("Code: " + entry.getKey() + ", Field: " + entry.getValue());
			}
		}
	}
	
	private List<Dictionary> bundle = new ArrayList<RDictionaryBundle.Dictionary>();
	
	public RDictionaryBundle(List<String> dictionarys){ // 从字典数据中构造字典
	}
	
	public String search(int dictNum, int code){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.search(code);
	}
	
	public void addDictionary(String dictString){
		Dictionary dictionary = new Dictionary();
		
		String [] parts = dictString.split(",", -1);
		for (int i = 0; i < parts.length; i++, i++)
			dictionary.addItem(Integer.valueOf(parts[i + 1]), parts[i]);
		
		bundle.add(dictionary);
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + i);
			bundle.get(i).showDictionary();
		}			
	}
}
