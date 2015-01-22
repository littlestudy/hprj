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
		
		public void showDictionary(){
			Iterator<Entry<Integer, String>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<Integer, String> entry = (Map.Entry<Integer, String>) iter.next();
				System.out.println("Code: " + entry.getKey() + ", Field: " + entry.getValue());
			}
		}
	}
	
	private List<Dictionary> bundle;
	
	public RDictionaryBundle(int dictAmount){ ////////////////
		bundle = new ArrayList<RDictionaryBundle.Dictionary>(dictAmount);
		for (int i = 0; i < dictAmount; i++)
			bundle.add(new Dictionary());
	}
	
	public RDictionaryBundle(List<String> dictionarys){ // 从字典数据中构造字典
	}
	
	public String search(int dictNum, int code){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.search(code);
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + i);
			bundle.get(i).showDictionary();
		}			
	}
}
