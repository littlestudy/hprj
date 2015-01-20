package org.v1.utils.mergeinmemory_v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DictionaryBundle {
	
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
		
		public void showDictionary(){
			Iterator<Entry<String, Integer>> iter = map.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				System.out.println("Item: " + entry.getKey() + ", Code: " + entry.getValue());
			}
		}
	}
	
	private List<Dictionary> bundle;
	
	public DictionaryBundle(int dictAmount){
		bundle = new ArrayList<DictionaryBundle.Dictionary>(dictAmount);
		for (int i = 0; i < dictAmount; i++)
			bundle.add(new Dictionary());
	}
	
	public Integer find(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return dictionary.find(key);
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + i);
			bundle.get(i).showDictionary();
		}
			
	}
}
