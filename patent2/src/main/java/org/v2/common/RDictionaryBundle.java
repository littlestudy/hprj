package org.v2.common;

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
		private int mDictionaryNumber;
		
		public Dictionary(int dictionaryNumber, String dictionaryStr){
			mDictionaryNumber = dictionaryNumber;
			map = new HashMap<Integer, String>();
			
			String [] parts = dictionaryStr.split(",", -1);
			for (int i = 0; i < parts.length; i++, i++){				
				map.put(Integer.valueOf(parts[i + 1]), parts[i]);
			}
		}
		
		public String search(int code){
			return map.get(code);
		}
		
		public int getDictionaryNumber() {
			return mDictionaryNumber;
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
	
	public String search(int dictNum, int code){
		Dictionary dictionary = null;
		for (int i = 0; i < bundle.size(); i++){
			dictionary = bundle.get(dictNum);
			if (dictionary.getDictionaryNumber() == dictNum)
				return dictionary.search(code);
		}
		return null;
	}
	
	public void addDictionary(int dictionaryNumber, String dictString){
		Dictionary dictionary = new Dictionary(dictionaryNumber, dictString);		
		bundle.add(dictionary);
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + bundle.get(i).getDictionaryNumber());
			bundle.get(i).showDictionary();
		}			
	}
}
