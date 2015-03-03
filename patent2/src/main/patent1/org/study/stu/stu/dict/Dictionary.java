package org.study.stu.stu.dict;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Dictionary{
	private int dictionaryNumber;
	private HashMap<Object, Object> map = new HashMap<Object, Object>();
	private int codeTop;
		
	public Object search(Object key){
		return map.get(key);
	}
	
	public Object find(Object key){
		Object value = map.get(key);
		if (value == null) {
			map.put(key, codeTop);
			value = codeTop;
			codeTop++;
		}
		return value;
	}
	
	public void createFromString(String dictionaryStr){
		String [] parts = dictionaryStr.split(",", -1);
		for (int i = 0; i < parts.length; i++, i++){				
			map.put(Integer.valueOf(parts[i + 1]), parts[i]);
		}
	}
	
	public int getDictionaryNumber() {
		return dictionaryNumber;
	}

	public void setDictionaryNumber(int dictionaryNumber) {
		this.dictionaryNumber = dictionaryNumber;
	}

	public void showDictionary(){ 
		Iterator<Entry<Object, Object>> iter = map.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) iter.next();
			System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
		}
	}
}
