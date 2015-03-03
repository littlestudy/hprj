package org.study.stu.stu.dict;

import java.util.ArrayList;
import java.util.List;

public class RDictionaryBundle {
	
	private List<Dictionary> bundle = new ArrayList<Dictionary>();
	
	public String search(int dictNum, int code){
		Dictionary dictionary = null;
		for (int i = 0; i < bundle.size(); i++){
			dictionary = bundle.get(dictNum);
			if (dictionary.getDictionaryNumber() == dictNum)
				return (String) dictionary.search(code);
		}
		return null;
	}
	
	public void addDictionary(int dictionaryNumber, String dictString){
		Dictionary dictionary = new Dictionary();		
		dictionary.setDictionaryNumber(dictionaryNumber);
		bundle.add(dictionary);
	}
	
	public void showDictionaries(){
		for (int i = 0; i < bundle.size(); i++){
			System.out.println("Dictionary Number: " + bundle.get(i).getDictionaryNumber());
			bundle.get(i).showDictionary();
		}			
	}
}
