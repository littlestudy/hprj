package org.study.stu.stu.dict;

import java.util.ArrayList;
import java.util.List;

public class CDictionaryBundle {
	
	private List<Dictionary> bundle;
	
	public CDictionaryBundle(int dictAmount){
		bundle = new ArrayList<Dictionary>(dictAmount);
		for (int i = 0; i < dictAmount; i++)
			bundle.add(new Dictionary());
	}
	
	public Integer find(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return (Integer) dictionary.find(key);
	}
	
	public Integer search(int dictNum, String key){
		Dictionary dictionary = bundle.get(dictNum);
		return (Integer) dictionary.search(key);
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
