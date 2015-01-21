package org.v1.utils.mimwd;

import java.util.ArrayList;
import java.util.List;

public class DataBlock {
	private DictionaryBundle mDictionaryBundle;
	private List<String> mRecords;
	
	public DataBlock(DictionaryBundle dictionaryBundle, List<TreeRecord> records) {
		super();
		mDictionaryBundle = dictionaryBundle;
		mRecords = new ArrayList<String>(records.size());
		for (TreeRecord treeRecord : records){
			mRecords.add(treeRecord.getTree());
		}
	}

	public DictionaryBundle getDictionaryBundle() {
		return mDictionaryBundle;
	}

	public void setDictionaryBundle(DictionaryBundle dictionaryBundle) {
		this.mDictionaryBundle = dictionaryBundle;
	}

	public List<String> getRecords() {
		return mRecords;
	}

	public void setRecords(List<String> mRecords) {
		this.mRecords = mRecords;
	}
	
	public void showDictionaryBundle(){
		mDictionaryBundle.showDictionaries();
	}
	
	public void showRecords(){
		for (String str : mRecords)
			System.out.println(str);
	}
}
