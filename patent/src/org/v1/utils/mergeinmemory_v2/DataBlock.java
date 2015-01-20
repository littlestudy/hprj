package org.v1.utils.mergeinmemory_v2;

import java.util.ArrayList;
import java.util.List;

public class DataBlock {
	private CDictionaryBundle mDictionaryBundle;
	private List<String> mRecords;
	
	public DataBlock(CDictionaryBundle dictionaryBundle, List<TreeRecord> records) {
		super();
		mDictionaryBundle = dictionaryBundle;
		mRecords = new ArrayList<String>(records.size());
		for (TreeRecord treeRecord : records){
			mRecords.add(treeRecord.getTree());
		}
	}

	public CDictionaryBundle getDictionaryBundle() {
		return mDictionaryBundle;
	}

	public void setDictionaryBundle(CDictionaryBundle dictionaryBundle) {
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
