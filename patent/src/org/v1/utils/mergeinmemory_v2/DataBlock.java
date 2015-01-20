package org.v1.utils.mergeinmemory_v2;

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

	public List<String> getmRecords() {
		return mRecords;
	}

	public void setmRecords(List<String> mRecords) {
		this.mRecords = mRecords;
	}
}
