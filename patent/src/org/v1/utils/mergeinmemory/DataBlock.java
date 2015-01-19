package org.v1.utils.mergeinmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataBlock {
	private List<Map<String, Integer>> mDictionary;
	private List<String> mRecords;
	
	public DataBlock(List<Map<String, Integer>> dictionary, List<TreeRecord> records) {
		super();
		mDictionary = dictionary;
		mRecords = new ArrayList<String>(records.size());
		for (TreeRecord treeRecord : records){
			mRecords.add(treeRecord.getTree());
		}
	}

	public List<Map<String, Integer>> getmDictionary() {
		return mDictionary;
	}

	public void setmDictionary(List<Map<String, Integer>> mDictionary) {
		this.mDictionary = mDictionary;
	}

	public List<String> getmRecords() {
		return mRecords;
	}

	public void setmRecords(List<String> mRecords) {
		this.mRecords = mRecords;
	}
}
