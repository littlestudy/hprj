package org.study.stu.stu;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.study.stu.stu.dict.CDictionaryBundle;
import org.study.stu.utils.SyncGenerator;

public class DataBlock {
	private CDictionaryBundle mDictionaryBundle;
	private List<String> mRecords;
	private GroupBundle mGroupBundle;
	
	private byte [] recordSync = SyncGenerator.getMD5Sync();
	
	public DataBlock(GroupBundle groupBundle, CDictionaryBundle dictionaryBundle, List<TreeRecord> records) {
		super();
		mGroupBundle = groupBundle;
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
	
	public GroupBundle getGroupBundle() {
		return mGroupBundle;
	}

	public void setGroupBundle(GroupBundle groupBundle) {
		this.mGroupBundle = groupBundle;
	}

	public void showGroupBundle(){
		mGroupBundle.showGroupBundle();
	}
	public void showDictionaryBundle(){
		mDictionaryBundle.showDictionaries();
	}
	
	public void showRecords(){
		for (String str : mRecords)
			System.out.println(str);
	}
	
	public void save(String path) throws Exception{
		//String dataPath = TestConstant.TEST_RESOURCES_DIR + path;
		String dataPath = path;	
		PrintStream ps = new PrintStream(dataPath);	
		ps.println(mGroupBundle.getGroupAmount() + "," + mGroupBundle.getGroupSeparator());
		for (int i = mGroupBundle.getGroupAmount(); i > 0; i--){			
			ps.println(StringUtils.join(mGroupBundle.getGroup(i - 1), ","));
			//System.out.println(StringUtils.join(mGroupBundle.getGroup(i), ","));
		}	
		
		ps.println(mDictionaryBundle.getDictionaryAmount());
		for (int i = 0; i < mDictionaryBundle.getDictionaryAmount(); i++){			
			ps.println(mDictionaryBundle.dictionaryToString(i));
			//System.out.println(mDictionaryBundle.dictionaryToString(i));
		}	
		
		ps.println(mRecords.size());
		for (int i = 0; i < mRecords.size(); i++){			
			ps.println(mRecords.get(i));
			ps.write(recordSync);
			//System.out.println(mRecords.get(i));
		}
		ps.close();		
	}
}
