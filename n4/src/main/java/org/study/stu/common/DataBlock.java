package org.study.stu.common;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.study.stu.common.dict.CDictionaryBundle;

public class DataBlock {
		
	public static final String DICTIONARY_AMOUNT = "DA";
	public static final String DICTIONARY = "D";
	public static final String RECORDS = "R";
	public static final String GROUP_BUNDLE = "G";
	public static final String RECORD_AMOUNT = "RA";
	
	private static Schema SCHEMA = null;
	
	private CDictionaryBundle mDictionaryBundle;
	private List<String> mRecords;
	private GroupBundle mGroupBundle;
	
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
			ps.println(mGroupBundle.getGroupBundleStr());
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
			//System.out.println(mRecords.get(i));
		}
		ps.close();		
	}
	
	public static Schema getRecordSchema(int dictAmount){
		if (SCHEMA == null) {
			StringBuilder sb = new StringBuilder();
			sb.append("{\"type\": \"record\", \"name\": \"DataBlock\", \"fields\":[");
		
			sb.append("{\"name\":\"" + GROUP_BUNDLE + "\", \"type\":\"bytes\"},");
		
			sb.append("{\"name\":\""+ DICTIONARY_AMOUNT +"\", \"type\":\"int\"},");
						
			for (int i = 0; i < dictAmount; i++){
				sb.append("{\"name\":\"" + DICTIONARY);
				sb.append(Integer.toString(i));
				sb.append( "\", \"type\":\"bytes\"},");
			}
		
			sb.append("{\"name\":\""+ RECORD_AMOUNT +"\", \"type\":\"int\"},");
			sb.append("{\"name\":\"" + RECORDS + "\", \"type\":\"bytes\"}");
		
			sb.append("]}");
			SCHEMA = new Schema.Parser().parse(sb.toString());			
		}
				
		return SCHEMA;
	}
	
	public GenericRecord toRecord() throws IOException{
		Schema schema = getRecordSchema(mDictionaryBundle.getDictionaryAmount());
		
		GenericRecord record = new GenericData.Record(schema);
		
		record.put(GROUP_BUNDLE, mGroupBundle.toByteBuffer());
		
		record.put(DICTIONARY_AMOUNT , mDictionaryBundle.getDictionaryAmount());
		for (int i = 0; i < mDictionaryBundle.getDictionaryAmount(); i++)
			record.put(DICTIONARY + i, mDictionaryBundle.getByteBufferFromDict(i));
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);		
		for (int i = 0; i < mRecords.size(); i++){
			byte[] recordBytes = mRecords.get(i).getBytes("UTF8");
			dataOutput.writeInt(recordBytes.length);
			dataOutput.write(recordBytes, 0, recordBytes.length);
		}
		
		record.put(RECORD_AMOUNT, mRecords.size());
		record.put(RECORDS, ByteBuffer.wrap(baos.toByteArray()));
		
		baos.close();
		baos = null;
		dataOutput = null;
		
		return record;
	}
}
