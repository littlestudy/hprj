package org.study.stu.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.Codec;
import org.apache.hadoop.io.Writable;
import org.study.stu.StuRuntimeException;
import org.study.stu.common.dict.CDictionaryBundle;
import org.study.stu.common.dict.RDictionaryBundle;

public class DataBlock implements Writable{
	
	private byte[] data;
	private int blockSize;	
	private int recordAmount = 0;
	
	private static int dictAmount = 0; // all datablock's dictionary's amount is the same
	private static GroupBundle groupBundle;
	
	public DataBlock(){		
	}
	
	public DataBlock(CDictionaryBundle dictionaryBundle, List<TreeRecord> records) throws IOException {
		
		dictAmount = dictionaryBundle.getDictionaryAmount();
		
		//System.out.println("record amount: " + records.size());		
		recordAmount = records.size();
				
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput dataOutput = new DataOutputStream(baos);
		
		//dataOutput.writeInt(dictAmount);
		dataOutput.writeInt(recordAmount);
		
		dictionaryBundle.writeAllDicts(dataOutput, baos.size());
	
		// one tree's string will exceed the dataOuput.writeUTF()'s size
		for (TreeRecord treeRecord : records){
			byte [] treeBytes = treeRecord.getTree().getBytes("UTF-8");
			dataOutput.writeInt(treeBytes.length);
			dataOutput.write(treeBytes, 0, treeBytes.length);
		}
				
		data = baos.toByteArray();
		blockSize = data.length;
		
		//ByteArrayInputStream bais = new ByteArrayInputStream(data);
		//DataInput dataInput = new DataInputStream(bais);
		//for (int i = 0; i < 7; i++)
			//System.out.println(dataInput.readInt());
		
		//showDataBlock1(dictionaryBundle, records);
	}
		
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(blockSize);
		out.write(data, 0, blockSize);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {		
		blockSize = in.readInt();
		data = new byte[blockSize];
		in.readFully(data);		
	}
	
	public boolean isAvailable(){
		return blockSize >= 0;
	}
	
	public static void setGroupBundleAndDictAmount(GroupBundle gBundle){
		groupBundle = gBundle;
		dictAmount = groupBundle.getFieldAmount();
	}
	
	public static GroupBundle getGroupBundle() {
		return groupBundle;
	}
	
	public void compressUsing(Codec c) throws IOException {
		if (blockSize < 0)
			return ;
		ByteBuffer result = c.compress(getAsByteBuffer());
		data = result.array();
		blockSize = result.remaining();
    }
    
	public void decompressUsing(Codec c) throws IOException {
		if (blockSize < 0)
			return ;
		ByteBuffer result = c.decompress(getAsByteBuffer());
		data = result.array();
		blockSize = result.remaining();
	}
	
	ByteBuffer getAsByteBuffer() {
		return ByteBuffer.wrap(data, 0, blockSize);
    }
	
	@Override
	public String toString() {
		return new String(data);
	}
	
	public int getBlockSize(){
		return this.blockSize;
	}
	
	public CDictionaryBundle getCDictionaryBundle() throws IOException{
		if (data == null)
			throw new StuRuntimeException("not a available DataBlock.");
		
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		DataInput dataInput = new DataInputStream(bais);

		//dictAmount = dataInput.readInt();
		recordAmount = dataInput.readInt();
		
		CDictionaryBundle bundle = new CDictionaryBundle(dictAmount);
		
		int [] dictOffset = new int[dictAmount + 1];
		for (int i = 1; i < dictAmount + 1; i++)
			dictOffset[i] = dataInput.readInt();
		
		int start = 4 + dictAmount * 4;
		int end = 0;
		for (int i = 1; i < dictAmount + 1; i++){
			end = dictOffset[i];
			bundle.setDictFromByteBuffer(i, ByteBuffer.wrap(data, start, end - start));
			start = end;
		}		
		
		return bundle;
	}
	
	public RDictionaryBundle getRDictionaryBundle() throws IOException{
		if (data == null)
			throw new StuRuntimeException("not a available DataBlock.");
		
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		DataInput dataInput = new DataInputStream(bais);

		recordAmount = dataInput.readInt();
		
		System.out.println("record Amount:" + recordAmount);
		System.out.println("dict Amount:" + dictAmount);
		
		RDictionaryBundle bundle = new RDictionaryBundle(dictAmount);
		
		int [] dictOffset = new int[dictAmount + 1];
		for (int i = 1; i < dictAmount + 1; i++){			
			dictOffset[i] = dataInput.readInt();			
		}		
		
		int start = 4 + dictAmount * 4;
		int end = 0;
		
		for (int i = 1; i < dictAmount + 1; i++){
			end = dictOffset[i];
			bundle.setDictFromByteBuffer(i - 1, ByteBuffer.wrap(data, start, end - start));
			start = end;
		}	
		//showDataBlock2(bundle);
		
		return bundle;
	}
	
	public List<String> getRecords() throws IOException{
		List<String> records = new ArrayList<String>();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		DataInput dataInput = new DataInputStream(bais);

		recordAmount = dataInput.readInt();
		
		System.out.println("record Amount:" + dictAmount);		
		
		int [] dictOffset = new int[dictAmount + 1];
		for (int i = 1; i < dictAmount + 1; i++){			
			dictOffset[i] = dataInput.readInt();			
		}
		
		//the size of [record count] + [dict offset array] is (4 + dictAmount * 4)
		bais.skip(dictOffset[dictAmount] - (4 + dictAmount * 4)); 
																
		for (int i = 0; i < recordAmount; i++) {
			int len = dataInput.readInt();
			byte[] recordByts = new byte[len];
			dataInput.readFully(recordByts);
			records.add(new String(recordByts, "UTF-8"));
		}
				
		return records;
	}
	
	public void showDataBlock1(CDictionaryBundle dictionaryBundle, List<TreeRecord> records){
		dictionaryBundle.showDictionaries();
		for (TreeRecord treeRecord : records){
			System.out.println(treeRecord.getTree());
		}	
	}
	
	public void showDataBlock2(RDictionaryBundle dictionaryBundle){
		dictionaryBundle.showDictionaries();
	}
	
	public void showInt() throws IOException{
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		DataInput dataInput = new DataInputStream(bais);
		System.out.println("showInt()");
		System.out.println(dataInput.readInt());
		System.out.println(dataInput.readInt());
		System.out.println(dataInput.readInt());
		System.out.println(dataInput.readInt());
		System.out.println(dataInput.readInt());
		System.out.println(dataInput.readInt());
		System.out.println(dataInput.readInt());
	}
}
