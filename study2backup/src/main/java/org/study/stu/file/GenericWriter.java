package org.study.stu.file;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.Codec;
import org.study.stu.StuRuntimeException;
import org.study.stu.common.DataBlock;
import org.study.stu.utils.DataFileConstants;
import org.study.stu.utils.SyncGenerator;

public class GenericWriter implements Closeable, Flushable{

	private boolean isOpen;	
	private BufferedOutputStream vout;
	private DataOutput dataOutput;
	private Codec codec;
	private final Map<String,byte[]> meta = new HashMap<String,byte[]>();  
	private byte[] sync;      
	
	public GenericWriter create(File file) throws IOException{
		return create(new FileOutputStream(file));
	}
	
	public GenericWriter create(OutputStream outs) throws IOException{
		assertNotOpen();
		vout = new BufferedOutputStream(outs);
		
		dataOutput = new DataOutputStream(outs);
		this.sync = SyncGenerator.getMD5Sync();

		if (this.codec == null) {
			this.codec = NullCodec.INSTANCE;
		}
		
		dataOutput.write(DataFileConstants.MAGIC, 0, DataFileConstants.MAGIC.length);
		dataOutput.writeInt(meta.size());
		for (Map.Entry<String, byte[]> entry : meta.entrySet()){
			dataOutput.writeUTF(entry.getKey());
			dataOutput.writeInt(entry.getValue().length);
			dataOutput.write(entry.getValue(), 0, entry.getValue().length);
		}
		dataOutput.write(sync, 0, sync.length);			
		
		this.isOpen = true;
		
		return this;		
	}
	
	public void append(DataBlock data) throws IOException{
		assertOpen();
		data.compressUsing(codec);
		data.write(dataOutput);
		dataOutput.write(sync, 0, sync.length);
	}
	
	public GenericWriter setCodec(Codec c) {
		assertNotOpen();
		this.codec = c;
		setMetaInternal(DataFileConstants.CONF_OUTPUT_CODEC, codec.getName());
		return this;
	}
	
	public GenericWriter setGroupBundle(String groupBundle, String separator){
		assertNotOpen();
		System.out.println("--write gb: " + groupBundle);
		setMetaInternal(DataFileConstants.GROUP_BUNDLE, groupBundle);
		setMetaInternal(DataFileConstants.GROUP_BUNDLE_SEPARATOR, separator);
		return this;
	}
	
	private GenericWriter setMetaInternal(String key, byte[] value) {
		assertNotOpen();
		meta.put(key, value);
		return this;
	}
	
	private GenericWriter setMetaInternal(String key, String value) {
		try {
			return setMetaInternal(key, value.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void assertOpen() {
		if (!isOpen) throw new StuRuntimeException("not open");
	}
	
	private void assertNotOpen() {
		if (isOpen) throw new StuRuntimeException("already open");
	}	
	
	@Override
	public void flush() throws IOException {
		vout.flush();		
	}

	@Override
	public void close() throws IOException {
		if (isOpen){
			vout.close();
			isOpen = false;
		}		
	}
}
