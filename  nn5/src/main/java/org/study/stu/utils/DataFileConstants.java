package org.study.stu.utils;

public class DataFileConstants {

	private DataFileConstants(){}
	
	public static final byte VERSION = 1;
	public static final byte[] MAGIC = new byte[] {
		(byte)'S', (byte)'t', (byte)'u', VERSION
	};
	public static final int SYNC_SIZE = 16;
	public static final int DEFAULT_SYNC_INTERVAL = 1000*SYNC_SIZE; 
	
	
	  public static final String CODEC = "avro.codec";
  public static final String NULL_CODEC = "null";
  public static final String DEFLATE_CODEC = "deflate";
  public static final String SNAPPY_CODEC = "snappy";
  public static final String BZIP2_CODEC = "bzip2";
  
  public static final String GROUP_BUNDLE = "GroupBuneld";
  public static final String GROUP_BUNDLE_SEPARATOR = "Separator";
  public static final String CONF_OUTPUT_CODEC = "codec";
}
