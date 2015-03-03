package org.study.stu.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.study.stu.stu.DataBlock;
import org.study.stu.stu.GroupBundle;
import org.study.stu.utils.Constant;

public class NELogAvroFileConverter extends AvroFileConverterBase{

	public NELogAvroFileConverter() {
		super(new GroupBundle(Constant.getNELogFiledGroup()));
	}

	@Override
	public GenericRecord convertBlockToAvroRecord(DataBlock dataBlock) {
		GenericRecord record = new GenericData.Record(new Schema.Parser().parse(createRecordJsonSchema()));
		return null;
	}

	@Override
	public String createRecordJsonSchema() {
		String SCHEMA_JSON =
			"{\"type\": \"record\", \"name\": \"SmallFilesTest\", "
          + "\"fields\": ["
          + "{\"name\":\"" //+ FIELD_FILENAME
          + "\", \"type\":\"string\"},"
          + "{\"name\":\"" //+ FIELD_CONTENTS
          + "\", \"type\":\"bytes\"}]}";
		return null;
	}

}
