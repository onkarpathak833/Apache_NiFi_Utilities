import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class ConvrtAvroToOrc {

	public static void convertAvroToOrc(InputStream ins){
		
		
		long startTime = System.currentTimeMillis();
        final long stripeSize = 100000; //context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
        final int bufferSize = 256; //context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final CompressionKind compressionType = CompressionKind.valueOf("SNAPPY"); //CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
        final AtomicReference<Schema> hiveAvroSchema = new AtomicReference<>(null);
        final AtomicInteger totalRecordCount = new AtomicInteger(0);
        final String fileName = "test.orc";//flowFile.getAttribute(CoreAttributes.FILENAME.key());
        	
            try (final InputStream in = new BufferedInputStream(ins);
            		OutputStream out = new ByteArrayOutputStream();
                 final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>())) {

                // Create ORC schema from Avro schema
                Schema avroSchema = reader.getSchema();

                TypeInfo orcSchema = NiFiOrcUtils.getOrcField(avroSchema);
                Configuration orcConfig = new Configuration();
                if (orcConfig == null) {
                    orcConfig = new Configuration();
                }

//               /* OrcFlowFileWriter orcWriter = NiFiOrcUtils.createWriter(
//                        out,
//                        new Path(fileName),
//                        orcConfig,
//                        orcSchema,
//                        stripeSize,
//                        compressionType,
//                        bufferSize);*/
                try {

                    int recordCount = 0;
                    GenericRecord currRecord = null;
                    while (reader.hasNext()) {
                        currRecord = reader.next(currRecord);
                        List<Schema.Field> fields = currRecord.getSchema().getFields();
                        if (fields != null) {
                            Object[] row = new Object[fields.size()];
                            for (int i = 0; i < fields.size(); i++) {
                                Schema.Field field = fields.get(i);
                                Schema fieldSchema = field.schema();
                                Object o = currRecord.get(field.name());
                                try {
                                    row[i] = NiFiOrcUtils.convertToORCObject(NiFiOrcUtils.getOrcField(fieldSchema), o);
                                } catch (ArrayIndexOutOfBoundsException aioobe) {
                                    System.out.println("Wxception while convrting Avro to ORC.");
                                }
                            }
                            //orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema, row));
                            recordCount++;
                        }
                    }
                    hiveAvroSchema.set(avroSchema);
                    totalRecordCount.set(recordCount);
                
		
		
		

            }
        }
        
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
