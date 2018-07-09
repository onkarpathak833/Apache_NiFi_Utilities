import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFlowFileWriter;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;




public class ConvrtAvroToOrc {

	public static void convertAvroToOrc(InputStream ins) {

		long startTime = System.currentTimeMillis();
		final long stripeSize = 100000; // context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
		final int bufferSize = 256; // context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
		//final CompressionKind compressionType = CompressionKind.valueOf("SNAPPY"); // CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
		 CompressionKind compressionType = CompressionKind.SNAPPY;
		
		final AtomicReference<Schema> hiveAvroSchema = new AtomicReference<>(null);
		final AtomicInteger totalRecordCount = new AtomicInteger(0);
		 String fileName = "//Users//techops//Documents//test.orc";// flowFile.getAttribute(CoreAttributes.FILENAME.key());
		/*public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
	            .name("orc-stripe-size")
	            .displayName("Stripe Size")
	            .description("The size of the memory buffer (in bytes) for writing stripes to an ORC file")
	            .required(true)
	            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
	            .defaultValue("64 MB")
	            .build();*/
		try {
			final InputStream in = new BufferedInputStream(ins);

			OutputStream out = new ByteArrayOutputStream();
			final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>());

			// Create ORC schema from Avro schema
			Schema avroSchema = reader.getSchema();

			TypeInfo orcSchema = NiFiOrcUtils.getOrcField(avroSchema);
			Configuration orcConfig = new Configuration();
			if (orcConfig == null) {
				orcConfig = new Configuration();
			}
			//WriterOptions opts = OrcFile.writerOptions(orcConfig);
			//opts.compress(compressionType);

			//opts.stripeSize(125675);
			//opts.bufferSize(102456);
			//opts.blockPadding(true);
			
			//Writer orcFileWriter = OrcFile.createWriter(new Path(fileName), opts);
			
			
			OrcFlowFileWriter orcWriter = NiFiOrcUtils.createWriter(
			out,
			 new Path(fileName),
			 orcConfig,
			 orcSchema,
			 stripeSize,
			 compressionType,
			 bufferSize);
			 
			 //TypeDescription schema = TypeDescription.fromString("struct");
			 /*Writer writer1 = OrcFile.createWriter(new Path(fileName),
						OrcFile.writerOptions(orcConfig)
								.setSchema(orcSchema));
			 */
			 
			try {
				File file = new File(fileName);
				if(!file.exists()){
					
					file.createNewFile();
				}
				FileWriter writer = new FileWriter(file);
				
				
				FileOutputStream fileOut = new FileOutputStream(file);
				
				            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
				
				          

				
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
								//objectOut.writeObject(row[i]);
							} catch (ArrayIndexOutOfBoundsException aioobe) {
								System.out.println("Wxception while convrting Avro to ORC.");
							}
						}
						//OrcStruct orcStruct = NiFiOrcUtils.createOrcStruct(orcSchema, row);
						//orcFileWriter.addRow(row);
						//objectOut.writeObject(orcStruct);
						//NiFiOrcUtils.createWriter(6, arg1, arg2, arg3, arg4, arg5, arg6)
//						orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema,
//						row));
						OrcStruct struct = NiFiOrcUtils.createOrcStruct(orcSchema, row);
						orcWriter.addRow(struct);
						//orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema, row));
						recordCount++;
					}
				}
				hiveAvroSchema.set(avroSchema);
				totalRecordCount.set(recordCount);

			} catch (Exception e) {
				System.out.println(e);
			} finally {
				//orcWriter.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}
	}

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		InputStream in = new FileInputStream(new File("//Users//techops//Documents//twitter.avro"));
		convertAvroToOrc(in);
	}

}
