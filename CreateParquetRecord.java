package trparquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.simple.JSONObject;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class CreateParquetRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateParquetRecord.class);

   private static Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "/org/parquet/fileToParquet.avsc";
    private static final Path OUT_PATH = new Path("C:\\parquetDestination\\fileRecord6.parquet");
    private String mParquetFileToRead;
    private Path mParquetFileToReadPath;
    /*static {
        try (InputStream inStream = CreateParquetRecord.class.getResourceAsStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (IOException e) {
            LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }*/

    public CreateParquetRecord(){
    }
     public CreateParquetRecord(String parquetFile){
         mParquetFileToRead = parquetFile;
         mParquetFileToReadPath =  new Path(mParquetFileToRead);
     }

    public void writeToParquetFile(List<JSONObject> jsonObjFiles) throws IOException {
        /* fs.open(source)*/
        List<InputStream> inputStreamList = new ArrayList<InputStream>();
         for(JSONObject jsonObj:jsonObjFiles){
             //InputStream jsonInputStream= new ByteArrayInputStream(jsonObj.toString().getBytes());
             //InputStream result = new ByteArrayInputStream(jsonFile.ge(Charset.forName("UTF-8"));
             String jsonObjStr =  jsonObj.toString();
             InputStream jsonInputStream = null;
             try {
                 //jsonInputStream = new FileInputStream(jsonObjStr);
                 jsonInputStream= new ByteArrayInputStream(jsonObjStr.getBytes());
             } catch (Exception e) {
                 e.printStackTrace();
             }
             inputStreamList.add(jsonInputStream);
         }

        Schema jsonSchema = JsonUtil.inferSchema(inputStreamList.get(0), "Record", 20);
        List<GenericData.Record> sampleData = new ArrayList<>();
        GenericData.Record record = new GenericData.Record(jsonSchema);
        record.put("_attachments", 1);
        record.put("_rid", "someString");
        record.put("filepath", "filePath");
        record.put("Id", "Id");
        record.put("id", "id");
        record.put("_self", "self");
        record.put("version", "ver1");
        record.put("tasks", "2");
        record.put("_etag", "1");
        record.put("_ts", "1");
        sampleData.add(record);

       GenericData.Record record2 = new GenericData.Record(jsonSchema);
        record2.put("_attachments", 2);
        record2.put("_rid", "someString2");
        record2.put("filepath", "filePath2");
        record2.put("Id", "Id2");
        record2.put("id", "id");
        record2.put("_self", "self2");
        record2.put("version", "ver2");
        record2.put("tasks", "2");
        record2.put("_etag", "1");
        record2.put("_ts", "2");
        sampleData.add(record2);

        try {
            writeToParquet(jsonSchema,sampleData, OUT_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void writeToParquet( Schema schema,List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public String readFromParquet() throws IOException {
        StringBuffer buffer = new StringBuffer();
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(mParquetFileToReadPath)
                .withConf(new Configuration())
                .build()) {
            GenericData.Record record = null;
            while ((record = reader.read()) != null) {
                System.out.println(record);
                buffer.append(record);
            }
        }
        return buffer.toString();
    }

}
