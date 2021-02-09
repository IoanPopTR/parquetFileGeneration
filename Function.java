package trparquet;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    /**
     * This function listens at endpoint "/api/HttpTrigger-Java". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpTrigger-Java&code={your function key}
     * 2. curl "{your host}/api/HttpTrigger-Java?name=HTTP%20Query&code={your function key}"
     * Function Key is not needed when running locally, it is used to invoke function deployed to Azure.
     * More details: https://aka.ms/functions_authorization_keys
     */
    @FunctionName("HttpTrigger-Java")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        //context.getLogger().info("Java HTTP trigger processed a request.");
        //readDataToParquetFile("C:\\parquetDestination\\fileRecord.parquet");
        writeDataToParquetFile();



        String query = request.getQueryParameters().get("name");
        String name = request.getBody().orElse(query);
        if (name == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a name on the query string or in the request body").build();
        } else {
            return request.createResponseBuilder(HttpStatus.OK).body("Submitted a parquet file").build();
        }
    }


    public  String  writeDataToParquetFile(){
        DocumentClient docclient = null;
        try {
            docclient = AzureCosmosDB.getDocumentClient();
        } catch (Exception ex){
            ex.printStackTrace();
        }
        List<Document> documents =   AzureCosmosDB.queryDocs( String.format("select * from c"),docclient,String.format("/dbs/%s/colls/%s", "sampleDB", "sampleContainer"));
        System.out.println("Documents fetched : " + documents.size());
        List<JSONObject>  jsonObjectList =  new ArrayList<JSONObject>();
        for(Document doc:documents){
            // System.out.println("Document id: " + id);
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject =  null;
            try {
                jsonObject = (JSONObject)jsonParser.parse(doc.toJson());
            } catch(Exception ex){
                ex.printStackTrace();
            }
            String idStr =  (String) jsonObject.get("id");
            System.out.println("Document id: " + idStr);
            String docBlobPath =  (String) jsonObject.get("DocBlobpath");
            System.out.println("DocBlobPath: " + docBlobPath);
            jsonObjectList.add(jsonObject);
            break;
        }

        CreateParquetRecord parquetRecord = new CreateParquetRecord();
        try {
            parquetRecord.writeToParquetFile(jsonObjectList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public  String  readDataToParquetFile(String parquetFileToRead){

        CreateParquetRecord parquetRecord = new CreateParquetRecord(parquetFileToRead);
        try {
            parquetRecord.readFromParquet();
        } catch (Exception e) {
            e.printStackTrace();
        }



        return null;
    }




    public static void main(String[] args) {
        try {
            Function func = new Function();
            HttpRequestMessage message = null;
            ExecutionContext context = null;
            func.run(message, context);
        } catch (Exception e) {
           /// e.printStackTrace();
        }
    }
}
