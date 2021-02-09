package trparquet;


import com.google.gson.Gson;
import com.microsoft.azure.documentdb.*;

import java.util.ArrayList;
import java.util.List;

public class AzureCosmosDB {

    private static DocumentClient singletonDocumentClient = null;
    private static Gson gson = new Gson();

    private static final String HOST = "https://truccosmosa.documents.azure.com:443/";
    private static final String MASTER_KEY = "aw0uISWd7BPdygfz16tRhGP37LesrI5qaTvCqKGWrVEabKco3OtaXPHdp4ENihXcRbV6JQPmX44qwj5PUyft4w==";

    public static DocumentClient getDocumentClient() throws DocumentClientException {
        if(singletonDocumentClient!=null)
        {
            return singletonDocumentClient;
        }
//        ConnectionPolicy policy = new ConnectionPolicy();
//        RetryOptions retryOptions = new RetryOptions();
//        retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
//        policy.setRetryOptions(retryOptions);
//        policy.setConnectionMode(ConnectionMode.Gateway);
//        policy.setMaxPoolSize(1000);
//        //singletonDocumentClient=new DocumentClient("https://truccosmosa.documents.azure.com:443/", "aw0uISWd7BPdygfz16tRhGP37LesrI5qaTvCqKGWrVEabKco3OtaXPHdp4ENihXcRbV6JQPmX44qwj5PUyft4w==", policy, ConsistencyLevel.Session);
        singletonDocumentClient = new DocumentClient(HOST, MASTER_KEY, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
        System.out.println("Conn : " + singletonDocumentClient);
        return singletonDocumentClient;
    }





    //Query the Cosmos DB
    static List<Document> queryDocs(
            String query,
            DocumentClient client,
            String collectionLink)
    {
        List<Document> documents=new ArrayList<Document>();

        FeedOptions options=new FeedOptions();
        options.setEnableCrossPartitionQuery(true);

        long startTime = System.currentTimeMillis();

        for(Document doc: client.queryDocuments(collectionLink, query, options).getQueryIterable().toList()){

            documents.add(doc);
        }

        long totalTime = System.currentTimeMillis() - startTime;

        return documents;
    }

    //Query the docs by Type
    static <T> List<T> queryDocsByType(
            String query,
           DocumentClient client,
           String collectionLink,
            Class<T> classType)
    {
        List<T> documents=new ArrayList<T>();
        for(Document doc: client.queryDocuments(collectionLink, query, null).getQueryIterable().toList()){
            documents.add(gson.fromJson(doc.toString(),classType));
        }
        return documents;
    }

    static ResourceResponse<Document> isDocumentExists(
            DocumentClient client,
            String documentUrl,
            String partitionKey,
            RequestOptions options) throws DocumentClientException {
        ResourceResponse<Document> response=null;
        if(options==null) options = new RequestOptions();
        if(!partitionKey.equals("")) {
            options.setPartitionKey(new PartitionKey(partitionKey));
        }
        try {
            long startTime = System.currentTimeMillis();

            response = client.readDocument(documentUrl, options);

            long totalTime = System.currentTimeMillis() - startTime;

        }catch (DocumentClientException ex)
        {
            if(ex.getStatusCode()==404) {
                System.out.println(ex.getMessage());
            }
            else {
                throw ex;
            }
        }
        return response;
    }







}
