import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RequestOptions;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class Program {

    private DocumentClient client;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();
    private int THROUGHPUT = 1000;

    public static void main(String[] args)
    {
        try{
            Program p = new Program();
            p.testCycle();
            System.out.println("-------------------Complete--------------------");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void testCycle() throws Exception {
        //Connection policy
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
        connectionPolicy.setMaxPoolSize(1000);
        //Connect to db
        String serviceEndpoint = System.getenv("ServiceEndpoint");
        String masterKey = System.getenv("MasterKey");
        this.client = new DocumentClient(serviceEndpoint,
                masterKey,
                connectionPolicy,
                ConsistencyLevel.Session);

        String databaseName = "devdb";
        String collectionName = "daily-trans";

        //Set client's retry options high for initialization
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(120);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);

        //create or connect to database and collection
        this.createDatabaseIfNotExists(databaseName);
        this.createDocumentCollectionIfNotExists(databaseName, collectionName);
        DocumentCollection collection = null;
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        System.out.println(collectionLink);
        collection = client.readCollection(collectionLink, null).getResource();

        //insert document
        stopwatch.start();
        String filepath = "C:\\Users\\Sparta Global\\Desktop\\trans\\d1.json";
        insertDocument(databaseName, collectionName, filepath);
        stopwatch.stop();
        System.out.println(String.format("Insert time is %s milliseconds", stopwatch.elapsed().toMillis()));
    }

    private void createDatabaseIfNotExists(String databaseName) throws DocumentClientException, IOException {
        String databaseLink = String.format("/dbs/%s", databaseName);

        // Check to verify a database with the id=FamilyDB does not exist
        try {
            this.client.readDatabase(databaseLink, null);
            System.out.println(String.format("Found %s", databaseName));
        } catch (DocumentClientException de) {
            // If the database does not exist, create a new database
            if (de.getStatusCode() == 404) {
                Database database = new Database();
                database.setId(databaseName);

                this.client.createDatabase(database, null);
                System.out.println(String.format("Created %s", databaseName));
            } else {
                throw de;
            }
        }
    }

    private void createDocumentCollectionIfNotExists(String databaseName, String collectionName) throws IOException,
            DocumentClientException {
        String databaseLink = String.format("/dbs/%s", databaseName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        try {
            System.out.println("-------------------------Clean Collection---------------------------");
            this.client.deleteCollection(collectionLink, null);
            this.client.readCollection(collectionLink, null);
            System.out.println(String.format("Found %s", collectionName));
        } catch (DocumentClientException de) {
            // If the document collection does not exist, create a new
            // collection
            if (de.getStatusCode() == 404) {
                DocumentCollection collectionInfo = new DocumentCollection();
                collectionInfo.setId(collectionName);

                // Set partition key
                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                Collection<String> paths = new ArrayList<>();
                paths.add("/mid");
                partitionKeyDefinition.setPaths(paths);
                collectionInfo.setPartitionKey(partitionKeyDefinition);

                // Optionally, you can configure the indexing policy of a
                // collection. Here we configure collections for maximum query
                // flexibility including string range queries.
                RangeIndex index = new RangeIndex(DataType.String);
                index.setPrecision(-1);

                collectionInfo.setIndexingPolicy(new IndexingPolicy(new Index[] { index }));

                // DocumentDB collections can be reserved with throughput
                // specified in request units/second. 1 RU is a normalized
                // request equivalent to the read of a 1KB document.
                RequestOptions requestOptions = new RequestOptions();
                //requestOptions.setOfferThroughput(THROUGHPUT);

                this.client.createCollection(databaseLink, collectionInfo, requestOptions);

                System.out.println(String.format("Created %s", collectionName));
            } else {
                throw de;
            }
        }

    }


    private void createDocumentIfNotExists(String databaseName, String collectionName, Document doc)
            throws DocumentClientException, IOException {
        try {
            String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, doc.getId());
            this.client.readDocument(documentLink, new RequestOptions());
        } catch (DocumentClientException de) {
            if (de.getStatusCode() == 404) {
                String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
                this.client.createDocument(collectionLink, doc, new RequestOptions(), true);
                System.out.println(String.format("Created Family %s", doc.getId()));
            } else {
                throw de;
            }
        }
    }

    private void insertDocument(String databaseName, String collectionName, String path){
        JSONParser parser = new JSONParser();
        try(FileReader reader = new FileReader(path)) {
            JSONObject obj = (JSONObject)parser.parse(reader);
            String content = obj.toJSONString();
            Document doc = new Document(content);
            createDocumentIfNotExists(databaseName,collectionName,doc);
            System.out.println("Successfully inserted a document");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (DocumentClientException e) {
            e.printStackTrace();
            System.out.println("Fail to insert document");
        }
    }
}
