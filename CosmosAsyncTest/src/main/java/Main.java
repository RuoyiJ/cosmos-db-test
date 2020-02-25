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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private DocumentClient client;
    private final ExecutorService executorService;
    //private Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final int THROUGHPUT = 5000;
    private final int batchSize = 10;
    private final int threadNum = 10;
    private final int batchNum = 10;
    private final int timeout = 200;
    private int fileInserted = 0;

    public Main() {
        executorService = Executors.newFixedThreadPool(100);
        //Connect to db
        String serviceEndpoint = System.getenv("ServiceEndpoint");
        String masterKey = System.getenv("MasterKey");
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);
        policy.setMaxPoolSize(1000);
        this.client = new DocumentClient(serviceEndpoint,
                masterKey,
                policy,
                ConsistencyLevel.Session);
    }

    public static void main(String[] args)
    {
        org.apache.log4j.Logger.getLogger("io.netty").setLevel(org.apache.log4j.Level.OFF);

        Main p = new Main();
        try{
            p.testCycle();
            System.out.println("-------------------Complete--------------------");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            System.out.println("Closing down DocumentClient");
            p.close();
        }
    }

    private void testCycle() throws Exception {

        String databaseName = "devdb";
        String collectionName = "daily-trans";


        //Set client's retry options high for initialization
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(120);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);

        //create or connect to database and collection
        this.createDatabaseIfNotExists(databaseName);
        this.createDocumentCollectionIfNotExists(databaseName, collectionName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        System.out.println(collectionLink);

        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);


        //insert document
        insertDocument(databaseName, collectionName, collectionLink);
        //System.out.println(String.format("Insert time is %s milliseconds", stopwatch.elapsed().toMillis()));
        System.out.println(String.format("%s files inserted", fileInserted));
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
            //this.client.deleteCollection(collectionLink, null);
            this.client.readCollection(collectionLink, null).getResource();
            System.out.println(String.format("Found %s", collectionName));
        } catch (DocumentClientException de) {
            // If the document collection does not exist, create a new
            // collection
            if (de.getStatusCode() == 404) {
                DocumentCollection collectionInfo = new DocumentCollection();
                collectionInfo.setId(collectionName);
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
                // request equivalent to the read of a 1KB document. Here we
                // create a collection with 10000 RU/s.
                RequestOptions requestOptions = new RequestOptions();
                requestOptions.setOfferThroughput(THROUGHPUT);

                this.client.createCollection(databaseLink, collectionInfo, requestOptions);

                System.out.println(String.format("Created %s", collectionName));
            } else {
                throw de;
            }
        }
    }

    private void createDocuments(String databaseName, String collectionName, int batchSize, ArrayList<Document> docs) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        for (int i = 0;i<batchSize;i++) {

        }

    }

    private void insertDocument(String databaseName, String collectionName, String collectionLink) throws Exception {

        //int time = 300000;
        Document doc = Auth.getDocFromJson();

        System.out.println("---------------write------------------");
        for(int i = 0; i< threadNum; i++) {
            ArrayList<Document> docs = new Auth(batchSize, i, doc).docDefinitions;
            Runnable r = () -> {
                try {
                    createDocuments(databaseName, collectionName, batchSize, docs);
                    //executeStoredProcedure(collectionLink,docs);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };
            Thread thread = new Thread(r);
            thread.start();
            //executorService.execute(r);

            //ArrayList<Observable<Document>> createObservables = new ArrayList<>();
            //createDocuments(databaseName, collectionName, batchSize, docs);
            //Observable.merge(createObservables, 100).toList().toBlocking().single();
        }
        System.in.read();

    }

    /*private void executeStoredProcedure(String collectionLink, ArrayList<Document> docs) throws InterruptedException {
        String sprocLink = String.format("%s/sprocs/%s", collectionLink, "insertDocument");
        final CountDownLatch completionLatch = new CountDownLatch(docs.size());

        //StoredProcedureResponse response;
        for (int i = 0; i< docs.size();i++) {
            Object[] storedProcedureArgs = new Object[]{docs.get(i)};
            AsyncCallable
            client.executeStoredProcedure(sprocLink, storedProcedureArgs).subscribeOn(scheduler)
                    .subscribe(r -> {
                        System.out.println(r.getResponseAsString());
                        completionLatch.countDown();
                    }, e -> {
                        e.printStackTrace();
                        completionLatch.countDown();
                    });
        }
        completionLatch.await();
    }*/

    private void heavyWork(String databaseName, String collectionName) {
        // I may do a lot of IO work: e.g., writing to log files
        // a lot of computational work
        // or may do Thread.sleep()

        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (Exception e) {
        }
    }
    private void close()
    {
        executorService.shutdown();
        client.close();
    }
}
