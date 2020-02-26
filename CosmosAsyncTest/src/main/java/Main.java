import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;

public class Main {
    private DocumentClient client;
    private final ExecutorService executorService;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final int THROUGHPUT = 10000;
    private final int threadNum = 50;
    private final int batchPerThread = 10;
    private final int batchSize = 50;
    private final int timeout = 200;
    private int fileInserted = 0;

    public Main() {
        executorService = Executors.newFixedThreadPool(threadNum);
        //Connect to db
        String serviceEndpoint = System.getenv("ServiceEndpoint");
        String masterKey = System.getenv("MasterKey");
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.Gateway);
        policy.setMaxPoolSize(1000);
        this.client = new DocumentClient(serviceEndpoint,
                masterKey,
                policy,
                ConsistencyLevel.Session);
    }

    public static void main(String[] args)
    {

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

        String databaseName = "testdb";
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
        System.out.println(String.format("Insert time is %s milliseconds", stopwatch.elapsed().toMillis()));
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
            System.out.println("-------------------------Fetch Collection---------------------------");
            this.client.deleteCollection(collectionLink, null);
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

    private void createDocuments(String databaseName, String collectionName,  ArrayList<Document> docs) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        //List<Future<Void>> futures = new ArrayList<>();

        // do some work on current thread
        for (int i = 0; i < docs.size(); i++) {
            create(collectionLink,docs.get(i));
            int index = i;
            /*CompletableFuture.runAsync(()-> {
                try {
                    create(collectionLink,docs.get(index));
                } catch (DocumentClientException e) {
                    e.printStackTrace();
                }
            });
             */
        }
        //System.out.println(String.format("Thread: %s stopwatch: %s", Thread.currentThread(), stopwatch.elapsed().toMillis()));
        System.out.println("-----------------------Sleep-------------------------");
        Thread.sleep(timeout);
        //System.out.println(String.format("Thread: %s rest: %s", Thread.currentThread(), stopwatch.elapsed().toMillis()));

    }

    private void create(String collectionLink,  Document doc) throws DocumentClientException {
        ResourceResponse response = client.createDocument(collectionLink, doc,null,true);
        fileInserted++;
        //System.out.println(String.format("Doc ID: %s Thread: %s stopwatch: %s", response.getResource().getId(),Thread.currentThread(), stopwatch.elapsed().toMillis()));
    }

    private void insertDocument(String databaseName, String collectionName, String collectionLink) throws Exception {

        Document doc = Auth.getDocFromJson();


        stopwatch.start();
        System.out.println("---------------write------------------");
        for (int b = 0; b < batchPerThread;b++)
        {
            for(int i = 0; i< threadNum; i++) {
                int batchNum = i + b*threadNum;
                ArrayList<Document> docs = new Auth(batchSize, batchNum , doc).docDefinitions;
                Runnable r = () -> {
                    try {
                        createDocuments(databaseName, collectionName, docs);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };

                /*executorService.execute(r);
                List<Callable<String>> tasks = new ArrayList<>();
                for(int j = 0; j< docs.size(); i++){
                    Insert insertTask = new Insert(client,collectionLink,docs.get(i));
                    tasks.add(insertTask);
                }
                List<Future<String>> s = executorService.invokeAll(tasks);
                 */
            }
            //Thread.sleep(2000);
        }

        System.in.read();
        stopwatch.stop();
    }

    private void close()
    {
        executorService.shutdown();
        client.close();
    }
}
