import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.microsoft.azure.documentdb.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class Main {
    private DocumentClient client;
    private final ExecutorService executorService;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final int THROUGHPUT = 400;
    private final int threadNum = 2;
    private final int batchPerThread = 1;
    private final int batchSize = 2;
    private final int timeout = 200;
    private int failure = 0;
    private int batchCount = 0;

    public Main() {
        executorService = Executors.newFixedThreadPool(threadNum);
        //Connect to db
        String serviceEndpoint = System.getenv("ServiceEndpoint");
        String masterKey = System.getenv("MasterKey");
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.Gateway);
        policy.setMaxPoolSize(200);
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
        DocumentCollection collection =this.createDocumentCollectionIfNotExists(databaseName, collectionName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        System.out.println(collectionLink);

        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);


        //insert documents
        insertBatches(databaseName, collectionName);
        System.out.println(String.format("%s failures", failure));

        // upsert documents
        upsertBatches(databaseName,collectionName);
        System.out.println(String.format("%s failures", failure));

        //Set throughput low
        changeContainerThroughput(collection, 1000);
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

    private DocumentCollection createDocumentCollectionIfNotExists(String databaseName, String collectionName) throws IOException,
            DocumentClientException {
        String databaseLink = String.format("/dbs/%s", databaseName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        try {
            System.out.println("-------------------------Fetch Collection---------------------------");
            //this.client.deleteCollection(collectionLink, null);
            DocumentCollection collection = this.client.readCollection(collectionLink, null).getResource();
            System.out.println(String.format("Found %s", collectionName));
            return collection;
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
                DocumentCollection collection = this.client.readCollection(collectionLink, null).getResource();
                return collection;
            } else {
                throw de;
            }
        }
    }

    private void createDocuments(String databaseName, String collectionName, ArrayList<Document> docs) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        //List<Future<Void>> futures = new ArrayList<>();

        // do some work on current thread
        for (int i = 0; i < docs.size(); i++) {
            create(collectionLink,docs.get(i));
            int index = i;
        }
        batchCount++;
        System.out.println(String.format("BatchCount: %s Thread: %s stopwatch: %s", batchCount, Thread.currentThread(), stopwatch.elapsed().toMillis()));
    }

    private void upsertDocuments(String databaseName, String collectionName, ArrayList<Document> docs) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        // do some work on current thread
        for (int i = 0; i < docs.size(); i++) {
            upsert(collectionLink,docs.get(i));
            int index = i;
        }
        batchCount++;
        System.out.println(String.format("BatchCount: %s Thread: %s stopwatch: %s", batchCount, Thread.currentThread(), stopwatch.elapsed().toMillis()));
    }

    private void create(String collectionLink,  Document doc) {
        RequestOptions requestOptions = new RequestOptions();
        PartitionKey partitionKey= new PartitionKey(doc.get("mid"));
        requestOptions.setPartitionKey(partitionKey);
        try {
            ResourceResponse response = client.createDocument(collectionLink, doc,requestOptions,true);
        } catch (DocumentClientException e) {
            failure++;
            e.printStackTrace();
        }
        //System.out.println(String.format("Doc ID: %s Thread: %s stopwatch: %s", response.getResource().getId(),Thread.currentThread(), stopwatch.elapsed().toMillis()));
    }

    private void upsert(String collectionLink, Document doc) {
        RequestOptions requestOptions = new RequestOptions();
        PartitionKey partitionKey= new PartitionKey(doc.get("mid"));
        requestOptions.setPartitionKey(partitionKey);
        try {
            ResourceResponse response = client.upsertDocument(collectionLink,doc,requestOptions,true);
        } catch (DocumentClientException e) {
            failure++;
            e.printStackTrace();
        }
    }

    private void insertBatches(String databaseName, String collectionName) throws Exception {

        Document doc = DocumentBuilder.getDocFromJson("d1.json");
        failure = 0;
        batchCount = 0;

        stopwatch.start();
        System.out.println("---------------write------------------");
        for (int b = 0; b < batchPerThread;b++)
        {
            //List<Callable<Integer>> tasks = new ArrayList<>();

            for(int i = 0; i< threadNum; i++) {
                int batchNum = i + b * threadNum;
                ArrayList<Document> docs = new DocumentBuilder(batchSize, batchNum , doc).docDefinitions;
                Runnable r = () -> {
                    try {
                        createDocuments(databaseName, collectionName, docs);
                        //docs.stream().forEach(d->{System.out.println(d.getId());});
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
                executorService.execute(r);

                /*String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
                InsertDocuments insertTask = new InsertDocuments(client,collectionLink,docs);
                tasks.add(insertTask);

                 */
            }
            /*List<Future<Integer>> callback = executorService.invokeAll(tasks);
            int totalFileNum = callback.stream()
                    .mapToInt(value -> {
                try {
                    return value.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } return 0;})
                    .reduce(0,(sum, value)-> sum + value);
            System.out.println("Total num of files inserted: " + totalFileNum);

             */
            System.out.println("-------------------------Sleep----------------------");
            Thread.sleep(timeout);
        }
        System.in.read();
        stopwatch.stop();
        stopwatch.reset();
    }
    private void upsertBatches(String databaseName, String collectionName) throws Exception{
        Document doc = DocumentBuilder.getDocFromJson("d2.json");
        failure = 0;
        batchCount = 0;

        stopwatch.start();
        System.out.println("---------------upsert------------------");
        for (int b = 0; b < batchPerThread;b++) {
            for (int i = 0; i < threadNum; i++) {
                int batchNum = i + b * threadNum;
                ArrayList<Document> docs = new DocumentBuilder(batchSize, batchNum, doc).docDefinitions;

                Runnable r = () -> {
                    try {
                        upsertDocuments(databaseName, collectionName, docs);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                };
                executorService.execute(r);
            }
            System.out.println("-------------------------Sleep----------------------");
            Thread.sleep(timeout);
        }
        System.in.read();
        stopwatch.stop();
    }

    private void changeContainerThroughput(DocumentCollection collection, int throughput) throws DocumentClientException {
        String collectionResourceId = collection.getResourceId();
        Iterator<Offer> it = client.queryOffers(
                String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId), null).getQueryIterator();
        Offer offer = it.next();
        // update the offer
        offer.getContent().put("offerThroughput", throughput);
        client.replaceOffer(offer);
        System.out.println(offer.getContent().getInt("offerThroughput"));

    }
    private void close()
    {
        executorService.shutdown();
        client.close();
    }
}
