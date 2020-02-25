import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AsyncCallable;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.DataType;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.IncludedPath;
import com.microsoft.azure.cosmosdb.Index;
import com.microsoft.azure.cosmosdb.IndexingPolicy;
import com.microsoft.azure.cosmosdb.PartitionKeyDefinition;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.StoredProcedureResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Program {

    private AsyncDocumentClient client;
    private final ExecutorService executorService;
    private final Scheduler scheduler;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final int THROUGHPUT = 5000;
    private final int batchSize = 10;
    private final int threadNum = 10;
    private final int batchNum = 10;
    private final int timeout = 200;
    private int fileInserted = 0;

    public Program() {
        executorService = Executors.newFixedThreadPool(100);
        scheduler = Schedulers.from(executorService);
        //Connect to db
        String serviceEndpoint = System.getenv("ServiceEndpoint");
        String masterKey = System.getenv("MasterKey");
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.Direct);
        policy.setMaxPoolSize(1000);
        this.client = new AsyncDocumentClient.Builder().withServiceEndpoint(serviceEndpoint)
                .withMasterKeyOrResourceToken(masterKey)
                .withConnectionPolicy(policy)
                .withConsistencyLevel(ConsistencyLevel.Eventual)
                .build();
    }

    public static void main(String[] args)
    {
        org.apache.log4j.Logger.getLogger("io.netty").setLevel(org.apache.log4j.Level.OFF);

        Program p = new Program();
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
        DocumentCollection collection = this.createDocumentCollection(databaseName, collectionName);
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

        // Check to verify a database with the id does not exist

        Observable<ResourceResponse<Database>> databaseReadObs = this.client.readDatabase(databaseLink, null);
        Observable<ResourceResponse<Database>> databaseExistenceObs = databaseReadObs.doOnNext(x -> {
            System.out.println("Found " + databaseName);
        }).onErrorResumeNext(e -> {
            if (e instanceof DocumentClientException) {
                DocumentClientException de = (DocumentClientException) e;
                // If the database does not exist, create a new database
                if (de.getStatusCode() == 404) {
                    Database database = new Database();
                    database.setId(databaseName);
                    System.out.println(String.format("Creating %s", databaseName));
                    return this.client.createDatabase(database, null);
                }
            }
            System.err.println(String.format("Reading database %s failed", databaseName));
            return Observable.error(e);
        });
        databaseExistenceObs.toCompletable().await();
        System.out.println(String.format("Checking database %s completed", databaseName));
    }


    private DocumentCollection createDocumentCollection(String databaseName, String collectionName) throws IOException,
            DocumentClientException {
        String databaseLink = String.format("/dbs/%s", databaseName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        //System.out.println("-------------------------Clean Collection---------------------------");

        try {
            //this.client.deleteCollection(collectionLink, null).toBlocking().single().getResource();
            client.readCollection(collectionLink,null).toBlocking().single().getResource();
            System.out.println("Found " + collectionName);
        }
        catch(Exception e)
        {
            System.out.println("no existing collection");

            if (e instanceof DocumentClientException) {
                DocumentClientException de = (DocumentClientException) e;
                // If the database does not exist, create a new database
                if (de.getStatusCode() == 404) {
                    DocumentCollection collectionInfo = new DocumentCollection();
                    collectionInfo.setId(collectionName);

                    // Set partition key
                    PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                    List<String> paths = new ArrayList<>();
                    paths.add("/mid");
                    partitionKeyDefinition.setPaths(paths);
                    collectionInfo.setPartitionKey(partitionKeyDefinition);

                    // Optionally, you can configure the indexing policy of a
                    // collection. Here we configure collections for maximum query
                    // flexibility including string range queries.
                    // Set indexing policy to be range range for string and number
                    IndexingPolicy indexingPolicy = new IndexingPolicy();
                    Collection<IncludedPath> includedPaths = new ArrayList<>();
                    IncludedPath includedPath = new IncludedPath();
                    includedPath.setPath("/*");
                    Collection<Index> indexes = new ArrayList<>();
                    Index stringIndex = Index.Range(DataType.String);
                    stringIndex.set("precision", -1);
                    indexes.add(stringIndex);
                    includedPath.setIndexes(indexes);
                    includedPaths.add(includedPath);
                    indexingPolicy.setIncludedPaths(includedPaths);
                    collectionInfo.setIndexingPolicy(indexingPolicy);

                    // DocumentDB collections can be reserved with throughput
                    // specified in request units/second. 1 RU is a normalized
                    // request equivalent to the read of a 1KB document.
                    RequestOptions requestOptions = new RequestOptions();
                    requestOptions.setOfferThroughput(THROUGHPUT);

                    this.client.createCollection(databaseLink, collectionInfo, requestOptions).toBlocking().single().getResource();

                    System.out.println(String.format("Created %s", collectionName));
                    return collectionInfo;
                }
            }
        }
        return null;
    }


    private void createDocuments(String databaseName, String collectionName, int batchSize, ArrayList<Document> docs) throws Exception {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        List<Observable<ResourceResponse<Document>>> observableList = new ArrayList<>();
        //final CountDownLatch completionLatch = new CountDownLatch(batchSize);

        for (int i = 0;i<batchSize;i++) {
            Observable<ResourceResponse<Document>> observable = this.client
                    .createDocument(collectionLink, docs.get(i), null, true);
            observable.subscribeOn(scheduler)
                    .subscribe(o -> {
                        System.out.println("Thread +" + Thread.currentThread() + " inserted doc id: " + o.getResource().getId() + " latency " + o.getRequestLatency().toMillis());

                    },
                    e -> {
                System.out.println(String.format("Encountered failure %s on thread %s",e.getMessage(), Thread.currentThread()));
                    },
                    ()->{
                fileInserted++;
            });

            observableList.add(observable);
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
