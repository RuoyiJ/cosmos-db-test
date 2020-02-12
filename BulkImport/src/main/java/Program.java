
import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.bulkexecutor.BulkImportResponse;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor;
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor.Builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class Program {

    private DocumentClient client;
    private Stopwatch totalWatch = Stopwatch.createUnstarted();
    private int OFFER_THROUGHPUT = 10000;
    /**
     * Run a Hello DocumentDB console application.
     *
     * @param args command line arguments
     * @throws DocumentClientException exception
     * @throws IOException
     */
    public static void main(String[] args) {

        try {
            Program p = new Program();
            p.cosmosDbTestCycle();
            System.out.println("Complete");
        } catch (Exception e) {
            System.out.println(String.format("DocumentDB failed with %s", e));
        }
    }
    private void cosmosDbTestCycle() throws  Exception{
        //Parse json doc
        System.out.println("----------------------Parsing json-----------------------");
        int numOfDocs = 10000;
        ArrayList<String> documents = JsonReader.generateJsonCollection(numOfDocs,"C:\\Users\\Sparta Global\\Desktop\\trans\\d1.json");
        ArrayList<String> documents2 = JsonReader.generateJsonCollection(numOfDocs,"C:\\Users\\Sparta Global\\Desktop\\trans\\d2.json");

        System.out.println(documents2.get(0));

        //set connection to cosmos db
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
        connectionPolicy.setMaxPoolSize(1000);
        String serviceEndpoint = System.getenv("ServiceEndpoint");
        String masterKey = System.getenv("MasterKey");
        System.out.println(serviceEndpoint);

        this.client = new DocumentClient(serviceEndpoint,
                masterKey,
                connectionPolicy,
                ConsistencyLevel.Session);


        String databaseName = "testdb";
        String collectionName = "daily-trans";

        // Set client's retry options high for initialization
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(120);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);

        //create or connect to database and collection
        this.createDatabaseIfNotExists(databaseName);
        this.createDocumentCollectionIfNotExists(databaseName, collectionName);
        DocumentCollection collection = null;
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        System.out.println(collectionLink);
        collection = client.readCollection(collectionLink, null).getResource();

        System.out.println("-------------------------Bulk Import---------------------------");
        executeImport(databaseName,collectionName,collection,documents);
        System.out.println("-------------------------SQL Query---------------------------");
        queryDocuments(collectionLink);
        System.in.read();
        System.out.println("-------------------------Bulk Upsert---------------------------");
        executeUpsert(databaseName,collectionName,collection,documents2);
        System.out.println("-------------------------SQL Query---------------------------");
        queryDocuments(collectionLink);

        System.in.read();
        //Set throughput back down
        String collectionResourceId = collection.getResourceId();
        Iterator<Offer> it = client.queryOffers(
                String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId), null).getQueryIterator();
        Offer offer = it.next();
        System.out.println(offer.getContent().getInt("offerThroughput"));
        // update the offer
        int newThroughput = 1000;
        offer.getContent().put("offerThroughput", newThroughput);
        client.replaceOffer(offer);
    }

    private void executeImport(String databaseName, String collectionName, DocumentCollection collection, ArrayList<String> documents) throws Exception {

        // Builder pattern
        Builder bulkExecutorBuilder = DocumentBulkExecutor.builder().from(
                client,
                databaseName,
                collectionName,
                collection.getPartitionKey(),
                OFFER_THROUGHPUT); // throughput you want to allocate for bulk import out of the container's total throughput

        // Instantiate DocumentBulkExecutor
        DocumentBulkExecutor bulkExecutor = bulkExecutorBuilder.build();

        // Set retries to 0 to pass complete control to bulk executor
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);

        BulkImportResponse bulkImportResponse = null;
        totalWatch.start();
        bulkImportResponse = bulkExecutor.importAll(documents, false, true, null);
        totalWatch.stop();
        System.out.println( String.format("Files imported: %s", bulkImportResponse.getNumberOfDocumentsImported()));
        System.out.println("Bad Input File: " + bulkImportResponse.getBadInputDocuments().size());
        System.out.println( String.format("Import time: %s milliseconds", bulkImportResponse.getTotalTimeTaken().toMillis()));
        System.out.println(String.format("Total Import time measured by stopwatch: %s milliseconds", totalWatch.elapsed().toMillis()));
        System.out.println("Average #Insert/sec: " + + bulkImportResponse.getNumberOfDocumentsImported()
                / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
        System.out.println("Average RU/sec: " + bulkImportResponse.getTotalRequestUnitsConsumed()
                / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
        totalWatch.reset();
    }

    private void executeUpsert(String databaseName, String collectionName, DocumentCollection collection, ArrayList<String> documents) throws Exception{

        // Set client's retry options high for initialization
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(120);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);
        //Builder
        Builder bulkExecutorBuilder = DocumentBulkExecutor.builder().from(
                client,
                databaseName,
                collectionName,
                collection.getPartitionKey(),
                OFFER_THROUGHPUT);

        //Instantiate Bulk executor
        try(DocumentBulkExecutor bulkExecutor = bulkExecutorBuilder.build()){
            //set retries to 0 to pass control to bulk executor
            client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
            client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);
            totalWatch.start();
            BulkImportResponse bulkImportResponse = null;
            bulkImportResponse = bulkExecutor.importAll(documents, true, true, null);

            totalWatch.stop();
            System.out.println( String.format("Files upsert: %s", bulkImportResponse.getNumberOfDocumentsImported()));
            System.out.println("Bad Input File: " + bulkImportResponse.getBadInputDocuments().size());
            System.out.println( String.format("Upsert time: %s milliseconds", bulkImportResponse.getTotalTimeTaken().toMillis()));
            System.out.println(String.format("Total Upsert time measured by stopwatch: %s milliseconds", totalWatch.elapsed().toMillis()));
            System.out.println("Average #Upsert/sec: " + + bulkImportResponse.getNumberOfDocumentsImported()
                    / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
            System.out.println("Average RU/sec: " + bulkImportResponse.getTotalRequestUnitsConsumed()
                    / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
            totalWatch.reset();
        }
    }

    private void queryDocuments(String collectionLink) throws DocumentClientException {
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(120);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);
        try {
            totalWatch.start();
            SqlQuery.calculateDailyTransactionTotal(this.client, collectionLink);
            totalWatch.stop();
            System.out.println(String.format("Query time measured by stopwatch: %s milliseconds", totalWatch.elapsed().toMillis()));
            totalWatch.reset();
            SqlQuery.countEcomOrderNum(this.client, collectionLink);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
        client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);
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
                requestOptions.setOfferThroughput(OFFER_THROUGHPUT);

                this.client.createCollection(databaseLink, collectionInfo, requestOptions);

                System.out.println(String.format("Created %s", collectionName));
            } else {
                throw de;
            }
        }

    }

}
