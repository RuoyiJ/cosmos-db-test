import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;

import java.util.ArrayList;
import java.util.concurrent.Callable;

public class InsertDocuments implements Callable<Integer> {

    private String collectionLink;
    private DocumentClient client;
    private ArrayList<Document> docs;

    public InsertDocuments(){}
    public InsertDocuments(DocumentClient client, String collectionLink, ArrayList<Document> docs){
        this.client = client;
        this.collectionLink = collectionLink;
        this.docs = docs;
    }

    @Override
    public Integer call() throws Exception {

        int fileInserted = 0;
        for (int i = 0;i<docs.size();i++) {
            RequestOptions requestOptions = new RequestOptions();
            PartitionKey partitionKey= new PartitionKey(docs.get(i).get("mid"));
            requestOptions.setPartitionKey(partitionKey);
            ResourceResponse response = client.createDocument(collectionLink,docs.get(i),requestOptions,true);
            //System.out.println(Thread.currentThread().getName() + " " + response.getResource().getId());
            fileInserted++;
        }
        return fileInserted;
    }
}
