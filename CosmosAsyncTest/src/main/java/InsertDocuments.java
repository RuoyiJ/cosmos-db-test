import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.ResourceResponse;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class InsertDocuments implements Callable<Long> {

    private String collectionLink;
    private DocumentClient client;
    private ArrayList<Document> docs;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();

    public InsertDocuments(){}
    public InsertDocuments(DocumentClient client, String collectionLink, ArrayList<Document> docs){
        this.client = client;
        this.collectionLink = collectionLink;
        this.docs = docs;
    }

    @Override
    public Long call() throws Exception {

        stopwatch.start();
        for (int i = 0;i<docs.size();i++) {
            ResourceResponse response = client.createDocument(collectionLink,docs.get(i),null,true);
            //System.out.println(Thread.currentThread().getName() + " " + response.getResource().getId());
        }
        stopwatch.stop();
        return stopwatch.elapsed().toMillis();
    }
}
