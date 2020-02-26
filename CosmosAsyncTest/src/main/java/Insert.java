import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.ResourceResponse;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Insert implements Callable<String> {

    private String collectionLink;
    private DocumentClient client;
    //private ArrayList<Document> docs;
    private Document doc;


    public Insert(){}
    public Insert(DocumentClient client, String collectionLink, Document doc){
        this.client = client;
        this.collectionLink = collectionLink;
        this.doc = doc;
    }

    @Override
    public String call() throws Exception {

        //for (int i = 0;i<docs.size();i++) {
            ResourceResponse response = client.createDocument(collectionLink,doc,null,true);
            System.out.println(Thread.currentThread().getName() + " " + response.getResource().getId());
        //}

        return response.getResource().getId();
    }
}
