
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.QueryIterable;


public class SqlQuery {
    public static void calculateDailyTransactionTotal(DocumentClient client, String connectionLink){
        //Some config to feedOptions
        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);
       // String sql = "SELECT {\"value\": t.auth.amount} FROM c JOIN t IN c.transactions";
        //String sql = "SELECT t.auth.amount FROM t IN c.transactions WHERE partitionKey = 202001";
        String sql = "SELECT SUM(c.auth.amount) AS totalTransAmount FROM c WHERE c.mid = '123456'";
        System.out.println(String.format("Sql query: %s", sql));
        FeedResponse<Document> queryResults = client.queryDocuments(connectionLink, sql,
                    feedOptions);

        QueryIterable<Document> docs = queryResults.getQueryIterable();
        for (Document trans : docs) {
            System.out.println(String.format("Result %s", trans));
            double d = trans.getDouble("totalTransAmount");
            System.out.println("Total trans amount for mid '123456': " + d);
        }
    }

    public static void countEcomOrderNum(DocumentClient client, String connectionLink){
        //Some config to feedOptions
        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);
        // String sql = "SELECT {\"value\": t.auth.amount} FROM c JOIN t IN c.transactions";
        //String sql = "SELECT t.auth.amount FROM t IN c.transactions WHERE partitionKey = 202001";
        String sql = "SELECT COUNT(c.ecom) AS numOfEcomOrder FROM c WHERE c.mid = '123456'";
        System.out.println(String.format("Sql query: %s", sql));
        FeedResponse<Document> queryResults = client.queryDocuments(connectionLink, sql,
                feedOptions);
        QueryIterable<Document> docs = queryResults.getQueryIterable();
        for (Document trans : docs) {
            System.out.println(String.format("Result %s", trans));
        }
    }
}
