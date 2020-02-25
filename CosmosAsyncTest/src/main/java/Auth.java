import com.microsoft.azure.documentdb.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Auth {
    ArrayList<Document> docDefinitions = new ArrayList<>();

    public Auth(int batchSize, int batchNum, Document doc) throws Exception{
        for(int i = 0; i< batchSize; i++)
        {
            Document newDoc = new Document(doc.toJson());
            StringBuilder id = new StringBuilder(doc.getId());
            //StringBuilder oId = new StringBuilder(doc.getString("auth.orderId"));
            int mid = 1234567 + (i + batchSize/15 * batchNum)/2 ;
            long orderId = 100000000 + (i + batchSize * batchNum);
            id.replace(0,7,Integer.toString(mid));
            //int last = oId.length();
            //oId.replace(last - 9, last, Long.toString(orderId));
            int last = id.length();
            id.replace(last - 9, last, Long.toString(orderId));
            newDoc.setId(id.toString());
            newDoc.set("mid", Integer.toString(mid));
            //doc.set("auth.orderId", oId.toString());
            docDefinitions.add(newDoc);
        }

    }

    public static Document getDocFromJson()
    {
        String filepath = "C:\\Users\\Sparta Global\\Desktop\\trans\\d1.json";
        JSONParser parser = new JSONParser();
        try(FileReader reader = new FileReader(filepath)) {
            JSONObject obj = (JSONObject) parser.parse(reader);
            String content = obj.toJSONString();
            Document doc = new Document(content);
            return doc;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
