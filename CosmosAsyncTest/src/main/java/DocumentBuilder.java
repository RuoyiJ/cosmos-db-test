import com.microsoft.azure.documentdb.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class DocumentBuilder {
    ArrayList<Document> docDefinitions = new ArrayList<>();
    private final int transPerMid = 10;

    public DocumentBuilder(int batchSize, int batchNum, Document doc) throws Exception{
        for(int i = 0; i< batchSize; i++)
        {
            Document newDoc = new Document(doc.toJson());
            StringBuilder id = new StringBuilder(doc.getId());
            //StringBuilder oId = new StringBuilder(doc.getString("auth.orderId"));
            int mid =  1234567 + i + (int)Math.floor(batchNum/transPerMid) * batchSize;
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

    public static Document getDocFromJson(String filename)
    {
        String root = System.getProperty("user.dir") + "\\";
        String filepath = root + filename;
        /*JSONParser parser = new JSONParser();
        try(FileReader reader = new FileReader(filepath)) {
            JSONObject obj = (JSONObject) parser.parse(reader);
            String content = obj.toJSONString();
            Document doc = new Document(content);
            return doc;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }*/
        String content = null;
        try {
            content = new String(Files.readAllBytes(Paths.get(filepath)));
            Document doc = new Document(content);
            return doc;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
