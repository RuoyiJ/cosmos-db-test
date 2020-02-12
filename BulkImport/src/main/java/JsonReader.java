

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class JsonReader {

    public static ArrayList<String> generateJsonCollection(int numOfDocs, String path)
    {
        ArrayList<String> docs = new ArrayList<>();

        JSONParser parser = new JSONParser();
        try (FileReader reader = new FileReader(path)) {
            JSONObject obj = (JSONObject)parser.parse(reader);
            for(int i = 0 ; i< numOfDocs; i++)
            {
                StringBuilder id = new StringBuilder(obj.get("id").toString());
                JSONObject auth = (JSONObject)obj.get("auth");
                StringBuilder oId = new StringBuilder(auth.get("orderId").toString());
                int mid = 123456 + i/30 ;
                long orderId = 100000000 + i;
                id.replace(0,6,Integer.toString(mid));
                int last = oId.length();
                oId.replace(last - 9, last, Long.toString(orderId));
                id.replace(6, id.length(), oId.toString());
                obj.replace("id",id.toString());
                obj.replace("mid", Integer.toString(mid));
                auth.replace("orderId", oId.toString());
                //System.out.println(id);
                docs.add(obj.toJSONString());
            }

            return docs;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
