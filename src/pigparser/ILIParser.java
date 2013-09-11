package pigparser;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.ContentHandler;
import org.joda.time.*;

/**
 *
 * @author Huang
 */
public class ILIParser extends EvalFunc<Tuple> {

// The main method in question. Gets run for every 'thing' that gets sent to
// this UDF
    public Tuple exec(Tuple input) throws IOException {
        //define a set of null values for future filter use
        String timestamp = "";
        String userid = "";
        String latitude = "-9999";
        String longitude = "-9999";
        String textraw = "";
        String locationraw = "";
        String tweetid = "";
        boolean vaccine = false;

        JSONObject obj;
        if (null == input) {
            return null;
        }
        String inputstring = (String) input.get(0);

        String[] tweet = inputstring.split("\\|", 2);


        JSONParser Jsonparser = new JSONParser();

        //get the nested objects
        try {
            obj = (JSONObject) Jsonparser.parse(tweet[1]);
        } catch (Exception e) {
            return null;
        }
        

        //get the user id
        //if (coordinates != null) {
            textraw = (String) obj.get("text");
            

            if (textraw != null && !textraw.isEmpty()) {
                textraw = replaceLinebreak(textraw);
            }

            JSONObject user = (JSONObject) obj.get("user");
           
            timestamp = (String) tweet[0];
            userid = (String) (user.get("id_str"));
            locationraw = (String) user.get("location");
            
            //get rid of the newline in the tweets, also get rid of the pipe 
            //  text = textraw.replaceAll("\\r\\n|\\r|\\n", " ");

            tweetid = (String) obj.get("id_str");

        

        try {
// In Soviet Russia, factory builds you!
            TupleFactory tf = TupleFactory.getInstance();
// Populate a tuple
            Tuple t = tf.newTuple();
            t.append(timestamp);
            t.append(userid);
           // t.append(locationraw);
           // t.append(longitude);
           // t.append(latitude);
            t.append(textraw);
            t.append(tweetid);
            return t;
        } catch (Exception e) {
// Any problems? Just return null and this one doesn't get
// 'generated' by pig
            return null;
        }
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema s = new Schema();
            s.add(new Schema.FieldSchema("timestamp", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("userid", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("text", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("tweetid", DataType.CHARARRAY));
            return s;
        } catch (Exception e) {
// Any problems? Just return null...there probably won't be any
// problems though.
            return null;
        }
    }

    private String replaceLinebreak(String input) {
        return input.replaceAll("\\r\\n|\\r|\\n|\\|", " ");
    }
    
    private static boolean stringContainsItemFromList(String inputString, String[] items)
{
    for(int i =0; i < items.length; i++)
    {
        if(inputString.contains(items[i]))
        {
            return true;
        }
    }
    return false;
}
    
}
