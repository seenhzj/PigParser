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
public class parser extends EvalFunc<Tuple> {

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
        JSONObject coordinates = (JSONObject) obj.get("coordinates");


        //get the user id
        if (coordinates != null) {

            JSONArray point = (JSONArray) coordinates.get("coordinates");
            JSONObject user = (JSONObject) obj.get("user");
            try {
                longitude = ((Double) point.get(0)).toString();
                latitude = ((Double) point.get(1)).toString();
            } catch (Exception e) {
                // do nothing
            }
            timestamp = (String) tweet[0];
            userid = (String) (user.get("id_str"));
            locationraw = (String) user.get("location");
            textraw = (String) obj.get("text");

            if (locationraw != null && !locationraw.isEmpty()) {
                locationraw = replaceLinebreak(locationraw);
            }
            if (textraw != null && !textraw.isEmpty()) {
                textraw = replaceLinebreak(textraw);
            }
            //get rid of the newline in the tweets, also get rid of the pipe 
            //  text = textraw.replaceAll("\\r\\n|\\r|\\n", " ");

            tweetid = (String) obj.get("id_str");

        }

        try {
// In Soviet Russia, factory builds you!
            TupleFactory tf = TupleFactory.getInstance();
// Populate a tuple
            Tuple t = tf.newTuple();
            t.append(timestamp);
            t.append(userid);
            t.append(locationraw);
            t.append(longitude);
            t.append(latitude);
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
            s.add(new Schema.FieldSchema("profilelocation", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("longitude", DataType.CHARARRAY));
            s.add(new Schema.FieldSchema("latitude", DataType.CHARARRAY));
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
}
