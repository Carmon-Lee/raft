import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * @author liguang
 */

public class RaftMain {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        configuration.getLocalServer().setPort(Integer.valueOf(args[0]));
        configuration.getLocalServer().start();

    }
}
