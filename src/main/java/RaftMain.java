/**
 * @author liguang
 */

public class RaftMain {

    public static void main(String[] args) {

        Configuration configuration=new Configuration();
        configuration.getLocalServer().start();
    }
}
