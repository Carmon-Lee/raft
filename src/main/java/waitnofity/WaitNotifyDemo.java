package waitnofity;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * @author liguang
 */

public class WaitNotifyDemo {

    static class Waiter implements Runnable {

        public void run() {

        }
    }

    public static void main(String[] args) throws Exception{

        WaitNotifyDemo demo = new WaitNotifyDemo();

        FutureTask<String> futureTask = new FutureTask<String>(new Callable<String>() {
            public String call() throws Exception {
                Thread.sleep(1000);
                return "success";
            }
        });
        new Thread(futureTask).start();

        String s = futureTask.get(3, TimeUnit.SECONDS);
        System.out.println("result:"+s);
        demo.timeCostly();
    }

    public void timeCostly() {

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
