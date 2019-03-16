import lombok.Data;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author liguang
 */

@Data
public class ServerNode {

    private String host;
    private int port;
    private int id;

    private int term;

    private ServerStatus status = ServerStatus.LEADER;
    private List<ServerNode> peers;
    private List<Socket> accepts = new ArrayList<>();
    private ConcurrentHashMap<Socket, ServerNode> clientSocketRegistry = new ConcurrentHashMap<>();
    private Socket socket;


    private Random random = new Random();
    private Thread countThread;
    private final Object lock = new Object();

    private final ExecutorService leaderThread = Executors.newFixedThreadPool(1);
    private final ExecutorService connectThread = Executors.newFixedThreadPool(1);
    private final ExecutorService followerThread = Executors.newFixedThreadPool(1);

    public void start() {
        countThread = new Thread(() -> {

            // while loops constantly
            while (true) {
                synchronized (lock) {
                    switch (status) {

                        // 如何自己实现一个超时的接口
                        case LEADER:
                            // the leader keeps sending signals to all nodes in the cluster,
                            // if it received a heartbeat with a term larger than this,
                            // then this server switches to follower
                            while (true) {
                                try {
                                    Future<String> submit = leaderThread.submit(() -> {
                                        peers.forEach(peer -> {
                                            // connect each peer
                                        });
                                        return "success";
                                    });
                                    String s = submit.get(100, TimeUnit.MILLISECONDS);
                                    System.out.println("send heart beat success, still being a leader");
                                    // if no message is received, then switch state

                                } catch (ExecutionException e) {
                                    e.printStackTrace();
                                } catch (TimeoutException | InterruptedException e) {
                                    e.printStackTrace();
                                    status = ServerStatus.CANDIDATE;
                                    break;
                                } finally {
                                    term++;
                                }
                            }

                        case FOLLOWER:
                            // the follower receives command from the leader within the specified time limit,
                            // on command reception, the follower will reset the timer,
                            // if no command is received during this time period,
                            // then the server will switch to candidate status
                            while (true) {
                                try {
                                    Thread.sleep(150 + random.nextInt(150));
                                    // if run out of time
                                    status = ServerStatus.CANDIDATE;
                                    break;
                                } catch (InterruptedException e) {
                                    System.out.println("======= Count for the next term:" + term + " ========");
                                } finally {
                                    term++;
                                }
                            }
                        case CANDIDATE:
                            // keeps sending votes until a signal from the leader is received
                            // or this server becomes leader itself
                            break;
                    }
                }
            }
        });
        countThread.start();

        connectThread.submit(() -> {
            for (; ; ) {
                try {
                    ServerSocket serverSocket = new ServerSocket(port);
                    Socket accept = serverSocket.accept();

                    accepts.add(accept);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    @Override
    public String toString() {
        return "ServerNode{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", id=" + id +
                '}';
    }
}
