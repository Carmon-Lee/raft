package org.liguang.raft.server;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.liguang.raft.RaftMessage;
import org.liguang.raft.util.SocketUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author liguang
 */

@Data
@Slf4j
public class ServerNode {

    private static int SLOW_FACTOR = 20;

    private String host;
    private int port;
    private int id;

    private volatile long term;
    private volatile boolean voted;

    private volatile ServerStatus status = ServerStatus.FOLLOWER;
    private List<ServerNode> peers;
    private List<Socket> accepts = new ArrayList<>();
    private ConcurrentHashMap<Socket, ServerNode> clientSocketRegistry = new ConcurrentHashMap<>();
    private Socket socket;


    private Random random = new Random();
    private Thread countThread;
    private final Object lock = new Object();

    private final ExecutorService leaderThread = Executors.newFixedThreadPool(1);
    private final ExecutorService acceptThread = Executors.newFixedThreadPool(1);
    private final ExecutorService connThread = Executors.newFixedThreadPool(1);
    private final ExecutorService followerThread = Executors.newFixedThreadPool(1);

    public void incTerm() {
        synchronized (lock) {
            term++;
        }
    }

    public synchronized void incTermAndVoted() {
        synchronized (lock) {
            term++;
            voted = true;
        }
    }

    public void start() {
        countThread = new Thread(() -> {

            // while loops constantly
            while (true) {
                switch (status) {

                    // 如何自己实现一个超时的接口
                    case LEADER:
                        // the leader keeps sending signals to all nodes in the cluster,
                        // if it received a heartbeat with a term larger than this,
                        // then this server switches to follower
                        System.out.println(":::::::::: switching to leader :::::::::::::");
                        while (true) {
                            try {
                                Future<String> submit = leaderThread.submit(() -> {
                                    peers.forEach(peer -> {
                                        // connect each peer
                                    });
                                    return "success";
                                });
                                String s = submit.get(SLOW_FACTOR * 100, TimeUnit.MILLISECONDS);
                                System.out.println("send heart beat success, still being a leader");
                                // if no message is received, then switch state

                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            } catch (TimeoutException | InterruptedException e) {
                                e.printStackTrace();
                                status = ServerStatus.CANDIDATE;
                                break;
                            } finally {
                                synchronized (lock) {
                                    term++;
                                }
                            }
                        }

                    case FOLLOWER:
                        // the follower receives command from the leader within the specified time limit,
                        // on command reception, the follower will reset the timer,
                        // if no command is received during this time period,
                        // then the server will switch to candidate status
                        log.info("---------switching to follower----------");
                        while (true) {
                            try {
                                voted = true;
                                Thread.sleep(SLOW_FACTOR * 150 + random.nextInt(SLOW_FACTOR * 150));
                                // if run out of time
                                status = ServerStatus.CANDIDATE;
                                break;
                            } catch (InterruptedException e) {
                                log.warn("======= Count for the next term:" + term + " ========");
                            } finally {
//                                incTermAndVoted();
                            }
                        }

                    case CANDIDATE:
                        log.info("=======Switching to candidate=======");

                        // keeps sending votes until a signal from the leader is received
                        // or this server becomes leader itself
                        for (; ; ) {
                            try {
                                Thread.sleep(SLOW_FACTOR * 50);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            peers.forEach(peer -> {
                                // 不需要给本机发送
                                if (!StringUtils.equals(this.getHost(), peer.getHost())
                                        || this.getPort() != peer.getPort()) {
                                    try {
                                        Socket socket = new Socket();
                                        socket.getLocalAddress();
                                        socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()));
                                        OutputStream outputStream = socket.getOutputStream();

                                        Map<String, Object> msg = RaftMessage.raftMessage(term, this.getHost(), this.getPort(), "test");
                                        outputStream.write(JSONObject.toJSONString(msg).getBytes());

                                        String readStr = SocketUtils.readStr(socket.getInputStream());
                                        JSONParser parser = new JSONParser();
                                        Map<String, Object> resp = (Map<String, Object>) parser.parse(readStr);
                                        synchronized (lock) {
                                            Long peerTerm = (Long) resp.get("term");
                                            if (peerTerm > this.term) {
                                                this.term = peerTerm;
                                            }
                                        }
                                        log.info("server received:{}", readStr);
                                        socket.close();
                                    } catch (Exception e) {
//                                    e.printStackTrace();
                                    }
                                }
                            });

                            incTerm();
                        }
                }

            }
        });
        countThread.start();

        acceptThread.submit(() -> {
            ServerSocket serverSocket = new ServerSocket(port);
            for (; ; ) {
                try {
                    Socket accept = serverSocket.accept();
                    log.debug("Socket accepted:" + accept);

                    String s = SocketUtils.readStr(accept.getInputStream());
                    JSONParser parser = new JSONParser();
                    Map<String, Object> parse = (Map<String, Object>) parser.parse(s);

                    Map<String, Object> raftResp;
                    String jsonString;

                    synchronized (lock) {
                        raftResp = RaftMessage.raftResp(term, host, port, voted);
                    }

                    jsonString = JSONObject.toJSONString(raftResp);
                    accept.getOutputStream().write(jsonString.getBytes());

                    log.info("client received:{}", parse);
                    accept.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }


    @Override
    public String toString() {
        return "org.liguang.raft.server.ServerNode{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", id=" + id +
                '}';
    }
}
