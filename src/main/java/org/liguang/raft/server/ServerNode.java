package org.liguang.raft.server;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.liguang.raft.RaftMessage;
import org.liguang.raft.util.SocketUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liguang
 */

@Data
@Slf4j
public class ServerNode {

    private static int SLOW_FACTOR = 10;

    private String host;
    private int port;
    private int id;

    private volatile long term;
    // voted: true代表已投票，false代表未投票
    private volatile boolean voted;

    private volatile ServerStatus status = ServerStatus.FOLLOWER;
    private List<ServerNode> peers;
    private int quorumNum;
    private volatile boolean cancelCand = false;
    private List<Socket> accepts = new ArrayList<>();
    private ConcurrentHashMap<Socket, ServerNode> clientSocketRegistry = new ConcurrentHashMap<>();
    private Socket socket;


    private Random random = new Random();
    private Thread countThread;
    private Thread electionTimer;
    private final Object lock = new Object();
    private final Object electionLock = new Object();
    private final Object sleepLock = new Object();
    private final CountDownLatch latch = new CountDownLatch(2);
    private final Lock lock1 = new ReentrantLock();
    private long electionStart;
    private long electionEnd;


    private final ExecutorService leaderThread = Executors.newFixedThreadPool(1);
    private final ExecutorService acceptThread = Executors.newFixedThreadPool(1);
    private final ExecutorService connThread = Executors.newFixedThreadPool(1);
    private final ExecutorService followerThread = Executors.newFixedThreadPool(1);

    public void incTermAndResetVoted() {
        synchronized (lock) {
            term++;
            voted = false;
        }
    }

    public void start() {

        acceptThread.submit(() -> {
            ServerSocket serverSocket = new ServerSocket(port);
            latch.countDown();
            for (; ; ) {
                try {
                    Socket accept = serverSocket.accept();
                    log.debug("Socket accepted:" + accept);

                    String s = SocketUtils.readStr(accept.getInputStream());
                    JSONParser parser = new JSONParser();
                    Map<String, Object> raftMsg = (Map<String, Object>) parser.parse(s);
                    ServerStatus voteStatus = Enum.valueOf(ServerStatus.class, (String) raftMsg.get("status"));
                    log.info("status:" + voteStatus.toString());
                    synchronized (sleepLock) {
                        sleepLock.notifyAll();
                    }


                    Map<String, Object> raftResp;
                    String jsonString;

                    synchronized (lock) {
                        if (voted) {
                            raftResp = RaftMessage.raftResp(term, host, port, status, false);
                        } else {
                            voted = true;
                            raftResp = RaftMessage.raftResp(term, host, port, status, true);
                        }
                    }

                    jsonString = JSONObject.toJSONString(raftResp);
                    accept.getOutputStream().write(jsonString.getBytes());

                    log.info("client received:{}", raftMsg);
                    accept.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        electionTimer = new Thread(() -> {
            log.info("--------- follower timer started ----------");
            latch.countDown();
            while (true) {
                synchronized (electionLock) {
                    // only follower keeps a timer
                    try {
                        if (status != ServerStatus.FOLLOWER) {
                            log.info("waiting for the server to switch into follower status");
                            electionLock.wait();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                synchronized (sleepLock) {
                    try {
                        long t0 = System.currentTimeMillis();
                        int waitTime = SLOW_FACTOR * 150 + random.nextInt(SLOW_FACTOR * 150);
                        log.info("timer waiting for a random time in milliseconds:" + waitTime);
                        sleepLock.wait(waitTime);
                        if (System.currentTimeMillis() - t0 >= waitTime) {
                            status = ServerStatus.CANDIDATE;
                            log.info("======== timeout ============");
                            sleepLock.notifyAll();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }, "electionTimer");
        electionTimer.start();

        countThread = new Thread(() -> {
            // wait for the
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
                                AtomicInteger aliveConn = new AtomicInteger(0);
                                peers.forEach(peer -> {
                                    // connect each peer
                                    try {
                                        Socket socket = new Socket();
                                        socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()));
                                        aliveConn.incrementAndGet();
                                        Thread.sleep(SLOW_FACTOR * 20);
                                    } catch (InterruptedException | IOException e) {
//                                        e.printStackTrace();
                                        if (e instanceof IOException) {
                                            log.warn("one follower is out of sync,check the connection");
                                        }
                                    }
                                });
                                if (aliveConn.get() < quorumNum) {
                                    status = ServerStatus.FOLLOWER;
                                    break;
                                }
                                log.info("send heart beat success, still being a leader");
                                // if no message is received, then switch state

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
                            synchronized (electionLock) {
                                electionLock.notifyAll();
                            }
                            synchronized (sleepLock) {
                                try {
                                    sleepLock.wait();
                                    log.info("follower has been awakened");
                                    break;
                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
                                }
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

                            AtomicInteger voteCount = new AtomicInteger(1);
                            voted = true;
                            peers.forEach(peer -> {
                                // 不需要给本机发送
                                if (!StringUtils.equals(this.getHost(), peer.getHost())
                                        || this.getPort() != peer.getPort()) {
                                    try {
                                        Socket socket = new Socket();
                                        socket.getLocalAddress();
                                        socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()));
                                        OutputStream outputStream = socket.getOutputStream();

                                        Map<String, Object> msg = RaftMessage.raftMessage(term, this.getHost(), this.getPort(), status, "test");
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
                                        ServerStatus status = ServerStatus.valueOf((String) resp.get("status"));
                                        if (status == ServerStatus.CANDIDATE) {
                                            cancelCand = true;
                                        }
                                        if ((Boolean) resp.get("voted")) {
                                            voteCount.incrementAndGet();
                                        }
                                        log.info("server received:{}", readStr);
                                        socket.close();
                                    } catch (Exception e) {
//                                    e.printStackTrace();
                                    }
                                }
                            });

                            incTermAndResetVoted();
                            if (voteCount.get() >= quorumNum) {
                                status = ServerStatus.LEADER;
                                break;
                            }
                            if (cancelCand) {
                                cancelCand = false;
                                status = ServerStatus.FOLLOWER;
                                break;
                            }
                        }
                }
            }
        });
        countThread.start();

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
