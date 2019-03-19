package org.liguang.raft.server;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.liguang.raft.RaftMessage;
import org.liguang.raft.util.SocketUtils;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
    private List<Socket> accepts = new ArrayList<>();
    private ConcurrentHashMap<Socket, ServerNode> clientSocketRegistry = new ConcurrentHashMap<>();
    private Socket socket;


    private Random random = new Random();
    private Thread countThread;
    private Thread electionTimer;
    private final Object lock = new Object();
    private final Object electionLock = new Object();
    private final CountDownLatch latch = new CountDownLatch(2);
    private final Lock lock1 = new ReentrantLock();
    private long electionStart;
    private long electionEnd;


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
                    if (voteStatus == ServerStatus.FOLLOWER) {
                        electionTimer.interrupt();
                    }


                    Map<String, Object> raftResp;
                    String jsonString;

                    synchronized (lock) {
                        if (voted) {
                            raftResp = RaftMessage.raftResp(term, host, port, false);
                        } else {
                            voted = true;
                            raftResp = RaftMessage.raftResp(term, host, port, true);
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
            log.info("--------- follower timing ----------");
            latch.countDown();
            while (true) {
                synchronized (electionLock) {
                    // only follower keeps a timer
                    try {
                        if (status != ServerStatus.FOLLOWER) {
                            electionLock.wait();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    Thread.sleep(SLOW_FACTOR * 150 + random.nextInt(SLOW_FACTOR * 150));
                    status = ServerStatus.CANDIDATE;
                    log.info("======== timeout ============");
                } catch (InterruptedException e) {
                    log.info("======== heart beat =========");
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
                                Future<String> submit = leaderThread.submit(() -> {
                                    peers.forEach(peer -> {
                                        // connect each peer
                                        try {
                                            Thread.sleep(SLOW_FACTOR * 20);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    });
                                    return "success";
                                });
                                String s = submit.get(SLOW_FACTOR * 100, TimeUnit.MILLISECONDS);
                                log.info("send heart beat success, still being a leader");
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
                            synchronized (electionLock) {
                                electionLock.notifyAll();
                            }
                            try {
//                                voted = true;
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

                            AtomicInteger voteCount = new AtomicInteger(1);
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

                            incTerm();
                            if (voteCount.get() >= 2) {
                                status = ServerStatus.LEADER;
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
