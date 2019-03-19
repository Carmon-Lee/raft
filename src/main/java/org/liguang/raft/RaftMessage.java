package org.liguang.raft;

import lombok.Data;
import org.liguang.raft.server.ServerStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liguang
 */

public class RaftMessage {

    public static Map<String, Object> raftMessage(long term,
                                                  String host,
                                                  int port,
                                                  ServerStatus status,
                                                  String content) {
        Map<String, Object> result = new HashMap<>();
        result.put("host", host);
        result.put("port", port);
        result.put("term", term);
        result.put("status", status.toString());
        result.put("content", content);
        return result;
    }

    public static Map<String, Object> raftResp(long term,
                                               String host,
                                               int port,
                                               ServerStatus status,
                                               boolean voted) {
        Map<String, Object> result = new HashMap<>();
        result.put("host", host);
        result.put("port", port);
        result.put("term", term);
        result.put("status", status.toString());
        result.put("voted", voted);
        return result;
    }
}


