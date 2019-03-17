package org.liguang.raft;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liguang
 */

public class RaftMessage {

    public static Map<String, Object> raftMessage(int term,
                                                  String host,
                                                  int port,
                                                  String content) {
        Map<String, Object> result = new HashMap<>();
        result.put("term", term);
        result.put("host", host);
        result.put("port", port);
        result.put("content", content);
        return result;
    }

    public static Map<String, Object> raftResp(int term,
                                                  String host,
                                                  int port,
                                                  boolean voted) {
        Map<String, Object> result = new HashMap<>();
        result.put("host", host);
        result.put("port", port);
        result.put("term", term);
        result.put("voted", voted);
        return result;
    }
}


