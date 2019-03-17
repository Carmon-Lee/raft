package org.liguang.raft.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author liguang
 * @desciprtion
 */

public class SocketUtils {


    /**
     * read inputstream until a end token '}' is found
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static String readStr(InputStream inputStream) throws IOException {
        byte[] reveive = new byte[1024];
        int index = 0;
        char read;

        do {
            read = (char) inputStream.read();
            reveive[index++] = (byte) read;
        }
        while (read != '}');

        return new String(reveive, 0, index);
    }

}
