package org.liguang.raft;

import org.liguang.raft.util.SocketUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author liguang
 */

public class RaftMain {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.getLocalServer().setPort(Integer.valueOf(args[0]));
        configuration.getLocalServer().start();


//        new Thread(() -> {
//            try {
//                ServerSocket serverSocket = new ServerSocket(22222);
//                Socket accept = serverSocket.accept();
//
//                InputStream inputStream = accept.getInputStream();
//                OutputStream outputStream = accept.getOutputStream();
//
//                outputStream.write("{server}".getBytes());
////                outputStream.close();
//
//                String s = SocketUtils.readStr(inputStream);
//                System.out.println(s);
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }).start();
//
//        new Thread(() -> {
//            try {
//                Socket socket = new Socket();
//                socket.connect(new InetSocketAddress("localhost", 22222));
//
//                InputStream inputStream = socket.getInputStream();
//                OutputStream outputStream = socket.getOutputStream();
//
//                System.out.println(SocketUtils.readStr(inputStream));
//                outputStream.write("{client}".getBytes());
////                outputStream.close();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }).start();

    }
}
