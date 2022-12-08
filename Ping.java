package ping;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.exit(-1);
        }

        String host = args[0];
        int port = 0;

        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 10 * 1000);
            System.exit(0);
        } catch (IOException e) {
            System.exit(1);
        }
    }
}