import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.Charset;

public
class gms_tcp_server {
    
public
    static void gms_tcp_in_buffer(InputStream inputStream, OutputStream outputStream)
    {
        int i = 0;
        byte[] buffer = new byte[256];
        String[] data = {"aaaabbbb",
                           "ccccdddd",
                           "eeeeffff",
                           "gggghhhh",
                           "01234567"};
        while (true) {
            try {
                int n = inputStream.read(buffer);
                String inputMsg = new String(buffer, 0, n);
                inputMsg = inputMsg.replaceAll("[\n\r]", "");

                if (inputMsg.equals("ok")) {
                    outputStream.write(data[i].getBytes());
                    System.out.println("Send msg: " + data[i]);
                    i = (i + 1) % 5;
                }
            } catch (Exception e) {
                break;
            }
        }
    }
    
public
    static void gms_tcp_char_set(InputStream inputStream, OutputStream outputStream)
    {
        byte[] inputMsg = new byte[64];
        
        try {
            String outString = "abcdefg";
            String outMsg = new String(outString.getBytes("GBK"));
            outputStream.write(outMsg.getBytes());
        } catch (Exception e) {
            return;
        }
    }

public
    static void gms_tcp_get_line(InputStream inputStream, OutputStream outputStream)
    {
        String outMsg = "get line, abcdefg1234567890\n";
        
        try {
            outputStream.write(outMsg.getBytes());
            System.out.println("Send msg: " + outMsg);
        } catch (Exception e) {
            return;
        }
    }
    
public
    static void gms_tcp_get_text(InputStream inputStream, OutputStream outputStream)
    {
        String outMsg = "get text, abcdefg1234567890\n";
        
        try {
            outputStream.write(outMsg.getBytes());
            System.out.println("Send msg: " + outMsg);
        } catch (Exception e) {
            return;
        }
    }
    
public
    static void gms_tcp_get_raw(InputStream inputStream, OutputStream outputStream)
    {
        byte[] outMsg = {1,2,3,4,5,6,7,8,9,10};
        
        try {
            outputStream.write(outMsg);
        } catch (Exception e) {
            return;
        }
    }
    
public
    static void gms_tcp_read_line(InputStream inputStream, OutputStream outputStream)
    {
        String outMsg = "read line, abcdefg1234567890\n";
        
        try {
            outputStream.write(outMsg.getBytes());
            System.out.println("Send msg: " + outMsg);
        } catch (Exception e) {
            return;
        }
    }
    
public
    static void gms_tcp_read_text(InputStream inputStream, OutputStream outputStream)
    {
        String outMsg = "read text, abcdefg1234567890\n";
        
        try {
            outputStream.write(outMsg.getBytes());
            System.out.println("Send msg: " + outMsg);
        } catch (Exception e) {
            return;
        }
    }

public
    static void gms_tcp_write_line(InputStream inputStream, OutputStream outputStream)
    {
        byte[] inputMsg = new byte[64];
        
        try {
            int n = inputStream.read(inputMsg);
            String inputMsgString = new String(inputMsg, 0, n);
            System.out.println(inputMsgString);
        } catch (Exception e) {
            return;
        }
    }
    
public
    static void gms_tcp_write_text(InputStream inputStream, OutputStream outputStream)
    {
        byte[] inputMsg = new byte[64];
        
        try {
            int n = inputStream.read(inputMsg);
            String inputMsgString = new String(inputMsg, 0, n);
            System.out.println(inputMsgString);
        } catch (Exception e) {
            return;
        }
    } 
    
public
    static void gms_tcp_read_raw(InputStream inputStream, OutputStream outputStream)
    {
        byte[] outMsg = {1,2,3,4,5,6,7,8,9,10};
        
        try {
            outputStream.write(outMsg);
        } catch (Exception e) {
            return;
        }
    }

public
    static void main(String[] args)
    {
        try {
            int port = 12358;
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Server is ok.");
            while (true) {
                boolean quit = false;
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connect ok.");
                
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();
                
                while (true) {
                    try {
                        byte[] testType = new byte[64];
                        int n = inputStream.read(testType);
                        String testTypeString = new String(testType, 0, n);
                        testTypeString = testTypeString.replaceAll("[\n\r]", "");
                        System.out.println("start test: " + testTypeString);
                        
                        if (testTypeString.equals("get line")) {
                            gms_tcp_get_line(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("get text")) {
                            gms_tcp_get_text(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("get raw")) {
                            gms_tcp_get_raw(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("read line")) {
                            gms_tcp_read_line(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("read text")) {
                            gms_tcp_read_text(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("read raw")) {
                            gms_tcp_read_raw(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("in buffer")) {
                            gms_tcp_in_buffer(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("write line")) {
                            gms_tcp_write_line(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("write text")) {
                            gms_tcp_write_text(inputStream, outputStream);
                            break;
                        } else if (testTypeString.equals("quit")) {
                            quit = true;
                            break;
                        } else {
                            gms_tcp_char_set(inputStream, outputStream);
                            break;
                        }
                    } catch (Exception e) {
                        break;
                    }
                }
                
                if (quit) {
                    break;
                }
                
                System.out.println("one client over\n===================================================================\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
