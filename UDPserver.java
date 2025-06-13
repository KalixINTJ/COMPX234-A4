import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class UDPserver{
    //The data block size (1000 bytes).
    public static int bufferSize = 1000;
    public static int minPort = 50000;
    public static int maxPort = 51000;
    public static int sPort;
    //The main server socket is used to receive requests.
    public static DatagramSocket sSocket;
    //Make sure there is enough space to receive these data packets.
    public static int bigBufferSize = 5000;

    public UDPserver(int p){
        this.sPort = p;
        try{
            //To create a UDP socket and bind it to the specified port.
            sSocket = new DatagramSocket(sPort);
            System.out.println("The server starts at port:" + sPort);
        }catch(SocketException e){
            //If the creation of the socket fails, print the error message and exit the program.
            System.err.println("Error!The server socket cannot be created.");
            System.exit(1);
        }
    }

    public class ClientHandler implements Runnable{
        //Client address and port.
        public InetAddress cAddress;
        public  int cPort;
        public String filename;
        //A UDP socket for file transfer.
        public DatagramSocket cSocket;
   
        public ClientHandler(InetAddress address, int port, String filename) {
            this.cAddress = address;
            this.cPort = port;
            this.filename = filename;
        }

          public void sendErrorResponse() throws IOException {
            String response = "ERR " + filename + " NOT_FOUND";
            byte[] responseData = response.getBytes();
            //To create and send error response data packet.
            DatagramPacket responsePacket = new DatagramPacket(responseData, responseData.length, cAddress, cPort);
            sSocket.send(responsePacket);
            System.out.println("Send response: " + response);
        }

        public void sendOkResponse(long fileSize, int port) throws IOException {
            String response = "OK " + filename + " SIZE " + fileSize + " PORT " + port;
            byte[] responseData = response.getBytes();
            //To create and send the OK response data packet.
            DatagramPacket responsePacket = new DatagramPacket(responseData, responseData.length, cAddress, cPort);
            sSocket.send(responsePacket);
            System.out.println("Send response: " + response);
        }

        @Override
        public void run(){
            try{
                //To create a file object.
                File file = new File(filename);
                //Randomly select a transmission port.
                int transferPort = ThreadLocalRandom.current().nextInt(minPort, maxPort + 1);
                cSocket = new DatagramSocket(transferPort);
                sendOkResponse(file.length(), transferPort);
                //To handle file transfer.
                handleFileTransfer(file);
            } catch (IOException e) {
                System.err.println("An error occurred when handling the client request: " + e.getMessage());
            }
        }

        public void handleFileTransfer(File file) throws IOException {
            //To create a file input stream.
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                //To create the receiving buffer and data packet.
                byte[] receiveBuffer = new byte[bigBufferSize];
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                while (true) {
                    cSocket.receive(receivePacket);
                    //To convert the received data to a string.
                    String request = new String(receivePacket.getData(), 0, receivePacket.getLength()).trim();
                    System.out.println("Received the file request: " + request);
                    cAddress = receivePacket.getAddress();
                    cPort = receivePacket.getPort();
                    //To handle GET requests.
                    if (request.startsWith("FILE") && request.contains("GET")) {
                        //To parse byte range.
                        int startPos = request.indexOf("START") + 6;
                        int endPosMarker = request.indexOf("END");
                        if(startPos >= 6 && endPosMarker > 0){
                            int start = Integer.parseInt(request.substring(startPos, endPosMarker).trim());
                            int end = Integer.parseInt(request.substring(endPosMarker + 4).trim());
                            //To send the file data block.
                            sendFileData(fileInputStream, start, end);
                        }
                    } else if (request.startsWith("FILE") && request.contains("CLOSE")) {
                        //To handle the closing request.
                        String response = "FILE " + filename + " CLOSE_OK";
                        byte[] responseData = response.getBytes();
                        //To create and send the shutdown response data packet.
                        DatagramPacket closepacket = new DatagramPacket(responseData, responseData.length, cAddress, cPort);
                        cSocket.send(closepacket);
                        System.out.println("File transfer completed. Close the connection: " + filename);
                        break;
                    }
                }
            }
        }
        public void sendFileData(FileInputStream fileInputStream, int start, int end) throws IOException {
            int length = Math.min(end - start + 1, bufferSize);
            //To create a data buffer.
            byte[] dataBuffer = new byte[length];
            //Locate to the specified position of the file.
            fileInputStream.getChannel().position(start);
            int bytesRead = fileInputStream.read(dataBuffer, 0, length);
            if (bytesRead > 0) {
                //The encoded data is Base64
                String encodedData = Base64.getEncoder().encodeToString(Arrays.copyOf(dataBuffer, bytesRead));
                 String response = "FILE " + filename + " OK START " + start + " END " + (start + bytesRead - 1) + " DATA " + encodedData;
                //To create and send data packets.
                byte[] responseData = response.getBytes();
                DatagramPacket responsePacket = new DatagramPacket(responseData, responseData.length, cAddress, cPort);
                cSocket.send(responsePacket);
                System.out.println("Send file data: " + filename + " Byte range " + start + "-" + (start + bytesRead - 1));
            }
        }
    }

    public void Run() throws IOException {
        while(true){
            //To create the receiving buffer and data packets.
            byte[] rBuffer = new byte[bigBufferSize];
            DatagramPacket rPacket = new DatagramPacket(rBuffer,rBuffer.length);
            System.out.println("Wait for the client connection...");

            //To receive client requests.
            sSocket.receive(rPacket);                
            //To convert the received data to a string.
            String message = new String(rPacket.getData(), 0, rPacket.getLength()).trim();
            System.out.println("Receive the message: " + message);

            if(message.startsWith("DOWNLOAD")){
                String filename = message.substring("DOWNLOAD".length()).trim();
                //Make sure the file name is valid.
                if(!filename.isEmpty()){
                    ClientHandler handler = new ClientHandler(rPacket.getAddress(), rPacket.getPort(), filename);
                    new Thread(handler).start();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 1){
            System.out.println("Usage: java UDPserver < Port number >.");
        }
        try{
            //To parse the port number.
            sPort = Integer.parseInt(args[0]);
            //To create a server instance and start it.
            new UDPserver(sPort).Run();
        }catch(NumberFormatException e){
            System.out.println("Error!The port number must be an integer.");
        }
    }
}