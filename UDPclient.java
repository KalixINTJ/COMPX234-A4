import java.io.*;
import java.net.*;
import java.util.*;

public class UDPclient{
    //The data block size (1000 bytes).
    public static int bufferSize = 1000;
    //The hostname and port number of the server, as well as the list of files containing the file names to be downloaded.
    public static String sHost;
    public static int sPort;
    public static String fileListName;
    //The UDP socket, used for communication.
    public static DatagramSocket cSocket;
    //Make sure there is enough space to receive these data packets.
    public static int bigBufferSize = 10000;
    //Timeout period(2000 ms).
    public static int time = 2000;
    //Maximum number of retries.
    public static int maxTry = 5; 

    public UDPclient(String h, int p, String f){
        this.sHost = h;
        this.sPort = p;
        this.fileListName = f;
        try{
            //To create a UDP socket.
            cSocket = new DatagramSocket();
        }catch(SocketException e) {
            //If the creation of the socket fails, print the error message and exit the program.
            System.err.println("Error!The client socket cannot be created.");
            System.exit(1);
        }
    }

    public List<String> readFileList(String name) throws IOException{
        //To create a list of file names.
        List<String> files = new ArrayList<>();
        //To use BufferedReader to read the file list.
        try(BufferedReader reader = new BufferedReader(new FileReader(name))){
            String line;
            while((line = reader.readLine()) != null){
                //To remove the first and last whitespace characters and ignore the blank lines.
                line = line.trim();
                if(!line.isEmpty()){
                    files.add(line);
                }
            }
        }
        return files;
    }
    //To send a message and wait for receiving.
    public String sendAndReceive(String m, String h, int p) throws IOException{
        //TO parse the address of the target host.
        InetAddress sAdress = InetAddress.getByName(h);
        //To convert the message to a byte array and create a receiving buffer.
        byte[] sendData = m.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, sAdress, p);
        byte[] rBuffer = new byte[bigBufferSize];
        DatagramPacket receivPacket = new DatagramPacket(rBuffer, rBuffer.length);
        //To initialize the retry count and the current timeout time.
        int retries = 0;
        int cTimeout = time;

        while (retries < maxTry) {
            try {
                //To send data packets and receive responses from the server.
                cSocket.send(sendPacket);
                System.out.println("Send: " + m);
                cSocket.receive(receivPacket);
                String response = new String(receivPacket.getData(), 0, receivPacket.getLength());
                System.out.println("Receive: " + response);
                return response;
            } catch (SocketTimeoutException e) {
                //If a timeout occurs, increase the retry count and adopt an exponential backoff strategy.
                retries++;
                System.out.println("Attempt to retransmit...(" + retries + "/" + maxTry + ")");
                //Exponential retreat.
                cTimeout *= 2;
                cSocket.setSoTimeout(cTimeout);
            }
        }
        throw new IOException("Reach the maximum number of retransmissions.");
     }
    //To download the specified file.
    public void downloadFile(String filename) {
        try {
            System.out.println("Start downloading the file: " + filename);
            String downloadRequest = "DOWNLOAD " + filename;
            //To send requests and receive responses from the server.
            String response = sendAndReceive(downloadRequest, sHost, sPort);
            if (response.startsWith("ERR")) {
                System.out.println("Error: " + response);
                return;
            }
            if (response.startsWith("OK")) {
                //To find the positions of the keywords "SIZE" and "PORT".
                int sizeIndex = response.indexOf("SIZE");
                int portIndex = response.indexOf("PORT");
                if (sizeIndex == -1 || portIndex == -1) {
                    System.out.println("Error: " + response);
                    return;
                }
                //To extract the file size and transfer port.
                long fileSize = Long.parseLong(response.substring(sizeIndex + 5, portIndex).trim());
                int transferPort = Integer.parseInt(response.substring(portIndex + 5).trim());
                System.out.println("The server accepts the download request. File size:" + fileSize + "Byte, transmission port: :" + transferPort);
                //To create a local file.
                try (FileOutputStream fileOutputStream = new FileOutputStream(filename)) {
                    long bytesReceived = 0;
                    //To receive file data blocks in a loop until the entire file is received.
                    while (bytesReceived < fileSize) {
                        //To calculate the start and end bytes of the current data block.
                        long start = bytesReceived;
                        long end = Math.min(start + bufferSize  - 1, fileSize - 1);
                        String fileRequest = "FILE " + filename + " GET START " + start + " END " + end;
                        //To send requests and receive responses.
                        String fileResponse = sendAndReceive(fileRequest, sHost, transferPort);
                        if (fileResponse.startsWith("FILE") && fileResponse.contains("OK")) {
                            String encodedData = fileResponse.substring(fileResponse.indexOf("DATA") + 5).trim();
                            //To decode data.
                            byte[] data = Base64.getDecoder().decode(encodedData);
                            //To write the decoded data to the local file.
                            fileOutputStream.write(data);
                            bytesReceived += data.length;
                            //To show progress.
                            System.out.print("*");
                            System.out.print(String.format(" %.1f%%\r", (double) bytesReceived / fileSize * 100));
                        } else {
                            System.out.println("Invalid file data response: " + fileResponse);
                            return;
                        }
                    }
                    System.out.println("\nFile download completed: " + filename+ " (" + bytesReceived + " 字节)");
                    //To send a closing request and wait for a response.
                    String closeRequest = "FILE " + filename + " CLOSE";
                    String closeResponse = sendAndReceive(closeRequest, sHost, transferPort);
                    if (closeResponse.startsWith("FILE") && closeResponse.contains("CLOSE_OK")) {
                        System.out.println("The transmission connection has been normally closed.");
                    } else {
                        System.out.println("Error: " + closeResponse);
                    }
                }
            } else {
                System.out.println("Error: " + response);
            }
        } catch (IOException e) {
            System.err.println("Error occurred when downloading the file: " + e.getMessage());
        }
    }
    public void run() throws IOException {
            //To read the file list and obtain the list of file names that need to be downloaded.
            List<String> files = readFileList(fileListName);
            for(String file : files){
                downloadFile(file);
            }
            if (cSocket != null && !cSocket.isClosed()) {
            cSocket.close();
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 3){
            System.out.println("Usage: java UDPclient < Server hostname > < Port number > < File list >.");
        }
        try{
            //To parse the command-line parameters and create the client instance and start it.
            UDPclient client = new UDPclient(args[0], Integer.parseInt(args[1]), args[2]);
            client.run();
        }catch(NumberFormatException e){
            System.out.println("Error!Usage: java UDPclient < Server hostname > < Port number > < File list > and the port number must be an integer.");
        }
    }
}