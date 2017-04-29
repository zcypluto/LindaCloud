/**
 * Created by Plutorian1 on 4/16/17.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.*;


public class Server{
    private List<ServerInfo> hostList;
    private int hostNum;
    private int hostPort;
    private String hostIP;
    private ServerInfoList srvInfo;
    private Tuples tuplesInfo;
    private String hostName;
    private String loginName;
    private ServerSocket serverSocket;
    private MD5Digest md5Dig;
    private Map<Integer, String> hostMap;
    private String acknowledge;

    public Server() {
        hostList = new ArrayList<>();
        srvInfo = new ServerInfoList();
        tuplesInfo = new Tuples();
        md5Dig = new MD5Digest();
        hostMap = new HashMap<>();
        hostNum = 0;
        loginName = "czhang3";
        acknowledge = null;
    }

    /* 1. Capture current ip and valid port
     * 2. while loop for command
     */
    public void start(String host, String login) throws IOException {
        hostName = host;
        loginName = login;
        int port = 0;
        // Get current IP
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                // filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while(addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    hostIP = addr.getHostAddress();
                    //System.out.println(iface.getDisplayName() + " " + ip);
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        // Build serverSocket socket and auto-generate port number
        serverSocket = new ServerSocket(port);//port=0
        hostPort = serverSocket.getLocalPort();

        // print message to confirm we get host information
        System.out.println(hostIP + " at the port number: "+ hostPort);

        //reboot host
        //Reboot();

        // 1. wait for command
        waitforcommand();
        // 2. receive of client message
        try {
            //System.out.println("wait for client message");
            while(true){
                Socket socket = serverSocket.accept();
                invoke(socket);
            }
        }
        finally{
            serverSocket.close();
        }
    }

    // Distribute all hosts' tuples
    private boolean DistributeTuple(boolean isServer){
        // 1. check local tuple and distribute
        List<String> tuples = tuplesInfo.GetTuplesContent(loginName, hostName);
        if (tuples != null) {
            //System.out.println("[Info] distribute - check tuples in " + hostName);
            for (int j = 0; j < tuples.size(); j++) {
                String tmp = tuples.get(j);
                tuplesInfo.deletetuple(tmp, loginName, hostName); //remove original tuple
                lindaOutcommand(tmp); // re-distribute tuple one by one
            }
        }

        // 2. notify other net to distribute if host is server
        if (!isServer) {
            //System.out.println("[Info] " + hostName + " is done for tuples distribution");
            return true;
        }
        String allhosts = srvInfo.GetNetsContent(loginName, hostName);
        if (allhosts == null) {
            System.out.println("Error: cannot retrieve nets information on " + hostName);
            return false;
        }
        String[] allhost = allhosts.split("\n");
        for(int i = 0; i < allhost.length; i++){
            String[] hostinfo = allhost[i].split(",");
            if (!hostinfo[0].equals(hostName)) {
                //System.out.println("[Info] notify " + hostinfo[0] + " to re-distribute tuples");
                sendAndSync(false, "[Disadd]", hostinfo[1], Integer.valueOf(hostinfo[2]));
            }
        }
        return true;
    }

    // Update net file and distribute all tuples
    private void updateNetAndTuples() {
        // write to net file
        srvInfo.writenets(hostList, loginName, hostName);
        try {
            String slcontent = srvInfo.GetNetsContent(loginName, hostName);
            slcontent = slcontent.replaceAll("\n", "\t"); // replace \n to \t
            List<ServerInfo> sl = srvInfo.loadnets(loginName, hostName);
            for(ServerInfo s : sl){
                if(s.getIP().equals(hostIP) && (s.getPORT() == hostPort)){
                    continue;
                }
                Socket sk = new Socket(s.getIP(), s.getPORT());
                PrintWriter out = new PrintWriter(sk.getOutputStream(),true);
                out.print("[add]" + slcontent);// call add function
                //out.print(slcontent);
                out.close();
            }
        } catch (IOException e){
            System.out.println("unable to create nets content");
        }

        // Distribute tuples when adding new machines
        // TODO: workaround wait for a while: ensure all hosts net is created
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.out.println("Error: " + hostName + " can not sleep");
        }
        DistributeTuple(true);
    }

    // Linda Delete command
    private void lindaDeletecommand(String cmd){
        String msg = cmd.substring(6, cmd.length()); //remove delete
        msg = msg.replaceAll("[\\(\\)\\s]+", ""); //remove white space
        String[] removehostslist = msg.split(",");
        HashMap<String, ServerInfo> map = new HashMap<>();

        if (hostList.isEmpty()) {
            System.out.println("Error: cannot delete host because of no host record");
            return;
        }

        for (int i = 0; i < hostList.size(); i++) {
            ServerInfo sinfo = hostList.get(i);
            map.put(sinfo.getName(), sinfo);
        }
        // remove host from the hostList and send delete message to target host
        List<ServerInfo> slist = srvInfo.loadnets(loginName, hostName);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < removehostslist.length; i++) {
            String name = removehostslist[i];
            if (map.containsKey(name)) {
                ServerInfo sinfo = map.get(name);
                // filter delete host information
                for (int j = 0; j < slist.size(); j++) {
                    if (!sinfo.getName().equals(slist.get(j).getName())) {
                        sb.append(slist.get(j).getName() + ",");
                        sb.append(slist.get(j).getIP() + ",");
                        sb.append(slist.get(j).getPORT() + "\t");
                    }
                }
                // remove host list record
                hostList.remove(map.get(name));
            }
        }

        // update net files
        for (int i = 0; i < removehostslist.length; i++) {
            String name = removehostslist[i];
            boolean isdelete = false;
            if (map.containsKey(name)) {
                ServerInfo sinfo = map.get(name);
                isdelete = sendAndSync(true, "[Disdel][" + hostName + "]" + sb.toString(), sinfo.getIP(), sinfo.getPORT());
                if (!isdelete) {
                    System.out.println("Error: cannot delete " + sinfo.getName());
                }
            }
        }
        updateHostIdNameMap();

        srvInfo.writenets(hostList, loginName, hostName);
        // debug
        /*
        for (int i = 0; i < hostList.size(); i++) {
            ServerInfo host = hostList.get(i);
            System.out.println(host.getName() + ":" + host.getPORT());
        }
        */
    }

    // Linda command: Add function
    private void lindaAddcommand(String cmd) {
        String msg = cmd.substring(3, cmd.length()); //remove add
        msg = msg.replaceAll("\\s+", ""); //remove white space
        StringBuilder sb = new StringBuilder();
        String host = null;
        String ip = null;
        int port = 0, cnt = 0;

        // add self net information once
        if (hostList.isEmpty()) {
            hostList.add(new ServerInfo(hostName, hostIP, hostPort));
        }
        for (int i = 0; i < msg.length(); i++) {
            char c = msg.charAt(i);
            if (c != '(' && c != ')' && c != ',' && c != ' ') {
                sb.append(c);
            }
            if (c == ',') {
                if (cnt == 0) {
                    host = sb.toString();
                    if (host.equals(hostName)) {
                        System.out.println("Error: " + host + " is used, rename it");
                        return;
                    }
                    sb.setLength(0);
                    cnt++;
                } else if (cnt == 1) {
                    ip = sb.toString();
                    sb.setLength(0);
                    cnt++;
                }
            } else if (c == ')' && cnt == 2) {
                port = Integer.valueOf(sb.toString());
                sb.setLength(0);
                cnt++;
            }
            if (cnt == 3) {
                // add other host net information
                hostList.add(new ServerInfo(host, ip, port));
                cnt = 0;
            }
        }
        hostIdNameMap();
        // debug
        /*
        for (ServerInfo info : hostList) {
           System.out.println(info.getName() + ", " + info.getIP() + ", " + info.getPORT());
        }
        */
        updateNetAndTuples();
    }

    // Check data type: String, Integer or Float
    private String checkType(String in) {
        String type = "";
        // check String first
        if (in.contains("?") && in.contains(":")) {
            return in; // variable match
        } else if (in.contains("\"")) {
            type = in.replaceFirst("\"", ""); //replace first "
            type = "S_" + type;
        } else if (in.contains(".")) {
            type = "F_" + in;
        } else {
            type = "I_" + in;
        }
        return type;
    }

    // Send messages to server and check feedback
    private boolean sendAndSync(boolean sync, String msg, String ip, int port) {
        // send message
        try {
            Socket socket = new Socket(ip, port);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            // send the message to other host
            out.write(msg);
            out.flush();
            out.close();
            socket.close();
        } catch (IOException e) {
            System.out.println("Error: cannot open socket + " + ip + ":" + port);
            return false;
        }

        if (sync) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (acknowledge != null) {
                if (acknowledge.startsWith("success")) {
                    return true;
                } else if (acknowledge.startsWith("fail")) {
                    return false;
                }
            } else {
                return false;
            }
        }

        return true;
    }

    // Hash map to host id and host name
    private void hostIdNameMap() {
        for (int i = 0; i < hostList.size(); i++) {
            ServerInfo host = hostList.get(i);
            if (!hostMap.containsKey(i)) {
                hostMap.put(i, host.getName());
            }
        }
        hostNum = hostList.size();
    }

    // Refresh Hash map if delete host information
    private void updateHostIdNameMap() {
        hostMap.clear();
        hostIdNameMap();
    }

    // Hash map for MD5 hashcode
    private int calhostID(int tuplehash){
        int hostID = 0;
        int numofslots = hostList.size();
        int slots=(int)Math.pow(2,16)-1;
        int slotavg=slots/numofslots;

        for(int i = 0; i < hostList.size(); i++) {
            if (tuplehash > slotavg) {
                slotavg += slotavg;
                continue;
            } else {
                hostID=i;
                break;
            }
        }
        return hostID;
    }

    // Create socket and send the command to other host
    private boolean sendMessages(boolean sync, String msg, int hostid) {
        // hash to determine a target host
        if (hostid == -1) {
            System.out.println("Error: can not hashfunction");
            return false;
        }
        // check hostid and get target host name
        String targethost = hostMap.get(hostid);
        // read nets file to collect complete target host info.
        ServerInfo host = srvInfo.readnets(loginName, hostName, targethost);
        if (host == null) {
            System.out.println("Error: cannot find target host - " + targethost);
            return false;
        }

        // if contain ?variable_name:type, search all hosts
        while (true) {
            boolean done = false;
            if (msg.contains("?") && msg.contains(":")) {
                String allhosts = srvInfo.GetNetsContent(loginName, hostName);
                String[] allhost = allhosts.split("\n");
                for (int i = 0; i < allhost.length; i++) {
                    String[] hostinfo = allhost[i].split(",");
                    done = sendAndSync(true, msg, hostinfo[1], Integer.valueOf(hostinfo[2]));
                    if (done) {
                        break;
                    }
                }
            } else {
                // send and sync
                done = sendAndSync(sync, msg, host.getIP(), host.getPORT());
                if (done) {
                    break;
                }
                /*
                if (!sendAndSync(sync, msg, host.getIP(), host.getPORT())) {
                    return false;
                }
                */
            }
            if (done) {
                break;
            }
        }

        // print message to confirm the data transaction completely
        String tmp = msg.replaceAll("S_", "\"");
        tmp = tmp.replaceAll("F_", "");
        tmp = tmp.replaceAll("I_", "");
        if (msg.contains("[out]")) {
            tmp = tmp.substring(5, tmp.length()); //remove [out]
            tmp = tmp.substring(hostName.length() + 2, tmp.length()); //remove [hostname]
            System.out.println("put tuple (" + tmp + ") on " + host.getIP());
        } else if (msg.contains("[rd]") || msg.contains("[in]")) {
            tmp = tmp.substring(4, tmp.length()); //remove [out]
            tmp = tmp.substring(hostName.length() + 2, tmp.length()); //remove [hostname]
            //System.out.println("get tuple (" + tmp + ") on " + host.getIP());
            System.out.println("get tuple (" + acknowledge.replaceFirst("success", "") + ") on " + host.getIP());
        }

        return true;
    }

    // Linda commnad: basic
    private String lindaBasic(String cmd) {
        StringBuilder sb = new StringBuilder();
        if (cmd.contains(",")) {
            String[] data = cmd.split(",");
            int len = data.length;
            String prefix = "";
            for (int i = 0; i < len; i++) {
                sb.append(prefix + checkType(data[i]));
                prefix = ",";
            }
        } else {
            sb.append(cmd);
        }
        return sb.toString();
    }

    // Linda command: Out function
    private void lindaOutcommand(String cmd) {
        String msg = lindaBasic(cmd);

        int tuplecode = md5Dig.HashFunc(msg);
        int hostid = calhostID(tuplecode);
        System.out.println("tuplecode =" + tuplecode);
        System.out.println("hostid =" + hostid);
        // send message
        if (!sendMessages(false, "[out][" + hostName + "]" + msg, hostid) ) {
            System.out.println("Error: Out command failed");
        }
    }

    // Linda command: In function: read and delete data
    private void lindaIncommand(String cmd) {
        String msg = lindaBasic(cmd);
        int tuplecode = md5Dig.HashFunc(msg);
        int hostid = calhostID(tuplecode);
        // send message
        if(!sendMessages(true, "[in][" + hostName + "]"  + msg, hostid)){
            System.out.println("Error: in command failed");
        }
    }

    // Linda command: Rd function, read data
    private void lindaRdcommand(String cmd) {
        String msg = lindaBasic(cmd);
        int tuplecode = md5Dig.HashFunc(msg);
        int hostid = calhostID(tuplecode);
        // send message
        if (!sendMessages(true, "[rd][" + hostName + "]" + msg, hostid) ) {
            System.out.println("Error: Rd command failed");
        }
    }

    // Process command-line command from local host
    private void lindaOthercommand(String cmd) {
        String msg = cmd.replaceAll("\\s+", ""); //remove white space
        if (cmd.startsWith("out") && msg.length() > 5) { //assume at least out()
            msg = msg.substring(4, msg.length() - 1); //remove ( )
            lindaOutcommand(msg);
        }
        if (cmd.startsWith("in") && msg.length() > 4) { //assume at least in()
            msg = msg.substring(3, msg.length() - 1); //remove ( )
            lindaIncommand(msg);
        }
        if (cmd.startsWith("rd") && msg.length() > 4) { //assume at least rd()
            msg = msg.substring(3, msg.length() - 1); //remove ( )
            lindaRdcommand(msg);
        }
    }

    private String gethostname(String msg) {
        StringBuilder sb = new StringBuilder();
        // get host name
        for (int i = 0; i < msg.length(); i++) {
            if (msg.charAt(i) != '[' && msg.charAt(i) != ']') {
                sb.append(msg.charAt(i));
            }
            if (msg.charAt(i) == ']') {
                break;
            }
        }
        return sb.toString();
    }
    // Process received command from other hosts
    private void receivedcommand(String cmd, Socket client) {
        String msg = cmd.replaceAll("S_", "\"");
        msg = msg.replaceAll("F_", "");
        msg = msg.replaceAll("I_", "");
        ServerInfo clientInfo;
        String clienthost;
        // out
        if (msg.startsWith("[out]")) {
            msg = msg.substring(5, msg.length()); //remove [out]
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            System.out.println("\n<receiver>put tuple (" + msg + ") on " + hostIP);
            System.out.print("linda> ");
            tuplesInfo.writetuple(msg, loginName, hostName);
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [out] can not find client information - " + clienthost);
                return;
            }
            //sendAndSync protocol: fail, success
            sendAndSync(false, "success", clientInfo.getIP(), clientInfo.getPORT());
        } else if (cmd.startsWith("[rd]")) {
            msg = msg.substring(4, msg.length()); //remove [rd]
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [rd] can not find client information - " + clienthost);
                return;
            }
            String data = tuplesInfo.readtuple(msg, loginName, hostName);
            String status = data == null ? "fail" : "success" + data;
            sendAndSync(true, status, clientInfo.getIP(), clientInfo.getPORT());
        } else if (msg.startsWith("[in]")){
            msg = msg.substring(4,msg.length());
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [in] can not find client information - " + clienthost);
                return;
            }
            String data = tuplesInfo.readtuple(msg, loginName, hostName);
            if (data != null) {
                tuplesInfo.deletetuple(data, loginName, hostName);
            }
            String status = data == null ? "fail" : "success" + data;
            sendAndSync(true, status, clientInfo.getIP(), clientInfo.getPORT());
        } else if (msg.startsWith("[add]")){
            msg = msg.substring(5, msg.length());
            String[] hostinfos = msg.split("\t");
            hostList.clear(); // remove original net information
            for (int i = 0; i < hostinfos.length; i++) {
                String[] hostinfo = hostinfos[i].split(",");
                hostList.add(new ServerInfo(hostinfo[0], hostinfo[1], Integer.valueOf(hostinfo[2])));
            }
            hostIdNameMap();
            srvInfo.writenets(hostList, loginName, hostName);
        } else if (msg.startsWith("success") || msg.startsWith("fail")) {
            acknowledge = msg;
        } else if (msg.startsWith("[Disadd]")) {
            DistributeTuple(false);
        } else if (msg.startsWith("[Disdel]")) {
            msg = msg.substring(8, msg.length());
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            String[] hostinfos = msg.split("\t");
            hostList.clear(); // remove original net information
            for (int i = 0; i < hostinfos.length; i++) {
                String[] hostinfo = hostinfos[i].split(",");
                hostList.add(new ServerInfo(hostinfo[0], hostinfo[1], Integer.valueOf(hostinfo[2])));
            }
            srvInfo.writenets(hostList, loginName, hostName);
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [delete] can not find server information - " + clienthost);
                return;
            }
            updateHostIdNameMap();
            DistributeTuple(false);
            sendAndSync(false, "success", clientInfo.getIP(), clientInfo.getPORT());
        } else {
            System.out.println("Other: " + msg);
        }
    }

    // wait for command
    private void waitforcommand() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                BufferedReader in = null;
                PrintWriter out = null;
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                System.out.print("linda> ");
                try {
                    while (true) {
                        String msg = reader.readLine();
                        if (msg.startsWith("add")) {
                            lindaAddcommand(msg);
                        } else if (msg.startsWith("delete")){
                            lindaDeletecommand(msg);
                        } else if (msg.equals("exit")) {
                            System.out.println("Bye");
                            System.exit(0);
                        } else {
                            lindaOthercommand(msg);
                        }
                        System.out.print("linda> ");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    // Receive client's messages
    private void invoke(final Socket client) throws IOException{
        new Thread(new Runnable() {
            @Override
            public void run() {
                BufferedReader in = null;
                PrintWriter out = null;
                try {
                    in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    out = new PrintWriter(client.getOutputStream());
                    String line;
                    while((line = in.readLine()) != null) {
                        receivedcommand(line, client);
                    }
                } catch (IOException ex){
                    ex.printStackTrace();
                } finally {
                    try {
                        in.close();
                    } catch (Exception e){
                        e.printStackTrace();
                    } try {
                        out.close();
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}
