/**
 * Created by Plutorian1 on 4/16/17.
 */
import java.io.*;
import java.net.*;
import java.util.*;


public class Server{
    private List<ServerInfo> hostList;
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
    private int backupID;
    private ServerInfo backupInfo;

    public Server() {
        hostList = new ArrayList<>();
        srvInfo = new ServerInfoList();
        tuplesInfo = new Tuples();
        md5Dig = new MD5Digest();
        hostMap = new HashMap<>();
        loginName = "czhang3";
        acknowledge = null;
        backupID = -1;
    }

    public void cleanAll() {
        hostList.clear();
        srvInfo.clean(loginName, hostName);
        //tuplesInfo.clean(loginName, hostName, false, "");
        updateHostIdNameMap();
        String path = "/tmp/" + loginName + "/linda/" + hostName;
        filesdel(new File(path));
        //folderdel(path);
    }

    // delete all files in given path
    public void filesdel(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                filesdel(f);
            }
        }
        file.delete();
    }

    // delete given path and its folders
    public static void folderdel(String path){
        File f= new File(path);
        if(f.exists()) {
            String[] list= f.list();
            if(list.length == 0) {
                if(f.delete()) {
                    return;
                }
            } else {
                for(int i=0; i < list.length; i++){
                    File f1= new File(path+"\\"+list[i]);
                    if(f1.isFile() && f1.exists()) {
                        f1.delete();
                    }
                    if(f1.isDirectory()) {
                        folderdel(""+f1);
                    }
                }
                folderdel(path);
            }
        }
    }

    // Hash map to host id and host name
    private void hostIdNameMap() {
        for (int i = 0; i < hostList.size(); i++) {
            ServerInfo host = hostList.get(i);
            if (!hostMap.containsKey(i)) {
                //System.out.println("hostid: " + i + " maps to hostname: " + host.getName());
                hostMap.put(i, host.getName());
            }
        }
    }

    // get host id
    private int getHostID() {
        for (Map.Entry<Integer, String> entry : hostMap.entrySet()) {
            //System.out.println("[Info] host id: " + entry.getKey() + " maps to hostname: " + entry.getValue());
            if (entry.getValue().equals(hostName)) {
                return entry.getKey();
            }
        }
        return -1;
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
        if (numofslots == 0) {
            return -1;
        }
        int slots = (int)Math.pow(2,16)-1;
        int slotavg = slots/numofslots;

        for(int i = 0; i < hostList.size(); i++) {
            if (tuplehash > slotavg) {
                slotavg += slotavg;
                continue;
            } else {
                hostID = i;
                break;
            }
        }
        return hostID;
    }

    // decide backup host ID
    private int getBackupID(int hostid) {
        if (hostid == -1) {
            return -1;
        }
        return (hostid + hostList.size()/2) % hostList.size();
    }

    // remove invalid host information from hostList and refresh hostmap
    private void removeInvalidHost(String ip, int port) {
        ServerInfo removeinfo = null;
        for (ServerInfo sinfo : hostList) {
            if (sinfo.getIP().equals(ip) && (sinfo.getPORT() == port)) {
                removeinfo = sinfo;
                break;
            }
        }
        hostList.remove(removeinfo);
    }

    // ==================================================================================
    // Main Entry
    // ==================================================================================
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
                tuplesInfo.deletetuple(tmp, loginName, hostName, false, ""); //remove original tuple
                lindaOutcommand(tmp); // re-distribute tuple one by one
            }
        }
        // 1.1 remove backup tuples
        int newbackupid = getBackupID(getHostID());
        if (newbackupid != backupID && backupID != -1 && backupInfo != null) {
            System.out.println("[Info] ready to remove " + hostName + "'s backup at " + backupInfo.getName() + ":" + backupInfo.getPORT());
            if (!sendAndSync(false, "[backuprm][" + hostName + "]", backupInfo.getIP(), backupInfo.getPORT())) {
                //if (!sendMessages(false, "[backuprm][" + hostName + "]", backupID)) {
                System.out.println("Error: cannot remove " + hostName + "'s backup tuples");
            }
        }
        System.out.println("[Info] hostid: " + getHostID() + " and old backup id: " + backupID + " new backup id:" + newbackupid);
        backupID = newbackupid; //update latest backup id
        if (backupID != -1) {
            //ServerInfo tmp = srvInfo.readnets(loginName, hostName, hostMap.get(backupID));
            // TODO: workaround, read backup info from hostList
            ServerInfo tmp = hostList.get(backupID);
            backupInfo = new ServerInfo(tmp.getName(), tmp.getIP(), tmp.getPORT());
            //System.out.println("[Info]: backup info = " + backupInfo.getName() + "," + backupInfo.getIP() + "," + backupInfo.getPORT());
        } else {
            System.out.println("[Info] No backup host for " + hostName);
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
        if (allhost.length == 0 || hostList.size() == 0) {
            return false;
        }
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
        String ip = null;
        int port = 0;
        // write to net file
        srvInfo.writenets(hostList, loginName, hostName);
        try {
            String slcontent = srvInfo.GetNetsContent(loginName, hostName);
            slcontent = slcontent.replaceAll("\n", "\t"); // replace \n to \t
            List<ServerInfo> sl = srvInfo.loadnets(loginName, hostName);
            for(ServerInfo s : sl){
                ip = s.getIP();
                port = s.getPORT();
                if(ip.equals(hostIP) && (port == hostPort)){
                    continue;
                }
                Socket sk = new Socket(ip, port);
                PrintWriter out = new PrintWriter(sk.getOutputStream(),true);
                out.print("[add][" + hostName +"]" + slcontent);// call add function
                //out.print(slcontent);
                out.close();
            }
        } catch (IOException e){
            System.out.println("unable to open socket " + ip + ":" + port + " to create nets file");
            removeInvalidHost(ip, port);
            updateHostIdNameMap();
            srvInfo.writenets(hostList, loginName, hostName);
        }

        // Distribute tuples when adding new machines
        // TODO: workaround wait for a while: ensure all hosts net is created
        try {
            Thread.sleep(100);
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
        boolean isdeleteself = false;

        if (hostList.isEmpty()) {
            System.out.println("Error: cannot delete host because of no host record");
            return;
        }

        // build delete host map table
        List<ServerInfo> slist = srvInfo.loadnets(loginName, hostName);
        if (slist == null || (hostList.size() != slist.size())) {
            System.out.println("Error: no net files on " + hostName);
            return;
        }
        for (int i = 0; i < removehostslist.length; i++) {
            for (ServerInfo sinfo : hostList) {
                if (sinfo.getName().equals(removehostslist[i])) {
                    map.put(sinfo.getName(), sinfo);
                }
            }
        }
        // remove host from the hostList and send delete message to target host
        StringBuilder sb = new StringBuilder();
        for (ServerInfo sinfo : slist) {
            if (!map.containsKey(sinfo.getName())) {
                // filter delete host information
                sb.append(sinfo.getName() + ",");
                sb.append(sinfo.getIP() + ",");
                sb.append(sinfo.getPORT() + "\t");
            } else {
                // remove host list record
                hostList.remove(map.get(sinfo.getName()));
            }
        }

        // update others net files except itself
        for (ServerInfo sinfo : slist) {
            String name = sinfo.getName();
            //if (!map.containsKey(name) && !name.equals(hostName)) {
            if (!map.containsKey(name)) {
                //try {
                    /*
                    Socket sk = new Socket(sinfo.getIP(), sinfo.getPORT());
                    PrintWriter out = new PrintWriter(sk.getOutputStream(), true);
                    out.print("[add]" + sb.toString());// call add function
                    out.flush();
                    Thread.sleep(5); //wait target host to update nets files
                    out.print("[Disadd]");
                    //System.out.println("[Info]: send update to " + sinfo.getName() + " with " + sb.toString());
                    out.close();
                    */
                sendAndSync(true, "[add][" + hostName + "]" + sb.toString(), sinfo.getIP(), sinfo.getPORT());
                sendAndSync(false, "[Disadd]", sinfo.getIP(), sinfo.getPORT());
                //} catch (IOException e) {
                //    System.out.println("Error: cannot open socket when deleting");
                //} catch (InterruptedException e) {
                //    System.out.println("Error: cannot make a thread to sleep");
                //}
            }
        }
        // update delete net files except itself
        for (Map.Entry<String, ServerInfo> entry : map.entrySet()) {
            if (!entry.getKey().equals(hostName)) {
                ServerInfo sinfo = entry.getValue();
                sendAndSync(true, "[Disdel][" + hostName + "]" + sb.toString(), sinfo.getIP(), sinfo.getPORT());
            } else {
                isdeleteself = true;
            }
        }
        updateHostIdNameMap();
        srvInfo.writenets(hostList, loginName, hostName);
        if (isdeleteself) {
            DistributeTuple(true);
            cleanAll();
        }
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
        char[] tmp=msg.toCharArray();
        if(tmp[0]!='('||tmp[tmp.length-1]!=')'){
            throw new IllegalArgumentException("invalid [add] input format");
        }
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

    // Create socket and send the command to other host
    private boolean sendMessages(boolean sync, String msg, int hostid) {
        // hash to determine a target host
        if (hostid == -1) {
            //System.out.println("Error: can not hashfunction");
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
            //tmp = tmp.substring(hostName.length() + 2, tmp.length()); //remove [hostname]
            tmp = tmp.substring(gethostname(tmp).length() + 2, tmp.length()); //remove [hostname]
            System.out.println("put tuple (" + tmp + ") on " + host.getIP());
        } else if (msg.contains("[backupout]")) {
            tmp = tmp.substring(11, tmp.length()); //remove [backupout]
            tmp = tmp.substring(gethostname(tmp).length() + 2, tmp.length()); //remove [hostname]
            System.out.println("[backup] put tuple (" + tmp + ") on " + host.getIP());
        } else if (msg.contains("[rd]") || msg.contains("[in]")) {
            tmp = tmp.substring(4, tmp.length()); //remove [rd], [in]
            tmp = tmp.substring(hostName.length() + 2, tmp.length()); //remove [hostname]
            //System.out.println("get tuple (" + tmp + ") on " + host.getIP());
            System.out.println("get tuple (" + acknowledge.replaceFirst("success", "") + ") on " + host.getIP());
        } else if (msg.contains("[backuprd]") || msg.contains("[backupin]")) {
            tmp = tmp.substring(10, tmp.length()); //remove [backuprd], [backupin]
            tmp = tmp.substring(hostName.length() + 2, tmp.length()); //remove [hostname]
            System.out.println("[backup] get tuple (" + acknowledge.replaceFirst("success", "") + ") on " + host.getIP());
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

    // Linda command: Out function, also send tuple to backup host
    private void lindaOutcommand(String cmd) {
        String msg = lindaBasic(cmd);

        int tuplecode = md5Dig.HashFunc(msg);
        int hostid = calhostID(tuplecode);
        int backid = getBackupID(hostid);
        //System.out.println("tuplecode =" + tuplecode);
        //System.out.println("hostid =" + hostid);
        if (hostid == -1 || backid == -1) {
            return;
        }
        // send message to target host
        String itself = hostName + "," + hostIP + "," + hostPort;
        //if (!sendMessages(false, "[out][" + hostName + "]" + msg, hostid) ) {
        if (!sendMessages(false, "[out][" + itself + "]" + msg, hostid) ) {
            System.out.println("Error: Out command failed");
        }
        // send message to backup host
        ServerInfo sinfo = srvInfo.readnets(loginName, hostName, hostMap.get(hostid));
        itself = sinfo.getName() + "," + sinfo.getIP() + "," + sinfo.getPORT();
        //if (!sendMessages(false, "[backupout][" + hostMap.get(hostid) + "]" + msg, backid) ) {
        if (!sendMessages(false, "[backupout][" + itself + "]" + msg, backid) ) {
            System.out.println("Error: Out command for backup failed");
        }
    }

    // Linda command: In function: read and delete data
    private void lindaIncommand(String cmd) {
        String msg = lindaBasic(cmd);
        int tuplecode = md5Dig.HashFunc(msg);
        int hostid = calhostID(tuplecode);
        int backid = getBackupID(hostid);
        boolean syncback = false;
        // send message
        if(!sendMessages(true, "[in][" + hostName + "]"  + msg, hostid)){
            syncback =  true; // read backup and sync the status
            System.out.println("Error: in command failed");
        }
        if(!sendMessages(syncback, "[backupin][" + hostMap.get(hostid) + "]"  + msg, backid)) {
            System.out.println("Error: [backup] in command failed");
        }
    }

    // Linda command: Rd function, read data
    private void lindaRdcommand(String cmd) {
        String msg = lindaBasic(cmd);
        int tuplecode = md5Dig.HashFunc(msg);
        int hostid = calhostID(tuplecode);
        int backid = getBackupID(hostid);
        // send message
        if (!sendMessages(true, "[rd][" + hostName + "]" + msg, hostid) ) {
            if (!sendMessages(true, "[backuprd][" + hostMap.get(hostid) + "]" + msg, backid) ) {
                System.out.println("Error: Rd command failed");
            }
        }
    }

    // Process command-line command from local host
    private void lindaOthercommand(String cmd) {
        String msg = cmd.replaceAll("\\s+", ""); //remove white space
        if (cmd.startsWith("out") && msg.length() > 5) { //assume at least out()
            char[] tmp=msg.toCharArray();
            if(tmp[3]!='('||tmp[tmp.length-1]!=')'){
                throw new IllegalArgumentException("invalid [out] input format");
            }else {
                msg = msg.substring(4, msg.length() - 1); //remove ( )
                lindaOutcommand(msg);
            }
        }
        if (cmd.startsWith("in") && msg.length() > 4) { //assume at least in()
            char[] tmp2=msg.toCharArray();
            if(tmp2[2]!='('||tmp2[tmp2.length-1]!=')'){
                throw new IllegalArgumentException("invalid [in] input format");
            }else {
                msg = msg.substring(3, msg.length() - 1); //remove ( )
                lindaIncommand(msg);
            }
        }
        if (cmd.startsWith("rd") && msg.length() > 4) { //assume at least rd()
            char[] tmp3=msg.toCharArray();
            if(tmp3[2]!='('||tmp3[tmp3.length-1]!=')'){
                throw new IllegalArgumentException("invalid [rd] input format");
            }else {
                msg = msg.substring(3, msg.length() - 1); //remove ( )
                lindaRdcommand(msg);
            }
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
        boolean isbackup = false;
        // out
        if (msg.startsWith("[out]") || msg.startsWith("[backupout]")) {
            if (msg.startsWith("[out]")) {
                msg = msg.substring(5, msg.length()); //remove [out]
            } else if (msg.startsWith("[backupout]")) {
                msg = msg.substring(11, msg.length()); //remove [backupout]
                isbackup = true;
            }
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            if (!clienthost.contains(",")) {
                clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
                if (clientInfo == null) {
                    System.out.println("Error: [out] can not find client information - " + clienthost);
                    return;
                }
            } else {
                String[] source = clienthost.split(",");
                clientInfo = new ServerInfo(source[0], source[1], Integer.valueOf(source[2]));
                if (clientInfo == null) {
                    System.out.println("Error: [backupout] can not find client information - " + clienthost);
                    return;
                }
            }
            tuplesInfo.writetuple(msg, loginName, hostName, isbackup, clientInfo.getName());
            //sendAndSync protocol: fail, success
            sendAndSync(false, "success", clientInfo.getIP(), clientInfo.getPORT());
            //message on target host
            if (!isbackup) {
                System.out.println("\n[Target] put tuple (" + msg + ") on " + hostIP);
            } else {
                System.out.println("\n[TargetBackup] put tuple (" + msg + ") on " + hostIP);
            }
            System.out.print("linda> ");
        } else if (cmd.startsWith("[rd]") || msg.startsWith("[backuprd]")) {
            if (msg.startsWith("[rd]")) {
                msg = msg.substring(4, msg.length()); //remove [rd]
            } else {
                msg = msg.substring(10, msg.length()); //remove [backuprd]
                isbackup = true;
            }
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [rd] can not find client information - " + clienthost);
                return;
            }
            String data = tuplesInfo.readtuple(msg, loginName, hostName, isbackup, clienthost);
            String status = data == null ? "fail" : "success" + data;
            sendAndSync(true, status, clientInfo.getIP(), clientInfo.getPORT());
            //System.out.println("\n[Receive]get tuple (" + msg + ") on " + clientInfo.getIP());
            //System.out.print("linda> ");
        } else if (msg.startsWith("[in]") || msg.startsWith("[backupin]")){
            if (msg.startsWith("[rd]")) {
                msg = msg.substring(4, msg.length()); //remove [in]
            } else {
                msg = msg.substring(10, msg.length()); //remove [backupin]
                isbackup = true;
            }
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [in] can not find client information - " + clienthost);
                return;
            }
            String data = tuplesInfo.readtuple(msg, loginName, hostName, isbackup, clienthost);
            if (data != null) {
                tuplesInfo.deletetuple(data, loginName, hostName, isbackup, clienthost);
            }
            String status = data == null ? "fail" : "success" + data;
            sendAndSync(true, status, clientInfo.getIP(), clientInfo.getPORT());
            //System.out.println("\n[Receive]get tuple (" + msg + ") on " + clientInfo.getIP());
            //System.out.print("linda> ");
        } else if (msg.startsWith("[add]")){
            msg = msg.substring(5, msg.length()); //remove[add]
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            String[] hostinfos = msg.split("\t");
            hostList.clear(); // remove original net information
            for (int i = 0; i < hostinfos.length; i++) {
                if (!hostinfos[i].equals("")) {
                    String[] hostinfo = hostinfos[i].split(",");
                    hostList.add(new ServerInfo(hostinfo[0], hostinfo[1], Integer.valueOf(hostinfo[2])));
                }
            }
            updateHostIdNameMap();
            srvInfo.writenets(hostList, loginName, hostName);
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            sendAndSync(false, "success", clientInfo.getIP(), clientInfo.getPORT());
        } else if (msg.startsWith("success") || msg.startsWith("fail")) {
            acknowledge = msg;
        } else if (msg.startsWith("[Disadd]")) {
            System.out.println("[Info] " + hostName + " is processing add distribution");
            DistributeTuple(false);
        } else if (msg.startsWith("[Disdel]")) {
            msg = msg.substring(8, msg.length()); //remove [Disdel]
            clienthost = gethostname(msg);
            msg = msg.substring(clienthost.length() + 2, msg.length()); //remove [hostname]
            String[] hostinfos = msg.split("\t");
            hostList.clear(); // remove original net information
            for (int i = 0; i < hostinfos.length; i++) {
                if (!hostinfos[i].equals("")) {
                    String[] hostinfo = hostinfos[i].split(",");
                    hostList.add(new ServerInfo(hostinfo[0], hostinfo[1], Integer.valueOf(hostinfo[2])));
                }
            }
            srvInfo.writenets(hostList, loginName, hostName);
            /*
            clientInfo = srvInfo.readnets(loginName, hostName, clienthost);
            if (clientInfo == null) {
                System.out.println("Error: [delete] can not find server information - " + clienthost);
                return;
            }
            */
            updateHostIdNameMap();
            DistributeTuple(false);
            //sendAndSync(false, "success", clientInfo.getIP(), clientInfo.getPORT());
            // clean everything
            cleanAll();
        } else if (msg.startsWith("[backuprm]")) {
            msg = msg.substring(10, msg.length()); //remove [backuprm]
            clienthost = gethostname(msg);
            System.out.println("[Info]: removing backup - " + clienthost);
            tuplesInfo.clean(loginName, hostName, true, clienthost);
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
