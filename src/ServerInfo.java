/**
 * Created by Plutorian1 on 4/12/17.
 */
public class ServerInfo {
    private String hostName;
    private String IP;
    private int PORT;

    public ServerInfo(String name, String ip, int port) {
        hostName = name;
        IP = ip;
        PORT = port;
    }

    public void seName(String name) {
        hostName = name;
    }
    public String getName() {
        return hostName;
    }

    public void setIP(String ip) {
        IP = ip;
    }
    public String getIP(){
        return IP;
    }

    public void setPORT(int port){
        PORT = port;
    }
    public int getPORT(){
        return PORT;
    }
}
