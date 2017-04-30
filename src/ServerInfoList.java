import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Plutorian1 on 4/12/17.
 */
public class ServerInfoList {

    public static void writenets(List<ServerInfo> sl, String login, String hostname) {
        BufferedWriter bw = null;
        String path = "/tmp/" + login + "/linda/" + hostname + "/nets";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P2\\"+ login +"\\linda\\" + hostname +"\\nets";
        File file = new File(path);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            try {
                file.createNewFile();
            } catch (IOException e) {
                System.out.println("Error: Can not create nets directory");
            }
        }
        // change mode
        try {
            Runtime.getRuntime().exec("chmod 777 " + "/tmp/" + login);
            Runtime.getRuntime().exec("chmod 777 " + "/tmp/" + login + "/linda");
            Runtime.getRuntime().exec("chmod 777 " + "/tmp/" + login + "/linda/" + hostname);
            Runtime.getRuntime().exec("chmod 666 " + path);
        } catch (IOException e){
            System.out.println("Error: can not change folder/files mode" + path);
        }
        // skip replicate net information
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));

        } catch (FileNotFoundException e) {
            System.out.println("Error: cannot read " + path);
        }
        try {
            //bw = new BufferedWriter(new FileWriter("/tmp/" + login + "/linda/" + hostname + "/nets"));
            bw = new BufferedWriter(new FileWriter(path, false));
            for (ServerInfo s : sl) {
                bw.write(s.getName()+","+ s.getIP() + "," + s.getPORT() + "\n");
                //System.out.println("write file");
            }
            bw.close();
        } catch (IOException e) {
            System.out.println("Error: Cannot write nets files");
        }
    }

    /*public static void pass_nets(String login,String cur_hostname,String nets_host){
        String path = "/tmp/" + login + "/linda/" + cur_hostname + "/nets";
        String npath="/tmp/" + login + "/linda/" + nets_host + "/nets";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + cur_hostname +"\\nets";
        //String npath = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + nets_host +"\\nets";
        File inputfile = new File(path);
        File passfile = new File(npath);

        if (!passfile.exists()) {
            passfile.getParentFile().mkdirs();
            try {
                passfile.createNewFile();
            } catch (IOException e) {
                System.out.println("Error: Cannot create nets directory");
            }
        }

        try{
            BufferedReader reader=new BufferedReader(new FileReader(inputfile));
            BufferedWriter writer=new BufferedWriter(new FileWriter(passfile,false));
            String line;
            while((line=reader.readLine())!=null){
                writer.write(line+"\n");
            }
            writer.close();


        }catch (IOException E){
            System.out.println("Fail TO PASS NETS FILE");
        }
    }*/

    // get specific host information by search target host name
    public ServerInfo readnets(String login, String hostname, String targethost) {
        String path = "/tmp/" + login + "/linda/" + hostname + "/nets";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P2\\"+ login +"\\linda\\" + hostname +"\\nets";
        try {
            FileReader fReader = new FileReader(path);
            BufferedReader bReader = new BufferedReader(fReader);
            String line = "";
            while ((line = bReader.readLine()) != null) {
                if (line.contains(targethost)) {
                    String[] s = line.split(",");
                    return new ServerInfo(s[0], s[1], Integer.valueOf(s[2]));
                }
            }
        } catch (IOException e) {
            System.out.println("Error: cannot open file: " + path);
        }
        System.out.println("Error: cannot find " + targethost + " on " + path);
        return null;
    }

    /**read all host info on nets file**/
    public List<ServerInfo> loadnets(String login,String hostname) {
        String path = "/tmp/" + login + "/linda/" + hostname + "/nets";
        try {
            //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P2\\"+ login +"\\linda\\" + hostname +"\\nets";
            BufferedReader inputfile = new BufferedReader(new FileReader(path));
            String input = "";
            List<ServerInfo> sl = new ArrayList<>();
            while ((input = inputfile.readLine()) != null) {
                String[] s = input.split(",");
                ServerInfo server = new ServerInfo(s[0], s[1], Integer.valueOf(s[2]));
                sl.add(server);
            }
            return sl;
        } catch (IOException e) {
            System.out.println("Error: cannot open " + path);
        }
        return null;
    }

    /**convert nets file into a string wefw**/
    public static String GetNetsContent(String login, String hostname) {
        try {
            String path = "/tmp/" + login + "/linda/" + hostname + "/nets";
            //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P2\\"+ login +"\\linda\\" + hostname +"\\nets";
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // clean all information
    public void clean(String login, String hostname) {
        String path = "/tmp/" + login + "/linda/" + hostname + "/nets";
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }
}
