import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Plutorian1 on 4/12/17.
 */
public class Tuples {

    // TODO: dont overwrite context, should add on
    public void writetuple(String tuple, String login, String hostname) {
        BufferedWriter bw = null;
        String path = "/tmp/" + login + "/linda/" + hostname + "/tuples";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + hostname +"\\tuples";

        File file = new File(path);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            try {
                file.createNewFile();
            } catch (IOException e) {
                System.out.println("Error: Cannot create directory");
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
        try {
            //bw = new BufferedWriter(new FileWriter("/tmp/" + login + "/linda/" + hostname + "/nets"));
            bw = new BufferedWriter(new FileWriter(path, true));

            bw.write(tuple+ "\n");
            //System.out.println("write file");

            bw.close();
        } catch (IOException e) {
            System.out.println("Error: Cannot write tuples files");
        }

        /*String path = "/tmp/" + login + "/linda/" + hostname + "/tuples";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + hostname +"\\tuples";
        FileWriter fw = null;
        File filename = new File(path);
        try{
            fw = new FileWriter(filename,true);
            fw.write(tuple);
            fw.flush();
        } catch (Exception e){
            System.out.println("Error: cannot write to tuple file");
        }*/
    }

    // check data type: String, Integer or Float
    private boolean checkType(String in, String type) {
        // check String first
        if (in.contains("\"") && type.equals("string")) {
            return true;
        } else if (in.contains(".") && type.equals("float")) {
            return true;
        } else if (type.equals("int")){
            for (int i = 0; i < in.length(); i++) {
                if(!Character.isDigit(in.charAt(i))) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    private boolean ismatchtype(String cmp, String gold) {
        if (cmp.contains("int") && checkType(gold, "int")) {
            return true;
        }
        if (cmp.contains("float") && checkType(gold, "float")) {
            return true;
        }
        if (cmp.contains("string") && checkType(gold, "string")) {
            return true;
        }
        return false;
    }

    private boolean isvarmatch(String targetdata, String line) {
        String[] tmp = targetdata.split(",");
        String[] gold = line.split(",");
        // check number of input date equal to number of stored data
        if (tmp.length != gold.length) {
            return false;
        }
        // check match or not
        for (int i = 0; i < tmp.length; i++) {
            if (tmp[i].contains("?") && tmp[i].contains(":")) {
                if (!ismatchtype(tmp[i].toLowerCase(), gold[i])) {
                    return false;
                }
            } else if (!gold[i].equals(tmp[i])) {
                return false;
            }
        }

        return true;
    }

    // get specific host information by search target data: only for "rd" command
    public String readtuple(String targetdata, String login, String hostname) {
        String path = "/tmp/" + login + "/linda/" + hostname + "/tuples";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + hostname +"\\tuples";
        boolean bvarmatch = (targetdata.contains("?") && targetdata.contains(":")) ? true : false;

        // check file exist or not
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }
        // check tuple content
        try {
            FileReader fReader = new FileReader(path);
            BufferedReader bReader = new BufferedReader(fReader);
            String line = "";
            while ((line = bReader.readLine()) != null) {
                if (bvarmatch) {
                    if (isvarmatch(targetdata, line)) {
                        return line;
                    }
                } else if (line.equals(targetdata)) {
                    //exactly match
                    return line;
                }
            }
        } catch (IOException e) {
            System.out.println("Error: cannot open file: " + path);
        }
        return null;
    }

    public void deletetuple(String targetdata, String login, String hostname){
        String path = "/tmp/" + login + "/linda/" + hostname + "/tuples";
        String tmppath = "/tmp/" + login + "/linda/" + hostname + "/tmptuples";
        //String path = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + hostname +"\\tuples";
        //String tmppath = "C:\\Users\\user\\CloudComputing\\Cloud_Computing_P1\\"+ login +"\\linda\\" + hostname +"\\tmptuples";
        File inputfile = new File(path);
        File tmpfile = new File(tmppath);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputfile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tmpfile));
            Runtime.getRuntime().exec("chmod 666 " + path);
            Runtime.getRuntime().exec("chmod 666 " + tmppath);

            String lineToRemove = targetdata;
            String currentLine;
            int cnt=0;

            while ((currentLine=reader.readLine())!=null){
                if(currentLine.equals(lineToRemove)&&cnt==0){
                    cnt++;
                    continue;
                }else{
                    writer.write(currentLine+"\n");
                }
            }
            writer.close();
            reader.close();
            if(!tmpfile.renameTo(inputfile)){
                System.out.println("Error. unable to rewrite the deleted TS");
            }
        }catch(IOException e){
            System.out.println("Error cannot delete tuple in file: "+path);
        }
    }

    public static List<String> GetTuplesContent(String login, String hostname) {

        String path = "/tmp/" + login + "/linda/" + hostname + "/tuples";
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }
        // check tuple content
        try {
            FileReader fReader = new FileReader(path);
            BufferedReader bReader = new BufferedReader(fReader);
            String line = "";
            List<String> tp=new ArrayList<>();
            while ((line = bReader.readLine()) != null) {
               tp.add(line);
            }
        } catch (IOException e) {
            System.out.println("Error: cannot open file: " + path);
        }
        return null;
    }

    public List<String> readtuple(String login,String hostname) throws IOException{
        BufferedReader br=new BufferedReader(new FileReader("/tmp/" + login + "/linda/" + hostname + "/tuples"));
        List<String> tuples=new ArrayList<>();
        String line;
        while((line=br.readLine())!=null){
            tuples.add(line);
        }
        return tuples;
    }
}
