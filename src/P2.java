/**
 * Created by Plutorian1 on 4/6/17.
 */
public class P2 {
    public static void main(String[] args) {
        String login = "czhang3";
        Server srv = new Server();
        try {
            if (args.length == 0) {
                System.out.println("Error: please add argument like host<id>");
                System.exit(0);
            }
            srv.start(args[0], login);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
