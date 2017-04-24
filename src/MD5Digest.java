import java.security.MessageDigest;

/**
 * Created by Plutorian1 on 4/11/17.
 */
public class MD5Digest {
    public int HashFunc(String orig) {
        try {
            double pow=Math.pow(2,16);

            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(orig.getBytes());
            byte[] digest = md.digest();
            StringBuffer sb = new StringBuffer();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            String sb2 = "";
            sb2 = sb.toString();
            //System.out.println(sb2);
            Integer tmp;
            tmp = hex2Decimal(sb2);
            tmp = Math.abs(tmp % (int)pow);
            return tmp;
        } catch (Exception e) {
            System.out.print("Error: Cannot hash to host ");
        }
        return -1;
    }

    public int hex2Decimal(String s){
        String digits = "0123456789ABCDEF";
        s = s.toUpperCase();
        int val = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            int d = digits.indexOf(c);
            val = 16*val + d;
        }
        return val;
    }
}