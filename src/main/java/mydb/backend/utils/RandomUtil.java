package mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {
    public static byte[] randomBytes(int length){
        Random r=new SecureRandom();
        byte[] buff=new byte[length];
        r.nextBytes(buff);
        return buff;
    }
}
