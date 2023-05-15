import CommonUtils.KafkaClientUtil;

import java.util.Random;

public class RandomStringGenerator {
    private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final Random random = new Random();

    public static String generate(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(CHARACTERS.length());
            char randomChar = CHARACTERS.charAt(randomIndex);
            sb.append(randomChar);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true){
            KafkaClientUtil.sendDataToKafka("test230514",generate(10));
            KafkaClientUtil.flush();
            Thread.sleep(1000);
        }
    }
}
