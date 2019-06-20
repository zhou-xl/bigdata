package util;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

public class PhoneNoGeneratorUtil {

    private static String firstNum = "1";
    private static String[] secondNumArray = {"3", "4", "5", "7", "8"};

    /**
     * 调用一次生成一个手机号，手机号后9位数字通过循环生成
     */
    public String getPhoneNo() {

        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        String secondNum = secondNumArray[random.nextInt(secondNumArray.length)];
        sb.append(firstNum);
        sb.append(secondNum);
        for (int i = 0; i < 9; i++) {
            Integer thirdNum = random.nextInt(10);
            sb.append(thirdNum.toString());
        }
        return sb.toString();
    }

    /**
     * 当需要生成较大量的手机号时调用该方法
     * 后9位数字随机生成，长度不足时则补0
     */
    public String getMultiPhoneNo() {

        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        String secondNum = secondNumArray[random.nextInt(secondNumArray.length)];
        sb.append(firstNum);
        sb.append(secondNum);
        Integer thirdNum = 1 + random.nextInt(999999999);
        if (thirdNum.toString().length() <= 9) {
            sb.append(thirdNum);
            for (int i = 1; i <= 9 - thirdNum.toString().length(); i++) {
                sb.append(0);
            }
        } else {
            sb.append(thirdNum.toString());
        }
        return sb.toString();

    }

    /**
     * 手机号校验
     */
    public boolean checkPhoneNo(String phoneNum) {

        // 定义手机号的规则
        String phoneNumPattern = "^1[34578][0-9]{9}";
        // 比对phoneNum是否符合定义的规则
        boolean result = Pattern.matches(phoneNumPattern, phoneNum);
        return result;

    }

    @Test
    public void test() throws IOException {

        String phoneNo = "";

        StringBuffer sb = new StringBuffer();

        int i = 1;
        while (i < 10000) {
            phoneNo = getMultiPhoneNo();

            if (checkPhoneNo(phoneNo)) {
                sb.append(phoneNo);
                sb.append(" ");
                if (i % 10 == 0) {
                    sb.append("\n");
                }
                i++;
            }

        }

        FileOutputStream outputStream = new FileOutputStream(new File("C:\\Users\\zhouxl\\Downloads\\temp\\phone.txt"));
        outputStream.write(sb.toString().getBytes());
        outputStream.close();
    }


}
