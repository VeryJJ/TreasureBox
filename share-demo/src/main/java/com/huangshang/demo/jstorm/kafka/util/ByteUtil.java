package com.huangshang.demo.jstorm.kafka.util;

import java.io.UnsupportedEncodingException;

/**
 * Created by huangshang on 2018/9/1 上午5:00.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
public class ByteUtil {
    public static String getStringFromByteArray(byte[] bytes) throws UnsupportedEncodingException {
        return new String(bytes, "UTF-8");
    }
}
