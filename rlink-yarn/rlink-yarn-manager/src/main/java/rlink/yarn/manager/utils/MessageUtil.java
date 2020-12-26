package rlink.yarn.manager.utils;

import com.alibaba.fastjson.JSON;
import rlink.yarn.manager.model.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageUtil.class);

    // message format: /*Rust*/ message
    private static final String MESSAGE_FORMAT = "/*Rust*/ %s";

    public static void send(Command command) {
        String msg = String.format(MESSAGE_FORMAT, JSON.toJSONString(command));
        LOGGER.info(msg);
    }
}
