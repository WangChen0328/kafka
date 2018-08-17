package cn.web;

import cn.kafka.Consumer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wangchen
 * @date 2018/8/15 13:22
 */
@RestController
public class ConsumerController {

    @RequestMapping("/consumer")
    public void main() {
        Consumer consumer = new Consumer();
        consumer.main();
    }
}
