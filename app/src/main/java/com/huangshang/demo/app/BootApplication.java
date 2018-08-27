package com.huangshang.demo.app;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Created by huangshang on 2018/8/25 下午3:53.
 * Description: ***
 *
 * @author <a href="mailto:chenjie@cai-inc.com"/>
 */
@SpringBootApplication
@ComponentScan(basePackages = {
        "com.huangshang.demo"
})
@EnableTransactionManagement
@MapperScan(basePackages = "com.huangshang.demo.center.dao")
public class BootApplication {
    public static void main (String[] args){
        SpringApplication application = new SpringApplication(BootApplication.class);
        application.run(args);
    }
}
