package com.gl.reader;

import com.gl.reader.service.ProcessController;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;


@EnableAsync
@SpringBootConfiguration
@SpringBootApplication(scanBasePackages = {"com.gl.reader"})
@EnableEncryptableProperties
public class EdrP1P2Process {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(EdrP1P2Process.class, args);
        ProcessController mainController = (ProcessController) context.getBean("processController");
        mainController.startApplication(context, args);
    }

}
