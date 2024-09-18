package com.softenggourav.service;

import com.softenggourav.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessageToTopic(String message){
        // CompletableFuture<SendResult<String, Object>> future = template.send("softenggourav-topic1", 2,null, message);

        // future.whenComplete((result,ex)->{
        //     if (ex == null) {
        //         System.out.println("Sent message=[" + message +
        //                 "] with offset=[" + result.getRecordMetadata().offset() + "]");
        //     } else {
        //         System.out.println("Unable to send message=[" +
        //                 message + "] due to : " + ex.getMessage());
        //     }
        // });
        template.send("softenggourav-topic1", 2,null, "welcome");
        template.send("softenggourav-topic1", 1,null, "read1");
        template.send("softenggourav-topic1", 2,null, "to");
        template.send("softenggourav-topic1", 3,null, "read3");
        template.send("softenggourav-topic1", 2,null, "family");

    }

    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("softenggourav-demo", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer.toString() +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            customer.toString() + "] due to : " + ex.getMessage());
                }
            });

        } catch (Exception ex) {
            System.out.println("ERROR : "+ ex.getMessage());
        }
    }
}
