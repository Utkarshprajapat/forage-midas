package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.junit.jupiter.api.Test;

import java.util.List;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class WilburBalanceInspector {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void inspectWilburBalance() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(3000);

        List<UserRecord> users = userRepository.findAll();
        for (UserRecord user : users) {
            if ("wilbur".equals(user.getName())) {
                float balance = user.getBalance();
                int roundedBalance = (int) Math.floor(balance);
                System.out.println("WILBUR BALANCE: " + balance);
                System.out.println("ROUNDED DOWN: " + roundedBalance);
                System.out.println("ANSWER: " + roundedBalance);
                return;
            }
        }
        System.out.println("Wilbur not found!");
    }
}