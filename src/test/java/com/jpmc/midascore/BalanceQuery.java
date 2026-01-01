package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BalanceQuery {
    
    @Autowired
    private UserRepository userRepository;
    
    public int getWaldorfBalance() {
        List<UserRecord> users = userRepository.findAll();
        for (UserRecord user : users) {
            if ("waldorf".equals(user.getName())) {
                return (int) Math.floor(user.getBalance());
            }
        }
        return 0;
    }
}


