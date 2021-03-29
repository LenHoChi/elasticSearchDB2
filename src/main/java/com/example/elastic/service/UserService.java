package com.example.elastic.service;

import com.example.elastic.model.Users;
import com.example.elastic.repository.UsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserService {
    @Autowired
    UsersRepository usersRepository;
    public Users saveUser(Users users){
        return usersRepository.save(users);
    }
    public Iterable<Users> findAllUsers(){
        return usersRepository.findAll();
    }
}
