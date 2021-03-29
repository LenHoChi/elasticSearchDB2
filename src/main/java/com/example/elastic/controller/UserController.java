package com.example.elastic.controller;

import com.example.elastic.model.Users;
import com.example.elastic.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserController {
    @Autowired
    UserService userService;
    @GetMapping("/find-All-Users")
    public Iterable<Users> findAll(){
        return userService.findAllUsers();
    }
    @PostMapping("/save-user")
    public Users saveUser(@RequestBody Users users){
        return userService.saveUser(users);
    }
}
