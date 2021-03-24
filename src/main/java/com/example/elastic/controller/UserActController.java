package com.example.elastic.controller;

import com.example.elastic.model.UserActivity;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.service.PlayGround;
import com.example.elastic.service.UserActService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;
@RestController
public class UserActController {
    @Autowired
    PlayGround playground;
    @Autowired
    private UserActService userActService;
    @Autowired
    private UserActRepository userActRepository;
    @Autowired
    private UserActDBRepository userActDBRepository;
    @GetMapping("/find-all")
    public List<UserActivity> findAllUsers() {
        return userActService.findAll();
    }
    @GetMapping("/find-by-url")
    public List<UserActivity> findByUrl(@RequestBody String url) {
        return userActService.findByUrl(url);
    }
    @GetMapping("/group-by-field")
    public List<String> groupByField() throws IOException {
        return userActService.groupByField();
    }
    @GetMapping("find-by-field")
    public List<UserActivity> solve2(@RequestBody String message) {
        return userActService.findByField(userActService.splitHeadTail(message),"2021-03-19","2021-03-19","PC-LenHo");
    }
    @PostMapping("/saveAll")
    public boolean saveAll() {
        return userActService.saveAll();
    }
    @GetMapping("/len")
    public void callFunction(){
        playground.startCron();
    }
}
