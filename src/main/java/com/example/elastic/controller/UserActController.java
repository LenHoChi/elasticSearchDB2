package com.example.elastic.controller;

import com.easyquartz.scheduler.ScheduleService;
import com.example.elastic.ElasticApplication;
import com.example.elastic.model.UserActivity;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.model.Users;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.repository.UsersRepository;
import com.example.elastic.service.UserActService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
@RestController
public class UserActController {

    @Autowired
    public UsersRepository usersRepository;
    @Autowired
    private UserActService userActService;
    @Autowired
    private UserActRepository userActRepository;
    @Autowired
    private UserActDBRepository userActDBRepository;
    @GetMapping("/find-all")
    public Iterable<UserActivity> findAllUsers() {
        return userActService.findAll();
    }
    @GetMapping("/find-all-db")
    public List<UserActivityDB> findAllUsersDB() {
        return userActDBRepository.findAll();
    }
    @GetMapping("/find-by-url")
    public List<UserActivity> findByUrl(@RequestBody String url) {
        return userActService.findByUrl(url);
    }
    @GetMapping("find-by-field")
    public List<UserActivity> solve2(@RequestBody String message) throws IOException {
        return userActService.findByField(userActService.splitHeadTail(message),"2021-03-30","2021-03-31","PC-QUANPHAM$");
    }
    @PostMapping("/saveAll")
    public boolean saveAll() {
        return userActService.saveAll();
    }
    @PostMapping("/pull-into-db")
    public boolean pullDataIntoDB2(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessing2(time);
    }
    @PostMapping("/pull-into-db2")
    public boolean pullDataIntoDB(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessing2(time);
    }
    @PostMapping("/pull-into-db4")
    public boolean pullDataIntoDB4(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessing(time);
    }
    @PostMapping("/pull-into-db5")
    public boolean pullDataIntoDB5(@RequestBody String time) throws IOException, ParseException {
        return userActService.mainProcessingTerm(time);
    }
    @GetMapping("/test")
    public void test(){
        userActService.saveST();
    }
    @GetMapping("/test2")
    public List<UserActivity> test2() throws IOException {
        return userActService.findByFieldCroll("www.facebook.com","2021-04-12","2021-04-12","PC-LenHo$");
    }
    @GetMapping("/test3")
    public void test3(){
        final long start = System.currentTimeMillis();
        List<Users> lstUsers = new ArrayList<>();
        for(int i=0;i<10000;i++){
            Users users = new Users("name"+i,"email");
            lstUsers.add(users);
        }
        usersRepository.saveAll(lstUsers);
        //---------------
//		for(int i=0;i<10000;i++){
//			Users users = new Users("name"+i,"email");
//			usersRepository.save(users);
//		}
        final  long end = System.currentTimeMillis();
        System.out.println(end-start);
    }
}
