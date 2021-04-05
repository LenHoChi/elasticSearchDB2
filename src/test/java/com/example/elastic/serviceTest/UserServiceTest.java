package com.example.elastic.serviceTest;

import com.example.elastic.model.Users;
import com.example.elastic.repository.UsersRepository;
import com.example.elastic.service.UserService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.Arrays;
import java.util.List;

@SpringBootTest
@RunWith(SpringRunner.class)
//@RunWith(SpringJUnit4ClassRunner.class)
@AutoConfigureMockMvc(addFilters = false)
@WebAppConfiguration
public class UserServiceTest {
    @MockBean
    private UsersRepository usersRepository;
    @Autowired
    private UserService userService;
    @Test
    public void testFindAllUsers(){
        Users users1 = new Users("1","newmooncsu@gmail.com");
        Users users2 = new Users("2","newmoon@gmail.com");
        List<Users> lstUsers = Arrays.asList(users1,users2);
        Mockito.when(usersRepository.findAll()).thenReturn(lstUsers);
        List<Users> result = (List<Users>) userService.findAllUsers();
        Assert.assertEquals(lstUsers.size(),result.size());
    }
    @Test
    public void testSaveUser(){
        Users users1 = new Users("1","newmooncsu@gmail.com");
        Mockito.when(usersRepository.save(users1)).thenReturn(users1);
        Users users2 = userService.saveUser(users1);
        Assert.assertEquals(users1.getUser_id(),users2.getUser_id());
    }
}
