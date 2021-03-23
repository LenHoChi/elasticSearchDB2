package com.example.elastic.serviceTest;


import com.example.elastic.model.MyKey;
import com.example.elastic.model.UserActivity;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.service.UserActService;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import javax.persistence.Column;
import javax.persistence.Id;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
@SpringBootTest
@RunWith(SpringRunner.class)
//@RunWith(SpringJUnit4ClassRunner.class)
@AutoConfigureMockMvc(addFilters = false)
@WebAppConfiguration
public class UserActServiceTest {
    @Autowired
    private UserActService userActService;

    @MockBean
    private UserActRepository userActRepository;
    @MockBean
    private UserActDBRepository userActDBRepository;
    @MockBean
    private ElasticsearchOperations elasticsearchOperations;
    @Test
    public void testFindAll(){
        UserActivity userActivityA = new UserActivity("1","www.youtube.com","2021-04-23","PC-LenHo");
        UserActivity userActivityB = new UserActivity("2","www.facebook.com","2021-05-23","PC-LenHo");

        List<UserActivity> lstUser = Arrays.asList(userActivityA, userActivityB);

        when(userActRepository.findAll()).thenReturn(lstUser);

        List<UserActivity> lstUserTest = userActService.findAll();

        assertEquals(lstUser.size(),lstUserTest.size());
    }
    @Test
    public void testFindByUrl(){
        UserActivity userActivityA = new UserActivity("1","www.youtube.com","2021-04-23","PC-LenHo");
        UserActivity userActivityB = new UserActivity("2","www.facebook.com","2021-05-23","PC-LenHo");

        List<UserActivity> lstUser = Arrays.asList(userActivityA, userActivityB);
        String message = "www.vnexpress.net";
        when(userActRepository.findByUrl(message)).thenReturn(lstUser);
        List<UserActivity> lstUserTest = userActService.findByUrl(message);
        assertEquals(lstUser.size(),lstUserTest.size());
    }
    @Test
    public void testSaveAll(){
        UserActivityDB userActivityDB = new UserActivityDB("PC-LenHo","www.youtube.com",2,"2021-02-12",12);
        List<UserActivityDB> lstUser = Arrays.asList(userActivityDB);

        when(userActDBRepository.saveAll(lstUser)).thenReturn(lstUser);
        Boolean result = userActService.saveAll();
        assertTrue(result);
    }
    @Test
    public void testGetDateFromEL(){
        String time = "2021-12-21T06:23:34";
        String timeResult="2021-12-21";
        String result = userActService.getDateFromEL(time);
        assertEquals(result,timeResult);
    }
    @Test
    public void testTimeFromEL(){
        String time = "2021-12-21T06:23:34Z";
        String timeResult="06:23:34";
        String result = userActService.getTimeFromEL(time);
        assertEquals(result,timeResult);
    }
    @Test
    public void testMilliTime(){
        String time = "06:23:34";
        float timeResult = (float) 23014.0;
        float result = userActService.getMilliTime(time);
        assertEquals(result,timeResult);
    }
    @Test
    public void testSplitHeadTail(){
        String url = "CONNECT www.youtube.com:443 HTTP/1.1";
        String urlResult="www.youtube.com";
        String result = userActService.splitHeadTail(url);
        assertEquals(result,urlResult);
    }
    @Test
    public void testCount(){
        String url = "CONNECT www.youtube.com:443 HTTP/1.1";
        String pcName = "PC-LenHo";
        String date = "2021-12-21T06:23:34Z";
        Optional<UserActivityDB> user = Optional.of(new UserActivityDB(pcName,url,2,date,12));

        when(userActDBRepository.findById(new MyKey(pcName,url,date))).thenReturn(user);
        int count = userActService.getCount(pcName,url,date);
        assertEquals(count,user.get().getCount());
    }
    @Test
    public void testCountFail(){
        String url = "CONNECT www.youtube.com:443 HTTP/1.1";
        String pcName = "PC-LenHo";
        String date = "2021-12-21T06:23:34Z";
        Optional<UserActivityDB> user = Optional.of(new UserActivityDB(pcName,url,2,date,12));

        when(userActDBRepository.findById(new MyKey(pcName,"url",date))).thenReturn(user);
        int count = userActService.getCount(pcName,url,date);
        assertEquals(count,0);
    }
    @Test
    public void testTotalTime(){
        String url = "CONNECT www.youtube.com:443 HTTP/1.1";
        String pcName = "PC-LenHo";
        String date = "2021-12-21T06:23:34Z";
        Optional<UserActivityDB> user = Optional.of(new UserActivityDB(pcName,url,2,date,12));

        when(userActDBRepository.findById(new MyKey(pcName,url,date))).thenReturn(user);
        float totalTime = userActService.getTotalTime(pcName,url,date);
        assertEquals(totalTime,user.get().getTotal_time());
    }
    @Test
    public void testTotalTimeFail(){
        String url = "CONNECT www.youtube.com:443 HTTP/1.1";
        String pcName = "PC-LenHo";
        String date = "2021-12-21T06:23:34Z";
        Optional<UserActivityDB> user = Optional.of(new UserActivityDB(pcName,url,2,date,12));

        when(userActDBRepository.findById(new MyKey(pcName,url,date))).thenReturn(user);
        float totalTime = userActService.getTotalTime(pcName,"url",date);
        assertEquals(totalTime,0);
    }
    @Test
    public void findByField(){
        String url = "CONNECT www.youtube.com:443 HTTP/1.1";
        String pcName = "PC-LenHo";
        String fromDate = "2021-12-21T06";
        String toDate = "2021-12-21T07";

        UserActivity userActivityA = new UserActivity("1","www.youtube.com","2021-04-23","PC-LenHo");
        UserActivity userActivityB = new UserActivity("2","www.facebook.com","2021-05-23","PC-LenHo");

        List<UserActivity> lstUser = Arrays.asList(userActivityA, userActivityB);

        Query query = new NativeSearchQueryBuilder().build();
        SearchHits<UserActivity> productHits = null;
        //productHits.forEach();

        when(elasticsearchOperations.search(query,UserActivity.class, IndexCoordinates.of("index"))).thenReturn(productHits);

    }
}
