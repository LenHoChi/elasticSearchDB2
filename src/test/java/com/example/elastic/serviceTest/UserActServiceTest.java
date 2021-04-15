package com.example.elastic.serviceTest;


import com.example.elastic.model.MyKey;
import com.example.elastic.model.UserActivity;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.model.Users;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.repository.UsersRepository;
import com.example.elastic.service.UserActService;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.jsmart.zerocode.core.domain.JsonTestCase;
import org.jsmart.zerocode.core.domain.LoadWith;
import org.jsmart.zerocode.core.domain.TargetEnv;
import org.jsmart.zerocode.core.domain.TestMapping;
import org.jsmart.zerocode.core.runner.ZeroCodeUnitRunner;
import org.jsmart.zerocode.core.runner.parallel.ZeroCodeLoadRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchHitsImpl;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import static org.assertj.core.api.Assertions.*;

import javax.persistence.Column;
import javax.persistence.Id;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
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
    private UsersRepository usersRepository;
    @MockBean
    private UserActDBRepository userActDBRepository;
   // private ElasticsearchOperations elasticsearchOperations2 = mock(ElasticsearchOperations.class);
    @Autowired
    private ElasticsearchOperations elasticsearchOperations;
//    @MockBean
//    private ElasticsearchOperations elasticsearchOperations2;

    @Test
    public void testFindAll(){
        UserActivity userActivityA = new UserActivity("1","www.youtube.com","2021-04-23","PC-LenHo");
        UserActivity userActivityB = new UserActivity("2","www.facebook.com","2021-05-23","PC-LenHo");

        List<UserActivity> lstUser = Arrays.asList(userActivityA, userActivityB);

        when(userActRepository.findAll()).thenReturn(lstUser);

//        List<UserActivity> lstUserTest = userActService.findAll();
//
//        assertEquals(lstUser.size(),lstUserTest.size());
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
        String timeResult="06:23:34Z";
        String result = userActService.getTimeFromEL(time);
        assertEquals(result,timeResult);
    }
    @Test
    public void testMilliTime(){
        String time = "06:23:34";
        float timeResult = (float) 23014.0;
        float result = userActService.getSecondFromTime(time);
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
        String url = "www.facebook.com";
        String pcName = "PC-LenHo0";
        String fromDate = "2021-04-02";
        String toDate = "2021-04-04";

        UserActivity userActivityA = new UserActivity("1","www.facebook.com","2021-04-03","PC-LenHo0");
        UserActivity userActivityB = new UserActivity("2","www.facebook.com","2021-04-03","PC-LenHo0");
        UserActivity userActivityC = new UserActivity("2","len","2021-04-03","PC-LenHo0");

        List<IndexQuery> indexQueries = new ArrayList<>();
        IndexQuery indexQuery1 = new IndexQuery();
        indexQuery1.setId("so1");
        indexQuery1.setObject(userActivityA);
        IndexQuery indexQuery2 = new IndexQuery();
        indexQuery2.setId("so2");
        indexQuery2.setObject(userActivityB);
        IndexQuery indexQuery3 = new IndexQuery();
        indexQuery3.setId("so3");
        indexQuery3.setObject(userActivityC);
        indexQueries.add(indexQuery1);
        indexQueries.add(indexQuery2);
        indexQueries.add(indexQuery3);
        IndexCoordinates index = IndexCoordinates.of("network_packet2");
        elasticsearchOperations.bulkIndex(indexQueries,index);
//        elasticsearchOperations.refresh(index);
//        elasticsearchOperations.indexOps(UserActivity.class).refresh();

//        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate")
//                .gte(fromDate)
//                .lte(toDate)).must(QueryBuilders.matchPhraseQuery("user_id",pcName)).must(QueryBuilders.matchPhraseQuery("url",url));

        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("user_id",pcName));//.must(QueryBuilders.matchPhraseQuery("url",url));

        Query searchQuery = new NativeSearchQueryBuilder()
                .withQuery(queryBuilder)
                .withSort(SortBuilders.fieldSort("localdate").order(SortOrder.ASC))
                .build();

        SearchHits<UserActivity> productHits = elasticsearchOperations.search(searchQuery, UserActivity.class,index);
        assertThat(productHits).isNotNull();
      //  productHits.forEach(node->node.getContent().setId("10"));
//        productHits.stream().map(userActivitySearchHit -> userActivitySearchHit.getContent().setId("1"));

    //    when(elasticsearchOperations.search(searchQuery,UserActivity.class,index)).thenReturn(productHits);

    //    List<UserActivity> result = userActService.findByField(url,fromDate,toDate,pcName);

//        Query searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("url","www.facebook.com"))).build();
//        SearchHits<UserActivity> userActivitySearchHits = elasticsearchOperations.search(searchQuery,UserActivity.class,index);
//        assertThat(userActivitySearchHits).hasSize(2);

   //     assertEquals(result.size(),5);
       // assertEquals(productHits.get().count(),10); //==2

      //  assertThat(productHits).isNotNull();
    }
    @Test
    public void testFindByField(){
        String url = "www.facebook.com";
        String pcName = "PC-LenHo";
        String fromDate = "2021-04-02";
        String toDate = "2021-04-05";
        List<UserActivity> result = userActService.findByField(url,fromDate,toDate,pcName);
        assertThat(result).isNotNull();
    }
    @Test
    public void testGroupByField() throws IOException {
        String url = "www.facebook.com";
        String pcName = "PC-LenHo0";
        String fromDate = "2021-04-02";
        String toDate = "2021-04-04";
        Terms term = userActService.groupByField(fromDate,toDate);
        assertThat(term).isNotNull();
    }
    @Test
    public void testFindByIDDB(){
        UserActivityDB userActivityDBA = new UserActivityDB("1","www.facebook.com",1,"2021-04-05",3.5f);
        when(userActDBRepository.findById(new MyKey("1","www.facebook.com","2021-04-05"))).thenReturn(Optional.of(userActivityDBA));
        Optional<UserActivityDB> userActivityDB = userActService.findUserByIDDB("1","www.facebook.com","2021-04-05");
        assertEquals(userActivityDB.get().getCount(),1);
    }
    @Test
    public void testCheckContain(){
        Boolean result = userActService.checkContain("CONNECT ");
        assertTrue(result);
    }
    @Test
    public void testCheckExists(){
        Users users = new Users("1","newmooncs2@gmail.com");
        when(usersRepository.findById("len")).thenReturn(Optional.of(users));
        Boolean result = userActService.checkExists("len");
        assertTrue(result);
    }
    @Test
    public void testCheckNotExists(){
        when(usersRepository.findById("len")).thenReturn(Optional.empty());
        Boolean result = userActService.checkExists("len");
        assertFalse(result);
    }
    @Test
    public void testProcessAddNotExists(){
        when(usersRepository.findById("len")).thenReturn(Optional.empty());
        Boolean result = userActService.processAdd("www.facebook.com",3f, "2021-04-04","PC-LenHo");
        assertFalse(result);
    }
    @Test
    public void testProcessAddExists(){
        Users users = new Users("1","newmooncsu@gmail.com");
        UserActivityDB userActivityDBA = new UserActivityDB("1","www.facebook.com",1,"2021-04-04",3.5f);

        when(userActDBRepository.findById(new MyKey("len","www.facebook.com","2021-04-04"))).thenReturn(Optional.of(userActivityDBA));
        when(usersRepository.findById("len")).thenReturn(Optional.of(users));
        Boolean result = userActService.processAdd("www.facebook.com",3f,"2021-04-04","len");
        assertTrue(result);
    }
}
