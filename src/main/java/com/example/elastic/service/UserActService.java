package com.example.elastic.service;

import com.example.elastic.convert.ToDB;
import com.example.elastic.model.MyKey;
import com.example.elastic.model.UserActivity;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;
@Transactional
@Service
public class UserActService {
    private final ElasticsearchOperations elasticsearchOperations;
    private final String index="test3";
    @Autowired
    UserActRepository userActRepository;
    @Autowired
    UserActDBRepository userActDBRepository;
    @Autowired
    public UserActService(final ElasticsearchOperations elasticsearchOperations) {
        super();
        this.elasticsearchOperations = elasticsearchOperations;
    }
    public List<UserActivity> findByField(final String message,String fromDate,String toDate,String pcName) {
        //QueryBuilder queryBuilderURL = boolQuery().must(matchPhraseQuery("user_id",pcName)).must(QueryBuilders.matchPhraseQuery("url",message));
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("@timestamp")
                                                                    .gte(fromDate)
                                                                    .lte(toDate)).must(QueryBuilders.matchPhraseQuery("user_id",pcName)).must(QueryBuilders.matchPhraseQuery("url",message));
        Query searchQuery = new NativeSearchQueryBuilder()
                //.withFilter(QueryBuilders.termQuery("@timestamp",date))
                //.withFilter(QueryBuilders.termQuery("url",message))
                //.withQuery(matchPhraseQuery("user_id",pcName))
                .withQuery(queryBuilder)
                .withSort(SortBuilders.fieldSort("@timestamp").order(SortOrder.ASC))
                .build();
        SearchHits<UserActivity> productHits =
                elasticsearchOperations
                        .search(searchQuery, UserActivity.class,
                                IndexCoordinates.of(index));
        List<UserActivity> productMatches = new ArrayList<>();
        productHits.forEach(srchHit-> productMatches.add(srchHit.getContent()));
        return productMatches;
    }
    public List<String> groupByField() throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(new TimeValue(15, TimeUnit.SECONDS));
        searchBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchBuilder.fetchSource(false);

        TermsAggregationBuilder aggregation1 = AggregationBuilders
                .terms("url")
                .field("url"+".keyword").size(1000);

        // @formatter:off
        TermsAggregationBuilder aggregation = AggregationBuilders
                .terms("url")
                .field("user_id" + ".keyword")
                .size(1000)
                .subAggregation(aggregation1)
                ;
        ClientConfiguration clientConfiguration =
                ClientConfiguration.builder().connectedTo("localhost:9200").build();
        RestHighLevelClient client = RestClients.create(clientConfiguration).rest();

        searchBuilder.aggregation(aggregation);
        SearchRequest searchRequest = Requests.searchRequest(index).allowPartialSearchResults(true)
                .source(searchBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Terms groupedProperties = searchResponse.getAggregations().get("url");
        // For each entry
        List<String> lst = new ArrayList<>();
        for (Terms.Bucket entry : groupedProperties.getBuckets()) {
            entry.getKey();// Term
            entry.getDocCount(); // Doc count
            String x = (String) entry.getKey();
            //x+="PC ";
            String y = "PC "+x;
            Terms bucket = entry.getAggregations().get("url");

            lst.add(y);

            bucket.getBuckets().stream().forEach(bucket1 -> {
                lst.add(bucket1.getKey().toString());
            });
        }
        return  lst;
    }
    //3mi=10800
    //10mi=36000get
    public boolean mainProcessing(String fromDate,String toDate) throws IOException {
        try {
            String dateDB = "";
            String pcName="";
            List<String> lstRoot = groupByField();
            for (int j = 0; j < lstRoot.size(); j++) {
                if(lstRoot.get(j).contains("PC")) {
                    String[]arr = lstRoot.get(j).split(" ");
                    pcName = arr[1];
                }
                if (lstRoot.get(j).contains("CONNECT")) {
                    List<UserActivity> lstUser = findByField(splitHeadTail(lstRoot.get(j)), fromDate,toDate,pcName);
                    float totalTime = 0;
                    int flag = 0;
                    try {
                        dateDB = getDateFromEL(lstUser.get(0).getTime());
                        //pcName = lstUser.get(0).getUser_id();
                    } catch (Exception e) {
                    }
                    for (int i = 0; i < lstUser.size() - 1; i++) {
                        flag = 0;
                        String timeRootF = getTimeFromEL(lstUser.get(i).getTime());
                        float milliTimeF = getMilliTime(timeRootF);
                        String timeRootS = getTimeFromEL(lstUser.get(i + 1).getTime());
                        float milliTimeS = getMilliTime(timeRootS);
                        float timeUsed = milliTimeS - milliTimeF;
                        //time between two surf bigger than 3 minutes--->solve // break time =3m
                        if (timeUsed >= 180) {
                            if(totalTime==0)
                                totalTime=180;
                            processAdd(splitHeadTail(lstUser.get(i).getUrl()), totalTime, dateDB, pcName);
                            totalTime = 0;
                            flag = 1;
                        } else
                            totalTime += timeUsed;
                        if (totalTime >= 600) {
                            processAdd(splitHeadTail(lstUser.get(i).getUrl()), totalTime, dateDB, pcName);
                            totalTime = 0;
                            flag = 1;
                        }
                        if(i==lstUser.size()-2){
                            if(timeUsed>180)
                                processAdd(splitHeadTail(lstUser.get(i).getUrl()), 180, dateDB, pcName);
                            else
                                processAdd(splitHeadTail(lstUser.get(i).getUrl()), timeUsed, dateDB, pcName);
                            flag=1;
                        }
                    }
                    if (flag == 0) {
                        try {
                            if(lstUser.size()==1)
                                totalTime=180;
                            processAdd(splitHeadTail(lstUser.get(0).getUrl()), totalTime, dateDB, pcName);
                        } catch (Exception e) {
                        }
                    }
                }
            }
        }catch (Exception e){
            return false;
        }
        return true;
    }
    public void processAdd(String message,float totalTime,String date, String pcName){
        int count=getCount(pcName,message,date);
        float total = getTotalTime(pcName,message,date);
        total = total*60;
        totalTime+=total;
        UserActivityDB userActivityDB = new UserActivityDB(pcName,message,++count,date,(totalTime/60));
        userActDBRepository.save(userActivityDB);
    }
    public float getTotalTime(String pcName, String url, String date){
        Optional<UserActivityDB> lstUser = userActDBRepository.findById(new MyKey(pcName,url,date));
        List<UserActivityDB> lstUserR = lstUser.stream().collect(Collectors.toList());
        for(int i=0;i<lstUser.stream().count();i++){
            if(lstUserR.get(i).getUrl().equals(url)&&lstUserR.get(i).getTime().equals(date)){
                return lstUserR.get(i).getTotal_time();
            }
        }
        return 0;
    }
    public int getCount(String pcName, String url, String date){
        Optional<UserActivityDB> lstUser = userActDBRepository.findById(new MyKey(pcName,url,date));
        List<UserActivityDB> lstUserR = lstUser.stream().collect(Collectors.toList());
        for(int i=0;i<lstUser.stream().count();i++){
            if(lstUserR.get(i).getUrl().equals(url)&&lstUserR.get(i).getTime().equals(date)){
                return lstUserR.get(i).getCount();
            }
        }
        return 0;
    }
    public String splitHeadTail(String x){
        String[] arr = x.split(" ");
        String[] arrX = arr[1].split(":");
        return arrX[0];
    }
    public float getMilliTime(String time) {
        float total;
        String[] arr = time.split(":");
        int hour = Integer.parseInt(arr[0]);
        int minute = Integer.parseInt(arr[1]);
        float second = Float.parseFloat(arr[2]);
        total = hour * 60 * 60 + minute * 60 + second;
        return total;
    }
    public String getTimeFromEL(String time) {
        String[] arr = time.split("T");
        String firstTime = arr[1];
        String[] arr2 = firstTime.split("Z");
        return arr2[0];
    }
    public String getDateFromEL(String time) {
        String[] arr = time.split("T");
        return arr[0];
    }
    public boolean saveAll(){
        ToDB infoToDB = new ToDB();
        Iterable<UserActivity> lstInfo = userActRepository.findAll();
        List<UserActivityDB> lstDBInfo = new ArrayList<>();
        Iterator item = lstInfo.iterator();
        while (item.hasNext()) {
            UserActivityDB dbInfo = infoToDB.convertInfoToDB((UserActivity) item.next());
            lstDBInfo.add(dbInfo);
        }
        userActDBRepository.saveAll(lstDBInfo);
        return true;
    }
    public List<UserActivity> findAll(){
        return (List<UserActivity>) userActRepository.findAll();
    }
    public List<UserActivity> findByUrl(String url){
        return userActRepository.findByUrl(url);
    }
}
