package com.example.elastic.service;

import com.example.elastic.configuration.DBConfig;
import com.example.elastic.model.MyKey;
import com.example.elastic.model.UserActivityDB;
import com.example.elastic.model.Users;
import com.example.elastic.repository.UserActDBRepository;
import com.example.elastic.repository.UserActRepository;
import com.example.elastic.repository.UsersRepository;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.*;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.jboss.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hibernate.tool.schema.SchemaToolingLogging.LOGGER;
@Service
public class UserActService2 {

    private final ElasticsearchOperations elasticsearchOperations;
    private final String index = "network_packet";

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Autowired
    UsersRepository usersRepository;
    @Autowired
    DBConfig dbConfig;
    @Autowired
    UserActRepository userActRepository;
    @Autowired
    UserActDBRepository userActDBRepository;

    @Autowired
    public UserActService2(final ElasticsearchOperations elasticsearchOperations) {
        super();
        this.elasticsearchOperations = elasticsearchOperations;
    }
    public static void main(String[] args) throws IOException {
        Instant before = Instant.now();
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
        int searchAfterKey = -1;
        System.out.println("After key start: " + searchAfterKey);
        Map<Object, Object> responseMap = null;
        boolean emptyBuckets = false;

        //list

        while (!emptyBuckets) {
            responseMap = getProfiles(client, searchAfterKey);
            emptyBuckets = (Boolean) responseMap.get("emptyBuckets");
            if (!emptyBuckets) {
                searchAfterKey = (Integer) responseMap.get("afterKeyVal");
            }
        }

        System.out.println("After key end: " + searchAfterKey);
        client.close();
        Instant after = Instant.now();
        long delta = Duration.between(before, after).toMillis();
        System.out.println("Time taken: " + delta + " ms.");
    }

    private static Map<Object, Object> getProfiles(RestHighLevelClient client, int searchAfterKey) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);

        QueryBuilder rangeQuery = QueryBuilders.rangeQuery("effectiveDate").from(0).to(1575518400, false);
        QueryBuilder boolQuery = QueryBuilders.boolQuery().must(rangeQuery);
        ConstantScoreQueryBuilder constScoreQB = new ConstantScoreQueryBuilder(boolQuery);

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<CompositeValuesSourceBuilder<?>>();
        TermsValuesSourceBuilder id = new TermsValuesSourceBuilder("agg_on_id").field("id");
        sources.add(id);
        CompositeAggregationBuilder compositeAggregationBuilder = new CompositeAggregationBuilder("by_id", sources);

        Map<String, Object> afterKey = new HashMap<String, Object>();
        afterKey.put("agg_on_id", searchAfterKey);
        compositeAggregationBuilder.size(10000).aggregateAfter(afterKey).subAggregation(AggregationBuilders
                .topHits("latest_snapshot").sort("effectiveDate", SortOrder.DESC).size(100).fetchSource(true));
        sourceBuilder.query(constScoreQB).aggregation(compositeAggregationBuilder);

        SearchRequest searchRequest = new SearchRequest("from_dynamo");
        searchRequest.source(sourceBuilder);
        System.out.println(searchRequest.source().toString());

        //		response
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        ParsedComposite aggs = searchResponse.getAggregations().get("by_id");

        for (ParsedComposite.ParsedBucket bucket : aggs.getBuckets()) {
            for (Aggregation pth : bucket.getAggregations().asList()) {
                SearchHit[] searchHit = ((ParsedTopHits) pth).getHits().getHits();
                for (SearchHit sh : searchHit) {
                    System.out.println(sh.getSourceAsString());
                }
            }
        }
        boolean emptyBuckets = aggs.getBuckets().isEmpty();
        Map<Object, Object> responseMap = new HashMap<Object, Object>();
        if (!emptyBuckets) {
            Integer afterKeyVal = (Integer) aggs.afterKey().get("agg_on_id");
            responseMap.put("afterKeyVal", afterKeyVal);
        }
        responseMap.put("emptyBuckets", emptyBuckets);
        return responseMap;
    }
    //------------------------------------------------BEGIN-------------------------------------------------------------
    public CompositeAggregation groupByFieldUpdateComposite(String fromDate, String toDate) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(new TimeValue(10, TimeUnit.DAYS));
        //searchBuilder.sort(new ScoreSortBuilder().order(SortOrder.ASC));
        searchBuilder.fetchSource(false);

        TermsAggregationBuilder aggregation1 = AggregationBuilders
                .terms("localdate")
                .field("localdate").size(1000)
                .order(BucketOrder.aggregation("_key",true));

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("user_id").field("user_id.keyword"));
        sources.add(new TermsValuesSourceBuilder("url").field("url.keyword"));
        CompositeAggregationBuilder compositeAggregationBuilder =
                new CompositeAggregationBuilder("byAttributes", sources).size(10000);

        compositeAggregationBuilder.subAggregation(aggregation1);

        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("localdate").gte(fromDate).lte(toDate));

        searchBuilder.query(queryBuilder);
        searchBuilder.aggregation(compositeAggregationBuilder);
        //searchBuilder.sort("@timestamp",SortOrder.ASC);

        SearchRequest searchRequest = Requests.searchRequest("network_packet").allowPartialSearchResults(true)
                .source(searchBuilder);
        SearchResponse searchResponse = dbConfig.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);

        CompositeAggregation compositeAggregation = searchResponse.getAggregations().get("byAttributes");
        return compositeAggregation;
    }
    public Boolean mainProcessing(String timeStamp) throws ParseException, IOException {
        final long startTime = System.currentTimeMillis();
        System.out.println(timeStamp);
        String fromDate = "", toDate = "";
        fromDate = "2021-04-12" + "T01+0700";
        toDate = "2021-04-14" + "T23+0700";
        CompositeAggregation lstRoot = groupByFieldUpdateComposite(fromDate, toDate);
        for (CompositeAggregation.Bucket entry : lstRoot.getBuckets()) {
            Terms bucket = entry.getAggregations().get("localdate");
            String finalPcName = (String) entry.getKey().get("user_id");
            String finalUrl = (String) entry.getKey().get("url");
            List<String> lstDate = new ArrayList<>();
            bucket.getBuckets().forEach(ele -> {
                lstDate.add(ele.getKeyAsString());
            });
            if (!checkContain(finalUrl)) {
                continue;
            }
            final long startTime2 = System.currentTimeMillis();
            executeProcess(lstDate, finalUrl, finalPcName);
            final long endTime2 = System.currentTimeMillis();
            all_save_time += (endTime2 - startTime2);
        }
        final long endTime = System.currentTimeMillis();
        LOGGER.log(Logger.Level.INFO, "Total execution time: " + (endTime - startTime));
        LOGGER.log(Logger.Level.INFO, "find time :" + findTime);
        findTime = 0;
        LOGGER.log(Logger.Level.INFO, "save time all :" + all_save_time);
        all_save_time = 0;
        LOGGER.log(Logger.Level.INFO, "save time :" + saveTime);
        saveTime = 0;
        return true;
    }
    public boolean checkContain(String url) {
        String[] arr = url.split(" ");
        return arr[0].contains("CONNECT");
    }
    public void executeProcess(List<String> lstDate, String url, String finalPcName) {
        List<String> lstDate2 = new ArrayList<>();
        for (int i = 0; i < lstDate.size() - 1; i++) {
            if (getDateFromEL(lstDate.get(i)).equalsIgnoreCase(getDateFromEL(lstDate.get(i + 1)))) {
                lstDate2.add(lstDate.get(i));
            } else {
                lstDate2.add(lstDate.get(i));
                subProcess(lstDate2, url, finalPcName);
                lstDate2 = new ArrayList<>();
            }
            if (i == lstDate.size() - 2) {
                lstDate2.add(lstDate.get(i + 1));
                subProcess(lstDate2, url, finalPcName);
            }
        }
        if (lstDate.size() == 1)
            subProcess(lstDate, url, finalPcName);
        //xl
        //prepare list
        final long start = System.currentTimeMillis();
        processAddCompare2(mKey);
        final long end = System.currentTimeMillis();
        findTime += (end - start);
        //add to db
        final long start3 = System.currentTimeMillis();
        userActDBRepository.saveAll(lstDbList);
        final long end3 = System.currentTimeMillis();
        saveTime += (end3 - start3);
        lstDbList = new ArrayList<>();
    }
    public String splitHeadTail(String x) {
        String[] arr = x.split(" ");
        String[] arrX = arr[1].split(":");
        return arrX[0];
    }
    public String getDateFromEL(String time) {
        String[] arr = time.split("T");
        return arr[0];
    }
    public void addLstKeyTemp(String message, String date, String pcName, float totalTime) {
        MyKey myKey = new MyKey(pcName, message, date);
        mKey.put(myKey,totalTime);
    }

    public void processAddCompare(String message, float totalTime, String date, String pcName) {
        addLstKeyTemp(message, date, pcName, totalTime);
    }

    public void processAddCompare2(Map<MyKey, Float> mKey) {
        Set<MyKey> kSet = mKey.keySet();// all mykey
        List<UserActivityDB> lstResult = userActDBRepository.findAllById(kSet);
        Map<UserActivityDB,Float> mResultExists = new HashMap<>();
        for(int i=0;i<lstResult.size();i++){
            MyKey myKey = new MyKey(lstResult.get(i).getUser_id(), lstResult.get(i).getUrl(), lstResult.get(i).getTime());
            mResultExists.put(lstResult.get(i),mKey.get(myKey));
            mKey.remove(myKey);
        }
        Map<UserActivityDB,Float> mResultNotExists = new HashMap<>();
        Set<MyKey> kSet2 = mKey.keySet();
        for(MyKey myKey : kSet2){
            UserActivityDB userActivityDB = new UserActivityDB(myKey.getUser_id(), myKey.getUrl(), 0, myKey.getTime(),0);
            mResultNotExists.put(userActivityDB,mKey.get(myKey));
        }
        //--got a list not exists--->lstResultNotExists
        addToListExistsAndNot(mResultExists,mResultNotExists);
    }
    public void addToListExistsAndNot(Map<UserActivityDB, Float> mResultExists, Map<UserActivityDB,Float> mResultNotExists) {
        int count = 0;
        float total = 0;
        float totalTime = 0;
        Set<UserActivityDB> set1 = mResultExists.keySet();
        for(UserActivityDB ele : set1){
            count = ele.getCount();
            total = ele.getTotal_time();
            totalTime = mResultExists.get(ele);
            total *= 60;
            totalTime += total;

            UserActivityDB userActivityDB = new UserActivityDB(ele.getUser_id(), ele.getUrl(), ++count, ele.getTime(), (totalTime / 60));
            if (checkExists(ele.getUser_id())) {
                lstDbList.add(userActivityDB);
            }
        }
        Set<UserActivityDB> set2 = mResultNotExists.keySet();
        for(UserActivityDB ele : set2){
            totalTime = mResultNotExists.get(ele);

            UserActivityDB userActivityDB = new UserActivityDB(ele.getUser_id(), ele.getUrl(), 1, ele.getTime(), ((totalTime / 60)));
            if (checkExists(ele.getUser_id())) {
                lstDbList.add(userActivityDB);
            }
        }
    }
    Map<MyKey, Float> mKey = new HashMap<>();
    List<UserActivityDB> lstDbList = new ArrayList<>();
    long findTime = 0;
    long all_save_time = 0;
    long saveTime = 0;
    public boolean checkExists(String pcName) {
        Optional<Users> user = usersRepository.findById(pcName);
        if (user.isPresent())
            return true;
        return false;
    }
    public void subProcess(List<String> lstUserRS, String url, String finalPcName) {
        AtomicReference<String> dateDB = new AtomicReference<>("");
        dateDB.set(getDateFromEL(lstUserRS.get(0)));
        float totalTime = 0;
        for (int i = 0; i < lstUserRS.size() - 1; i++) {
            String timeRootF = getTimeFromEL2(lstUserRS.get(i));
            float secondTimeF = getSecondFromTime2(timeRootF);
            String timeRootS = getTimeFromEL2(lstUserRS.get(i + 1));
            float secondTimeS = getSecondFromTime2(timeRootS);
            float timeUsed = secondTimeS - secondTimeF;
            //time between two surf bigger than 3 minutes--->solve // break time =3m
            if (timeUsed >= 180) {
                if (totalTime == 0)
                    totalTime = 180;
                processAddCompare(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
                totalTime = 0;
            } else
                totalTime += timeUsed;
            if (totalTime >= 600) {
                processAddCompare(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
                totalTime = 0;
            }
            if (i == lstUserRS.size() - 2) {
                if (timeUsed >= 180)
                    processAddCompare(splitHeadTail(url), 180, dateDB.get(), finalPcName);
                else
                    processAddCompare(splitHeadTail(url), totalTime, dateDB.get(), finalPcName);
            }
        }
        if (lstUserRS.size() == 1)
            processAddCompare(splitHeadTail(url), 180, dateDB.get(), finalPcName);
    }
    public float getSecondFromTime2(String time) {
        float total;
        String[] arr = time.split(":");
        int hour = Integer.parseInt(arr[0]) + 7;
        int minute = Integer.parseInt(arr[1]);
        float second = Float.parseFloat(arr[2]);
        total = hour * 60 * 60 + minute * 60 + second;
        return total;
    }

    public String getTimeFromEL2(String time) {
        String[] arr = time.split("T");
        String firstTime = arr[1];
        String[] arr2 = firstTime.split("Z");
        return arr2[0];
    }
}
