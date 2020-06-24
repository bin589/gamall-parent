package com.binbin.gmallpublisher.server.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.binbin.gmallpublisher.server.EsService;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.Search.Builder;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation.Entry;

/**
 * @author libin
 * @create 2020-06-20 4:40 下午
 */
@Service
public class EsServiceImpl implements EsService {

    @Autowired
    private JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();

        try {
            SearchResult result = jestClient.execute(search);
            Long total = result.getTotal();
            return total;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("求总数异常");
        }

    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder aggBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();

        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation groupbyHr = result.getAggregations().getTermsAggregation("groupby_hr");
            Map<String, Long> aggMap = new HashMap<>();
            if (groupbyHr != null) {
                List<Entry> buckets = groupbyHr.getBuckets();
                for (Entry bucket : buckets) {
                    aggMap.put(bucket.getKey(), bucket.getCount());
                }
            }
            return aggMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("错误");
        }
    }
}
