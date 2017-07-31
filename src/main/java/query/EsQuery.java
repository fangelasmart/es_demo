package query;

import client.EsClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by angela on 17/7/7.
 */
public class EsQuery {

    /**
     * 查询
     *
     * @param queryBuilder
     * @param indexName
     * @param indexType
     * @param start
     * @param row
     * @return
     */
    public static SearchResponse getSearchResponse(QueryBuilder queryBuilder, String indexName, String indexType, int start, int row) {

        long begin = System.currentTimeMillis();
        Client client = EsClient.getEsConnection();
        long end = System.currentTimeMillis();
        System.out.println("连接ES所需时间：" + (end - begin));
        SearchResponse searchResponse =
                client.prepareSearch(indexName)
                        .setTypes(indexType)
                        .setQuery(queryBuilder)
                        .setFrom(start)
                        .setSize(row)
                        .execute()
                        .actionGet();
        client.close();
        return searchResponse;
    }


    /**
     * 获取查询的queryBuilder
     *
     * @param map
     * @return
     */
    public static QueryBuilder getQuery(Map<String, Object> map) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        //查询字段或条件判空
        if (null == map) {
            return null;
        }
        Iterator entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            String field = (String) entry.getKey();
            String value = (String) entry.getValue();
            query = conditionQuery(query, field, value);
        }
        return query;
    }

    /**
     * 根据参数拼接queryBuilder
     *
     * @param queryBuilder
     * @param field
     * @param value
     * @return
     */
    private static BoolQueryBuilder conditionQuery(BoolQueryBuilder queryBuilder, String field, String value) {
        if (StringUtils.isEmpty(field)) {
            return queryBuilder;
        }
        if ("name".equals(field)) {
            queryBuilder.must(QueryBuilders.termsQuery(field, value));
        }
        if ("class".equals(field)) {
            queryBuilder.should(QueryBuilders.termsQuery(field, value));
        }
        return queryBuilder;
    }

    /**
     * term query
     *
     * @param term
     * @param value
     * @param client
     * @param index
     * @param type
     * @return
     */
    public SearchResponse termQuery(String term, String value, Client client, String index, String type) {
        //term
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(term, value);
        //response
        SearchResponse searchResponse = client.prepareSearch(index) //index
                .setSearchType(type)    //document type
                .setQuery(termsQueryBuilder) //query
                .execute()  //execte
                .actionGet();   //return result
        return searchResponse;
    }

    /**
     * order term query
     *
     * @param term
     * @param value
     * @param client
     * @param index
     * @param type
     * @param sort
     * @param sortType
     * @return
     */
    public SearchResponse termSortQuery(String term, String value, Client client, String index, String type, String sort, SortOrder sortType) {

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(term, value);
        //set order field and type
        SortBuilder sortBuilder = SortBuilders.fieldSort(sort).order(sortType);

        SearchResponse searchResponse = client.prepareSearch(index)
                .setSearchType(type)
                .setQuery(termsQueryBuilder)
                .addSort(sortBuilder)   //order
                .execute()
                .actionGet();
        return searchResponse;
    }

    /**
     * order term page query
     *
     * @param term
     * @param value
     * @param client
     * @param index
     * @param type
     * @param sortFiled
     * @param sort
     * @param from
     * @param size
     * @return
     */
    public SearchResponse termSortAndPageQuery(String term, String value, Client client, String index, String type, String sortFiled, SortOrder sort, int from, int size) {
        //term
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(term, value);
        //sort
        SortBuilder sortBuilder = SortBuilders.fieldSort(sortFiled).order(sort);

        SearchResponse searchResponse = client.prepareSearch(index)
                .setSearchType(type)
                .setQuery(termsQueryBuilder)
                .addSort(sortBuilder)
                .setFrom(from)  //from
                .setSize(size)  //size
                .execute()
                .actionGet();

        return searchResponse;
    }

    /**
     * sort page term query and return specified fields
     *
     * @param client
     * @param term
     * @param value
     * @param index
     * @param type
     * @param sort
     * @param order
     * @param fields
     * @param from
     * @param size
     * @return
     */
    public SearchResponse fieldTermQuery(Client client, String term, String value, String index, String type, String sort, SortOrder order, List<String> fields, int from, int size) {
        //term
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(term, value);
        //sort
        SortBuilder sortBuilder = SortBuilders.fieldSort(sort).order(order);

        SearchResponse searchResponse = client.prepareSearch(index)
                .setSearchType(type)
                .setQuery(termsQueryBuilder)
                .addSort(sortBuilder)
                .setFrom(from)
                .setSize(size)
                .addFields(fields.toString())
                .execute().actionGet();

        SearchHits searchHits = searchResponse.getHits();
        long resultSize = searchHits.getTotalHits();
        if (resultSize <= 0) {
            return null;
        }
        for (SearchHit hit : searchHits.getHits()) {
            Map<String, SearchHitField> map = hit.getFields();
            SearchHitField searchHitField = map.get("name");
            System.out.println("name:" + searchHitField.getName() + "--------value:" + searchHitField.getValue());
        }
        return searchResponse;
    }

  /*  public SearchResponse partialTermQuery(String index,String type,Client client,){

    }
*/


}
