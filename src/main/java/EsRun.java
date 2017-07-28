import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import query.EsQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by angela on 17/7/7.
 */
public class EsRun {
    public static void main(String[] args) {
        Map<String,Object> map = new HashMap<String,Object>();

        map.put("name","lily");
        map.put("class","001");

        QueryBuilder queryBuilder = EsQuery.getQuery(map);
        SearchResponse searchResponse = EsQuery.getSearchResponse(queryBuilder,"fq","teacher",1,10);
        System.out.println("------------"+searchResponse.toString());

    }
}
