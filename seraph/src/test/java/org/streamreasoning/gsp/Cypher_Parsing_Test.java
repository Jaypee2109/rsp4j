package org.streamreasoning.gsp;

import org.junit.Test;

import org.streamreasoning.gsp.engine.*;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;


import java.io.IOException;


import static junit.framework.TestCase.assertEquals;

public class Cypher_Parsing_Test {

    @Test
    public void query_test() throws IOException {

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT10S\n" +
                "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT5S " +
                "INTO <http://stream2> }\n");

        String expected_query = "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                                "RETURN r1.user_id\n";

        assertEquals(expected_query, q.getR2R());

    }

    @Test
    public void query_test_minus() throws IOException{

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT10S\n" +
                "MATCH (b1:E-Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT5S " +
                "INTO <http://stream2> }\n");

        String expected_query = "MATCH (b1:E-Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n";

        assertEquals(expected_query, q.getR2R());

    }

    @Test
    public void query_test_mixed_arrows() throws IOException{

        ContinuousQuery studentTrick = QueryFactory.parse("" +
                "REGISTER QUERY student_trick STARTING AT 2022-10-14T14:45 {\n" +
                "WITH duration({minutes : 5}) as _5m,\n" +
                "duration({minutes : 20}) as _20m\n" +
                "MATCH (s:Station)<-[r1:rentedAt]-(b1:Bike),\n" +
                "(b1)-[n1:returnedAt]->(p:Station),\n" +
                "(p)<-[r2:rentedAt]-(b2:Bike),\n" +
                "(b2)-[n2:returnedAt]->(o:Station)\n" +
                "WITHIN PT1H\n" +
                "WHERE r1.user_id = n1.user_id AND\n" +
                "n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND\n" +
                "n1.val_time < r2.val_time AND\n" +
                "duration.between(n1.val_time,r2.val_time) < _5m AND\n" +
                "duration.between(r1.val_time,n1.val_time) < _20m AND\n" +
                "duration.between(r2.val_time,n2.val_time) < _20m\n" +
                "EMIT r1.user_id, s.id, p.id, o.id\n" +
                "ON ENTERING\n" +
                "EVERY PT5M\n" +
                "}");

        String expected_query = "" +
                "WITH duration({minutes : 5}) as _5m,\n" +
                "duration({minutes : 20}) as _20m\n" +
                "MATCH (s:Station)<-[r1:rentedAt]-(b1:Bike),\n" +
                "(b1)-[n1:returnedAt]->(p:Station),\n" +
                "(p)<-[r2:rentedAt]-(b2:Bike),\n" +
                "(b2)-[n2:returnedAt]->(o:Station)\n" +
                "WHERE r1.user_id = n1.user_id AND\n" +
                "n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND\n" +
                "n1.val_time < r2.val_time AND\n" +
                "duration.between(n1.val_time,r2.val_time) < _5m AND\n" +
                "duration.between(r1.val_time,n1.val_time) < _20m AND\n" +
                "duration.between(r2.val_time,n2.val_time) < _20m\n" +
                "EMIT r1.user_id, s.id, p.id, o.id\n";

        assertEquals(expected_query, studentTrick.getR2R());

    }

    @Test
    public void query_test_or_operator() throws IOException{

        ContinuousQuery studentTrick = QueryFactory.parse("" +
                "REGISTER QUERY student_trick STARTING AT 2022-10-14T14:45 {\n" +
                "WITH duration({minutes : 5}) as _5m,\n" +
                "duration({minutes : 20}) as _20m\n" +
                "MATCH (s:Station)->[r1:rentedAt]-(b1:Bike|Bike_test),\n" +
                "(b1)-[n1:returnedAt]->(p:Station)\n" +
                "WITHIN PT1H\n" +
                "WHERE r1.user_id = n1.user_id AND\n" +
                "n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND\n" +
                "n1.val_time < r2.val_time AND\n" +
                "duration.between(n1.val_time,r2.val_time) < _5m AND\n" +
                "duration.between(r1.val_time,n1.val_time) < _20m AND\n" +
                "duration.between(r2.val_time,n2.val_time) < _20m\n" +
                "EMIT r1.user_id, s.id, p.id, o.id\n" +
                "ON ENTERING\n" +
                "EVERY PT5M\n" +
                "}");

        String expected_query = "" +
                "WITH duration({minutes : 5}) as _5m,\n" +
                "duration({minutes : 20}) as _20m\n" +
                "MATCH (s:Station)->[r1:rentedAt]-(b1:Bike|Bike_test),\n" +
                "(b1)-[n1:returnedAt]->(p:Station)\n" +
                "WHERE r1.user_id = n1.user_id AND\n" +
                "n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND\n" +
                "n1.val_time < r2.val_time AND\n" +
                "duration.between(n1.val_time,r2.val_time) < _5m AND\n" +
                "duration.between(r1.val_time,n1.val_time) < _20m AND\n" +
                "duration.between(r2.val_time,n2.val_time) < _20m\n" +
                "EMIT r1.user_id, s.id, p.id, o.id\n";

        assertEquals(expected_query, studentTrick.getR2R());

    }

    @Test
    public void query_test_studentTrick() throws IOException{

        ContinuousQuery studentTrick = QueryFactory.parse("" +
                "REGISTER QUERY student_trick STARTING AT 2022-10-14T14:45 {\n" +
                "WITH duration({minutes : 5}) as _5m,\n" +
                "duration({minutes : 20}) as _20m\n" +
                "MATCH (s:Station)<-[r1:rentedAt]-(b1:Bike|E-Bike),\n" +
                "(b1)-[n1:returnedAt]->(p:Station),\n" +
                "(p)<-[r2:rentedAt]-(b2:Bike|E-Bike),\n" +
                "(b2)-[n2:returnedAt]->(o:Station)\n" +
                "WITHIN PT1H\n" +
                "WHERE r1.user_id = n1.user_id AND\n" +
                "n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND\n" +
                "n1.val_time < r2.val_time AND\n" +
                "duration.between(n1.val_time,r2.val_time) < _5m AND\n" +
                "duration.between(r1.val_time,n1.val_time) < _20m AND\n" +
                "duration.between(r2.val_time,n2.val_time) < _20m\n" +
                "EMIT r1.user_id, s.id, p.id, o.id\n" +
                "ON ENTERING\n" +
                "EVERY PT5M\n" +
                "}");

        String expected_query = "" +
                "WITH duration({minutes : 5}) as _5m,\n" +
                "duration({minutes : 20}) as _20m\n" +
                "MATCH (s:Station)<-[r1:rentedAt]-(b1:Bike|E-Bike),\n" +
                "(b1)-[n1:returnedAt]->(p:Station),\n" +
                "(p)<-[r2:rentedAt]-(b2:Bike|E-Bike),\n" +
                "(b2)-[n2:returnedAt]->(o:Station)\n" +
                "WHERE r1.user_id = n1.user_id AND\n" +
                "n1.user_id = r2.user_id AND r2.user_id = n2.user_id AND\n" +
                "n1.val_time < r2.val_time AND\n" +
                "duration.between(n1.val_time,r2.val_time) < _5m AND\n" +
                "duration.between(r1.val_time,n1.val_time) < _20m AND\n" +
                "duration.between(r2.val_time,n2.val_time) < _20m\n" +
                "EMIT r1.user_id, s.id, p.id, o.id\n";

        assertEquals(expected_query, studentTrick.getR2R());

    }

}
