package org.streamreasoning.gsp;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;
import org.streamreasoning.gsp.data.PGraph;
import org.streamreasoning.gsp.engine.QueryFactory;
import org.streamreasoning.gsp.engine.Seraph;
import org.streamreasoning.gsp.engine.SeraphSDSImpl;
import org.streamreasoning.gsp.engine.TimeVaryingPGraph;
import org.streamreasoning.gsp.engine.windowing.SeraphStreamToRelationOp;
import org.streamreasoning.rsp4j.api.engine.config.EngineConfiguration;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class Window_Parsing_Test {

    @Test
    public void window_test_1() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ConfigurationException {

        EngineConfiguration ec = EngineConfiguration.loadConfig("/seraph.properties");

        Seraph sr = new Seraph(ec);

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT10S\n" +
                "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT5S " +
                "INTO <http://stream2> }\n");

        ContinuousQueryExecution<PGraph, PGraph, Map<String, Object>, Map<String, Object>> cqe = sr.register(q);

        //range in ms
        //10 secs
        long range = 10000;
        //step in ms
        //5 secs
        long step = 5000;

        SeraphSDSImpl sds = (SeraphSDSImpl) cqe.sds();

        TimeVaryingPGraph tvg = (TimeVaryingPGraph) sds.getGraphs().get(0);

        SeraphStreamToRelationOp op = (SeraphStreamToRelationOp) tvg.getOp();

        assertEquals(range, op.getA());
        assertEquals(step, op.getB());

    }

    @Test
    public void window_test_minutes() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ConfigurationException {

        EngineConfiguration ec = EngineConfiguration.loadConfig("/seraph.properties");

        Seraph sr = new Seraph(ec);

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT1M\n" +
                "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT1M " +
                "INTO <http://stream2> }\n");

        ContinuousQueryExecution<PGraph, PGraph, Map<String, Object>, Map<String, Object>> cqe = sr.register(q);

        //range in ms
        //1 min
        long range = 60000;
        //step in ms
        //1 min
        long step = 60000;

        SeraphSDSImpl sds = (SeraphSDSImpl) cqe.sds();

        TimeVaryingPGraph tvg = (TimeVaryingPGraph) sds.getGraphs().get(0);

        SeraphStreamToRelationOp op = (SeraphStreamToRelationOp) tvg.getOp();

        assertEquals(range, op.getA());
        assertEquals(step, op.getB());

    }
    @Test
    public void window_test_hours() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ConfigurationException {

        EngineConfiguration ec = EngineConfiguration.loadConfig("/seraph.properties");

        Seraph sr = new Seraph(ec);

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT2H\n" +
                "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT1H " +
                "INTO <http://stream2> }\n");

        ContinuousQueryExecution<PGraph, PGraph, Map<String, Object>, Map<String, Object>> cqe = sr.register(q);

        //range in ms
        //2 hours
        long range = 7200000;
        //step in ms
        //1 hour
        long step = 3600000;

        SeraphSDSImpl sds = (SeraphSDSImpl) cqe.sds();

        TimeVaryingPGraph tvg = (TimeVaryingPGraph) sds.getGraphs().get(0);

        SeraphStreamToRelationOp op = (SeraphStreamToRelationOp) tvg.getOp();

        assertEquals(range, op.getA());
        assertEquals(step, op.getB());

    }

    @Test
    public void window_test_mixed() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ConfigurationException {

        EngineConfiguration ec = EngineConfiguration.loadConfig("/seraph.properties");

        Seraph sr = new Seraph(ec);

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT2H\n" +
                "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT1M " +
                "INTO <http://stream2> }\n");

        ContinuousQueryExecution<PGraph, PGraph, Map<String, Object>, Map<String, Object>> cqe = sr.register(q);

        //range in ms
        //2 hours
        long range = 7200000;
        //step in ms
        //1 min
        long step = 60000;

        SeraphSDSImpl sds = (SeraphSDSImpl) cqe.sds();

        TimeVaryingPGraph tvg = (TimeVaryingPGraph) sds.getGraphs().get(0);

        SeraphStreamToRelationOp op = (SeraphStreamToRelationOp) tvg.getOp();

        assertEquals(range, op.getA());
        assertEquals(step, op.getB());

    }

    @Test
    public void window_test_step_bigger_than_range() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ConfigurationException {

        EngineConfiguration ec = EngineConfiguration.loadConfig("/seraph.properties");

        Seraph sr = new Seraph(ec);

        ContinuousQuery q = QueryFactory.parse("" +
                "REGISTER <kafka://example> {\n" +
                "FROM STREAM  <http://stream1> STARTING FROM LATEST\n" +
                "WITH WINDOW RANGE PT1M\n" +
                "MATCH (b1:Bike)-[r1:rentedAt]->(s:Station)\n" +
                "RETURN r1.user_id\n" +
                "EMIT SNAPSHOT EVERY PT2M " +
                "INTO <http://stream2> }\n");

        ContinuousQueryExecution<PGraph, PGraph, Map<String, Object>, Map<String, Object>> cqe = sr.register(q);

        //range in ms
        long range = 60000;
        //step in ms
        long step = 120000;

        SeraphSDSImpl sds = (SeraphSDSImpl) cqe.sds();

        TimeVaryingPGraph tvg = (TimeVaryingPGraph) sds.getGraphs().get(0);

        SeraphStreamToRelationOp op = (SeraphStreamToRelationOp) tvg.getOp();

        assertEquals(range, op.getA());
        assertEquals(step, op.getB());

    }


}
