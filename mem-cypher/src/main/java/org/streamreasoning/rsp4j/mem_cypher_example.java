package org.streamreasoning.rsp4j;

import com.google.gson.Gson;
import org.opencypher.memcypher.api.MemCypherGraph;
import org.opencypher.memcypher.api.MemCypherSession;
import org.opencypher.memcypher.apps.Demo;
import org.opencypher.memcypher.apps.DemoData;
import org.opencypher.okapi.api.graph.CypherResult;
import org.opencypher.okapi.api.graph.PropertyGraph;
import org.opencypher.okapi.impl.util.PrintOptions;
import org.streamreasoning.gsp.data.Source;
import scala.Console;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;

public class mem_cypher_example {

    public static void main(String[] args) throws FileNotFoundException {

        MemCypherSession session = MemCypherSession.create();
        //ToDo either build a conversion layer from pgraph to memCypherGraph or directly load jsons as MemCypherGraphs

        String fileName = "demo.json";
        //Create a property graph using the test.json as a base
       /* URL url = Source.class.getClassLoader().getResource(fileName);
        FileReader fileReader = new FileReader(url.getPath());

        Gson gson = new Gson();

        MemCypherGraph json_graph = gson.fromJson(fileReader, MemCypherGraph.class);
*/
        MemCypherGraph graph = MemCypherGraph.create(DemoData.nodes(), DemoData.rels(), session);

        String query = "" +
                "MATCH (n:Person)\n" +
                "RETURN n.city, n.age\n" +
                "ORDER BY n.city ASC, n.age DESC";

        String query_demo = Demo.query();

        CypherResult result = graph.cypher_with_defaults(query);

       result.show(new PrintOptions(Console.out(), 20));
    }


}