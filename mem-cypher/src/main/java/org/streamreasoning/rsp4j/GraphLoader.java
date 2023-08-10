package org.streamreasoning.rsp4j;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.opencypher.memcypher.api.MemCypherGraph;
import org.opencypher.memcypher.api.MemCypherSession;
import org.opencypher.memcypher.api.value.MemNode;
import org.opencypher.memcypher.api.value.MemRelationship;
import org.opencypher.memcypher.apps.Demo;
import org.opencypher.okapi.api.graph.CypherResult;
import org.opencypher.okapi.impl.util.PrintOptions;
import org.streamreasoning.gsp.data.Source;
import scala.Console;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Set;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class GraphLoader {

    public static MemCypherGraph loadGraphFromFile(String jsonFilePath, MemCypherSession session) throws IOException {
        FileReader fileReader = new FileReader(jsonFilePath);
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Set.class, new SetInstanceCreator<>())
                .registerTypeAdapter(MemCypherGraph.class, new MemCypherGraphDeserializer(session))
                .create();

        return gson.fromJson(fileReader, MemCypherGraph.class);
    }

    private static class SetInstanceCreator<E> implements InstanceCreator<Set<E>> {
        @Override
        public Set<E> createInstance(Type type) {
            // Use Scala's empty set
            return (Set<E>) scala.collection.immutable.Set$.MODULE$.empty();
        }
    }

    private static class MemCypherGraphDeserializer implements JsonDeserializer<MemCypherGraph> {
        private final MemCypherSession session;

        public MemCypherGraphDeserializer(MemCypherSession session) {
            this.session = session;
        }
        @Override
        public MemCypherGraph deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            JsonArray nodesJsonArray = jsonObject.getAsJsonArray("nodes");
            JsonArray relsJsonArray = jsonObject.getAsJsonArray("edges");

            // Cast List to the expected types before converting to Seq
            List<MemNode> memNodes = context.deserialize(nodesJsonArray, new TypeToken<List<MemNode>>() {}.getType());
            List<MemRelationship> memRels = context.deserialize(relsJsonArray, new TypeToken<List<MemRelationship>>() {}.getType());

            // Convert to Seq using ScalaConverters
            Seq<MemNode> memNodesSeq = JavaConverters.asScalaBufferConverter(memNodes).asScala().toSeq();
            Seq<MemRelationship> memRelsSeq = JavaConverters.asScalaBufferConverter(memRels).asScala().toSeq();

            return MemCypherGraph.create(memNodesSeq, memRelsSeq, session);
        }
    }

    public static void main(String[] args) {
        try{
            MemCypherSession session = MemCypherSession.create();
            String fileName = "demo.json";
            //Create a property graph using the test.json as a base
            URL url = Source.class.getClassLoader().getResource(fileName);
            MemCypherGraph graph = loadGraphFromFile(url.getPath(), session);
            String query = "" +
                    "MATCH (n:Person)\n" +
                    "RETURN n.city, n.age\n" +
                    "ORDER BY n.city ASC, n.age DESC";

            CypherResult result = graph.cypher_with_defaults(query);

            result.show(new PrintOptions(Console.out(), 20));
            // Use the loaded graph for further operations
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
