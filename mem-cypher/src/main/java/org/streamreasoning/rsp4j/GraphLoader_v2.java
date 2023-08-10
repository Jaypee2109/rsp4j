package org.streamreasoning.rsp4j;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.opencypher.memcypher.api.MemCypherGraph;
import org.streamreasoning.gsp.data.Source;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GraphLoader_v2 {
    public static void main(String[] args) {
        String fileName = "demo.json";
        //Create a property graph using the test.json as a base
        URL url = Source.class.getClassLoader().getResource(fileName);
        try {
            FileReader fileReader = new FileReader(url.getPath());

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

            // Define the TypeReference to inform Jackson about the type we want to deserialize to
            TypeReference<MemCypherGraph> typeReference = new TypeReference<MemCypherGraph>() {
            };

            // Deserialize the JSON data into the MemCypherGraph object
            MemCypherGraph graph = objectMapper.readValue(fileReader, MemCypherGraph.class);

            // Use the loaded graph for further operations
            System.out.println("Loaded graph:");
            System.out.println(graph);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
