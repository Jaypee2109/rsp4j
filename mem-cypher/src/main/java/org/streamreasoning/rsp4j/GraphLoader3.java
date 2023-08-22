package org.streamreasoning.rsp4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencypher.okapi.api.value.CypherValue;
import org.streamreasoning.gsp.data.Source;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Set;

class GraphLoader3 {

    static class MemNode {
        public long id;
        public Set<String> labels;
        public CypherValue.CypherMap$ properties;
    }

    static class MemRelationship {
        public long id;
        public long source;
        public long target;
        public String relType;
        public CypherValue.CypherMap$ properties;
    }

    static class MemCypherGraph {
        public List<MemNode> nodes;
        public List<MemRelationship> rels;
    }

    public static void main(String[] args) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String fileName = "demo.json";
            URL url = Source.class.getClassLoader().getResource(fileName);
            File jsonFile = new File(url.getPath());
            MemCypherGraph memCypherGraph = objectMapper.readValue(jsonFile, MemCypherGraph.class);

            // Now you can work with the loaded data in memCypherGraph
            System.out.println("Nodes:");
            for (MemNode node : memCypherGraph.nodes) {
                System.out.println("ID: " + node.id);
                System.out.println("Labels: " + node.labels);
                System.out.println("Properties: " + node.properties);
            }

            System.out.println("Relationships:");
            for (MemRelationship rel : memCypherGraph.rels) {
                System.out.println("ID: " + rel.id);
                System.out.println("Source: " + rel.source);
                System.out.println("Target: " + rel.target);
                System.out.println("Type: " + rel.relType);
                System.out.println("Properties: " + rel.properties);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}