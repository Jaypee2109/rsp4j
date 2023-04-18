package org.streamreasoning.rsp4j.io.utils.parsing;


/***
 * A strategy for parsing strings to internal used objects, such as RDF Graphs or Triples.
 * @param <T>  The result type of the parsing procedure
 */
@FunctionalInterface
public interface ParsingStrategy<T> {

    /**
     * Parses a string to specified object type T and returns it.
     *
     * @param parseString the string that needs parsing
     * @return the parsed object
     */
    default ParsingResult<T> parseAndAddTime(String parseString) {
        T parsed = parse(parseString);
        return new ParsingResult<T>(parsed);
    }

    public T parse(String parseString);

}
