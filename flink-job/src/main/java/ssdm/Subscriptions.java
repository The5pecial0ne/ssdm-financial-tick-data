package ssdm;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The symbols traders subscribe to (project spec 3.1, item 3).
 *
 * Every symbol is still computed, because EMAs need the full history, but results and
 * advisories are only published for subscribed ones. The list is read on the client when the
 * job is submitted and shipped inside the job graph, so the TaskManagers don't need the file.
 *
 * Accepted forms: "all" (or empty), a comma-separated list, or "@path/to/file" with one symbol per line.
 */
public class Subscriptions implements Serializable {

    private final Set<String> symbols;   // null means everything

    private Subscriptions(Set<String> symbols) {
        this.symbols = symbols;
    }

    public static Subscriptions parse(String spec) throws IOException {
        if (spec == null || spec.isBlank() || spec.equalsIgnoreCase("all")) {
            return new Subscriptions(null);
        }
        if (spec.startsWith("@")) {
            Set<String> s = Files.readAllLines(Paths.get(spec.substring(1))).stream()
                    .map(String::trim)
                    .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                    .collect(Collectors.toCollection(HashSet::new));
            return new Subscriptions(s);
        }
        return new Subscriptions(Arrays.stream(spec.split(","))
                .map(String::trim).filter(x -> !x.isEmpty())
                .collect(Collectors.toCollection(HashSet::new)));
    }

    public boolean contains(String symbol) {
        return symbols == null || symbols.contains(symbol);
    }

    public String describe() {
        return symbols == null ? "all symbols" : symbols.size() + " symbols";
    }
}
