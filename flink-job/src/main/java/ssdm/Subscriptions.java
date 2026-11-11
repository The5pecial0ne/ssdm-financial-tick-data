package ssdm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * The symbols traders are subscribed to.
 *
 * Averages are computed for every symbol all the time, but only subscribed
 * symbols are published. "ALL" publishes everything, which is what the
 * correctness check against the answer key needs.
 */
public class Subscriptions implements FilterFunction<EmaResult> {

    private static final long serialVersionUID = 1L;

    private final boolean all;
    private final Set<String> symbols;

    /** @param spec "ALL", or symbols separated by commas */
    public Subscriptions(String spec) {
        String trimmed = spec == null ? "" : spec.trim();
        this.all = trimmed.isEmpty() || trimmed.equalsIgnoreCase("ALL");
        this.symbols = new HashSet<>();
        if (!all) {
            Arrays.stream(trimmed.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(symbols::add);
        }
    }

    @Override
    public boolean filter(EmaResult result) {
        return all || symbols.contains(result.symbol);
    }

    @Override
    public String toString() {
        return all ? "all symbols" : symbols.size() + " symbols";
    }
}
