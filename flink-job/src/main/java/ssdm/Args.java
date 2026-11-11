package ssdm;

import java.util.HashMap;
import java.util.Map;

/**
 * Tiny "--name value" reader for the job's settings.
 * A setting not given on the command line falls back to an environment
 * variable (--max-out-of-order-ms becomes MAX_OUT_OF_ORDER_MS), then to the default.
 */
public final class Args {

    private final Map<String, String> values = new HashMap<>();

    public static Args parse(String[] args) {
        Args parsed = new Args();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Expected --name value, got: " + args[i]);
            }
            String name = args[i].substring(2);
            boolean hasValue = i + 1 < args.length && !args[i + 1].startsWith("--");
            parsed.values.put(name, hasValue ? args[++i] : "true");
        }
        return parsed;
    }

    public String get(String name, String fallback) {
        String value = values.get(name);
        if (value == null) {
            value = System.getenv(name.replace('-', '_').toUpperCase());
        }
        return value == null || value.isEmpty() ? fallback : value;
    }

    public long getLong(String name, long fallback) {
        return Long.parseLong(get(name, Long.toString(fallback)));
    }

    public boolean getBoolean(String name, boolean fallback) {
        return Boolean.parseBoolean(get(name, Boolean.toString(fallback)));
    }
}
