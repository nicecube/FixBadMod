package ca.nicecube.fixbadmod.config;

import java.util.ArrayList;
import java.util.List;

public class FixBadModConfig {
    private Scan scan = new Scan();
    private List<Rule> rules = new ArrayList<>();

    public Scan getScan() {
        return this.scan;
    }

    public List<Rule> getRules() {
        return this.rules;
    }

    public static FixBadModConfig defaults() {
        FixBadModConfig config = new FixBadModConfig();
        config.rules.add(new Rule());
        return config;
    }

    public void normalize() {
        if (this.scan == null) {
            this.scan = new Scan();
        }
        this.scan.normalize();

        if (this.rules == null) {
            this.rules = new ArrayList<>();
        }

        List<Rule> normalized = new ArrayList<>(this.rules.size());
        for (Rule rule : this.rules) {
            if (rule == null) {
                continue;
            }

            rule.normalize();
            if (rule.getMatch().isEmpty()) {
                continue;
            }

            normalized.add(rule);
        }

        this.rules = normalized;
    }

    public static class Scan {
        private int maxReplacementsPerChunk = -1;

        public int getMaxReplacementsPerChunk() {
            return this.maxReplacementsPerChunk;
        }

        public void normalize() {
            if (this.maxReplacementsPerChunk == 0 || this.maxReplacementsPerChunk < -1) {
                this.maxReplacementsPerChunk = -1;
            }
        }
    }

    public static class Rule {
        private boolean enabled = true;
        private String match = "NoCube_Tavern_";
        private String mode = "prefix";
        private String replaceWith = "__DELETE__";

        public boolean isEnabled() {
            return this.enabled;
        }

        public String getMatch() {
            return this.match;
        }

        public String getMode() {
            return this.mode;
        }

        public String getReplaceWith() {
            return this.replaceWith;
        }

        public void normalize() {
            if (this.match == null) {
                this.match = "";
            }
            this.match = this.match.trim();

            if (this.mode == null || this.mode.isBlank()) {
                this.mode = "prefix";
            }
            this.mode = this.mode.trim().toLowerCase();

            if (this.replaceWith == null || this.replaceWith.isBlank()) {
                this.replaceWith = "__DELETE__";
            }
            this.replaceWith = this.replaceWith.trim();
        }
    }
}
