package ca.nicecube.fixbadmod.config;

import java.util.ArrayList;
import java.util.List;

public class FixBadModTemplateConfig {
    private boolean enable = false;
    private String description = "";
    private List<FixBadModConfig.Rule> rules = new ArrayList<>();

    public boolean isEnable() {
        return this.enable;
    }

    public String getDescription() {
        return this.description;
    }

    public List<FixBadModConfig.Rule> getRules() {
        return this.rules;
    }

    public void normalize() {
        if (this.description == null) {
            this.description = "";
        }
        this.description = this.description.trim();

        if (this.rules == null) {
            this.rules = new ArrayList<>();
        }

        List<FixBadModConfig.Rule> normalized = new ArrayList<>(this.rules.size());
        for (FixBadModConfig.Rule rule : this.rules) {
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
}
