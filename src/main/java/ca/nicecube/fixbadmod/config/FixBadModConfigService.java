package ca.nicecube.fixbadmod.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hypixel.hytale.logger.HytaleLogger;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FixBadModConfigService {
    private static final Gson GSON = new GsonBuilder()
        .setPrettyPrinting()
        .disableHtmlEscaping()
        .create();
    private static final String CONFIG_FILE_NAME = "config.json";
    private static final String TEMPLATES_DIR_NAME = "templates";
    private static final String DEFAULT_TEMPLATE_FILE_NAME = "nocube_tavern.json";
    private static final String DEFAULT_TEMPLATE_RESOURCE_PATH = "templates/nocube_tavern.json";
    private static final DateTimeFormatter BACKUP_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final HytaleLogger logger;
    private final Path dataDirectory;
    private final Path configPath;
    private final Path templatesDirectory;
    private int lastTemplateFileCount;
    private int lastEnabledTemplateCount;
    private int lastTemplateRuleCount;
    private int lastEffectiveRuleCount;
    private int lastConfigRuleCount;

    public FixBadModConfigService(HytaleLogger logger, Path dataDirectory) {
        this.logger = logger;
        this.dataDirectory = dataDirectory;
        this.configPath = dataDirectory.resolve(CONFIG_FILE_NAME);
        this.templatesDirectory = dataDirectory.resolve(TEMPLATES_DIR_NAME);
    }

    public Path getConfigPath() {
        return this.configPath;
    }

    public Path getTemplatesDirectory() {
        return this.templatesDirectory;
    }

    public int getLastTemplateFileCount() {
        return this.lastTemplateFileCount;
    }

    public int getLastEnabledTemplateCount() {
        return this.lastEnabledTemplateCount;
    }

    public int getLastTemplateRuleCount() {
        return this.lastTemplateRuleCount;
    }

    public int getLastEffectiveRuleCount() {
        return this.lastEffectiveRuleCount;
    }

    public int getLastConfigRuleCount() {
        return this.lastConfigRuleCount;
    }

    public FixBadModConfig loadOrCreate() {
        try {
            Files.createDirectories(this.dataDirectory);
            Files.createDirectories(this.templatesDirectory);
            this.ensureBundledTemplateExists();

            if (Files.notExists(this.configPath)) {
                FixBadModConfig defaults = FixBadModConfig.defaults();
                defaults.normalize();
                this.save(defaults);
                this.logger.atInfo().log("[FixBadMod] Created default config at %s", this.configPath.toAbsolutePath());
                return this.buildEffectiveConfig(defaults);
            }

            FixBadModConfig loaded;
            try (Reader reader = Files.newBufferedReader(this.configPath)) {
                loaded = GSON.fromJson(reader, FixBadModConfig.class);
            }

            if (loaded == null) {
                this.logger.atWarning().log(
                    "[FixBadMod] Config file was empty. Recreating defaults at %s",
                    this.configPath.toAbsolutePath()
                );
                loaded = FixBadModConfig.defaults();
            }

            loaded.normalize();
            this.save(loaded);
            return this.buildEffectiveConfig(loaded);
        } catch (Exception e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed to load config at %s. Falling back to defaults.",
                this.configPath.toAbsolutePath()
            );
            this.backupBrokenConfigIfPresent();

            FixBadModConfig defaults = FixBadModConfig.defaults();
            defaults.normalize();
            this.save(defaults);
            return this.buildEffectiveConfig(defaults);
        }
    }

    private void ensureBundledTemplateExists() {
        Path targetTemplatePath = this.templatesDirectory.resolve(DEFAULT_TEMPLATE_FILE_NAME);
        if (Files.exists(targetTemplatePath)) {
            return;
        }

        try (InputStream resource = FixBadModConfigService.class.getClassLoader()
            .getResourceAsStream(DEFAULT_TEMPLATE_RESOURCE_PATH)) {
            if (resource == null) {
                this.logger.atWarning().log(
                    "[FixBadMod] Built-in template resource '%s' not found. Skipping default template creation.",
                    DEFAULT_TEMPLATE_RESOURCE_PATH
                );
                return;
            }

            Files.copy(resource, targetTemplatePath);
            this.logger.atInfo().log(
                "[FixBadMod] Created default template at %s",
                targetTemplatePath.toAbsolutePath()
            );
        } catch (IOException e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed creating default template at %s",
                targetTemplatePath.toAbsolutePath()
            );
        }
    }

    private FixBadModConfig buildEffectiveConfig(FixBadModConfig baseConfig) {
        int configRuleCount = baseConfig.getRules().size();
        TemplateLoadResult templateLoadResult = this.loadEnabledTemplateRules();

        baseConfig.appendRules(templateLoadResult.rules);
        baseConfig.normalize();

        this.lastConfigRuleCount = configRuleCount;
        this.lastTemplateFileCount = templateLoadResult.templateFileCount;
        this.lastEnabledTemplateCount = templateLoadResult.enabledTemplateCount;
        this.lastTemplateRuleCount = templateLoadResult.enabledTemplateRulesCount;
        this.lastEffectiveRuleCount = baseConfig.getRules().size();

        this.logger.atInfo().log(
            "[FixBadMod] Loaded templates from %s: files=%d enabled=%d templateRules=%d configRules=%d effectiveRules=%d",
            this.templatesDirectory.toAbsolutePath(),
            this.lastTemplateFileCount,
            this.lastEnabledTemplateCount,
            this.lastTemplateRuleCount,
            this.lastConfigRuleCount,
            this.lastEffectiveRuleCount
        );

        return baseConfig;
    }

    private TemplateLoadResult loadEnabledTemplateRules() {
        TemplateLoadResult result = new TemplateLoadResult();
        if (Files.notExists(this.templatesDirectory)) {
            return result;
        }

        try (var templates = Files.list(this.templatesDirectory)) {
            templates
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".json"))
                .sorted()
                .forEach(path -> this.loadTemplateFile(path, result));
        } catch (IOException e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed reading templates directory %s",
                this.templatesDirectory.toAbsolutePath()
            );
        }

        return result;
    }

    private void loadTemplateFile(Path templatePath, TemplateLoadResult result) {
        result.templateFileCount += 1;

        FixBadModTemplateConfig template;
        try (Reader reader = Files.newBufferedReader(templatePath)) {
            template = GSON.fromJson(reader, FixBadModTemplateConfig.class);
        } catch (Exception e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed to parse template %s",
                templatePath.toAbsolutePath()
            );
            return;
        }

        if (template == null) {
            this.logger.atWarning().log(
                "[FixBadMod] Template %s is empty. Skipping.",
                templatePath.toAbsolutePath()
            );
            return;
        }

        template.normalize();
        if (!template.isEnable()) {
            return;
        }

        result.enabledTemplateCount += 1;
        result.enabledTemplateRulesCount += template.getRules().size();
        result.rules.addAll(template.getRules());
        this.logger.atInfo().log(
            "[FixBadMod] Enabled template '%s' (%s) with %d rules",
            templatePath.getFileName(),
            template.getDescription().isBlank() ? "no description" : template.getDescription(),
            template.getRules().size()
        );
    }

    private void backupBrokenConfigIfPresent() {
        if (Files.notExists(this.configPath)) {
            return;
        }

        String suffix = LocalDateTime.now().format(BACKUP_SUFFIX);
        Path backupPath = this.dataDirectory.resolve("config.broken-" + suffix + ".json");

        try {
            Files.move(this.configPath, backupPath);
            this.logger.atWarning().log("[FixBadMod] Backed up broken config to %s", backupPath.toAbsolutePath());
        } catch (IOException moveException) {
            this.logger.atWarning().withCause(moveException).log(
                "[FixBadMod] Could not backup broken config at %s",
                this.configPath.toAbsolutePath()
            );
        }
    }

    private void save(FixBadModConfig config) {
        try (Writer writer = Files.newBufferedWriter(this.configPath)) {
            GSON.toJson(config, writer);
        } catch (IOException e) {
            throw new IllegalStateException("Could not write config file: " + this.configPath.toAbsolutePath(), e);
        }
    }

    private static final class TemplateLoadResult {
        private int templateFileCount;
        private int enabledTemplateCount;
        private int enabledTemplateRulesCount;
        private final java.util.List<FixBadModConfig.Rule> rules = new java.util.ArrayList<>();
    }
}
