package ca.nicecube.fixbadmod.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hypixel.hytale.logger.HytaleLogger;

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
    private static final DateTimeFormatter BACKUP_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final HytaleLogger logger;
    private final Path dataDirectory;
    private final Path configPath;

    public FixBadModConfigService(HytaleLogger logger, Path dataDirectory) {
        this.logger = logger;
        this.dataDirectory = dataDirectory;
        this.configPath = dataDirectory.resolve(CONFIG_FILE_NAME);
    }

    public Path getConfigPath() {
        return this.configPath;
    }

    public FixBadModConfig loadOrCreate() {
        try {
            Files.createDirectories(this.dataDirectory);
            if (Files.notExists(this.configPath)) {
                FixBadModConfig defaults = FixBadModConfig.defaults();
                defaults.normalize();
                this.save(defaults);
                this.logger.atInfo().log("[FixBadMod] Created default config at %s", this.configPath.toAbsolutePath());
                return defaults;
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
            return loaded;
        } catch (Exception e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed to load config at %s. Falling back to defaults.",
                this.configPath.toAbsolutePath()
            );
            this.backupBrokenConfigIfPresent();

            FixBadModConfig defaults = FixBadModConfig.defaults();
            defaults.normalize();
            this.save(defaults);
            return defaults;
        }
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
}
