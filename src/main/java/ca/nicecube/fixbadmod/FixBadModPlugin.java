package ca.nicecube.fixbadmod;

import ca.nicecube.fixbadmod.commands.FixBadModCommand;
import ca.nicecube.fixbadmod.config.FixBadModConfig;
import ca.nicecube.fixbadmod.config.FixBadModConfigService;
import ca.nicecube.fixbadmod.scan.WorldSanitizerService;
import com.hypixel.hytale.server.core.plugin.JavaPlugin;
import com.hypixel.hytale.server.core.plugin.JavaPluginInit;

import javax.annotation.Nonnull;
import java.nio.file.Path;

public class FixBadModPlugin extends JavaPlugin {
    private FixBadModConfigService configService;
    private FixBadModConfig config;
    private WorldSanitizerService sanitizerService;

    public FixBadModPlugin(@Nonnull JavaPluginInit init) {
        super(init);
    }

    @Override
    protected void setup() {
        Path configuredDataDirectory = this.getDataDirectory();
        Path parentDirectory = configuredDataDirectory.getParent();
        if (parentDirectory != null) {
            configuredDataDirectory = parentDirectory.resolve("FixBadMod");
        }

        this.configService = new FixBadModConfigService(this.getLogger(), configuredDataDirectory);
        this.config = this.configService.loadOrCreate();
        this.sanitizerService = new WorldSanitizerService(this.getLogger());

        this.getCommandRegistry().registerCommand(new FixBadModCommand(this, this.sanitizerService));

        this.getLogger().atInfo().log(
            "[%s] Enabled. Config: %s | templates=%s | rules=%d (config=%d templates=%d enabledTemplates=%d/%d)",
            this.getName(),
            this.configService.getConfigPath().toAbsolutePath(),
            this.configService.getTemplatesDirectory().toAbsolutePath(),
            this.configService.getLastEffectiveRuleCount(),
            this.configService.getLastConfigRuleCount(),
            this.configService.getLastTemplateRuleCount(),
            this.configService.getLastEnabledTemplateCount(),
            this.configService.getLastTemplateFileCount()
        );
        this.getLogger().atInfo().log(
            "[%s] Command: /fixbadmod scan --world=<world> | /fixbadmod execute --world=<world> | /fixbadmod status --world=<world> | /fixbadmod cancel --world=<world> | /fixbadmod reload",
            this.getName()
        );
    }

    @Override
    protected void shutdown() {
        if (this.sanitizerService != null) {
            this.sanitizerService.cancelAll("Plugin is shutting down");
        }

        this.getLogger().atInfo().log("[%s] Disabled.", this.getName());
    }

    public FixBadModConfig getConfigSnapshot() {
        return this.config;
    }

    public FixBadModConfig reloadConfig() {
        this.config = this.configService.loadOrCreate();
        return this.config;
    }

    public Path getConfigPath() {
        return this.configService.getConfigPath();
    }

    public Path getTemplatesPath() {
        return this.configService.getTemplatesDirectory();
    }
}
