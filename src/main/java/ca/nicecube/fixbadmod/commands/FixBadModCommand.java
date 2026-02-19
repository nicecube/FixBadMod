package ca.nicecube.fixbadmod.commands;

import ca.nicecube.fixbadmod.FixBadModPlugin;
import ca.nicecube.fixbadmod.scan.WorldSanitizerService;
import com.hypixel.hytale.component.Store;
import com.hypixel.hytale.server.core.Message;
import com.hypixel.hytale.server.core.command.system.CommandContext;
import com.hypixel.hytale.server.core.command.system.basecommands.AbstractCommandCollection;
import com.hypixel.hytale.server.core.command.system.basecommands.AbstractWorldCommand;
import com.hypixel.hytale.server.core.command.system.basecommands.CommandBase;
import com.hypixel.hytale.server.core.universe.world.World;
import com.hypixel.hytale.server.core.universe.world.storage.EntityStore;

public class FixBadModCommand extends AbstractCommandCollection {
    private final FixBadModPlugin plugin;
    private final WorldSanitizerService sanitizerService;

    public FixBadModCommand(FixBadModPlugin plugin, WorldSanitizerService sanitizerService) {
        super("fixbadmod", "Dry-scan chunks and replace broken mod blocks after confirmation");
        this.plugin = plugin;
        this.sanitizerService = sanitizerService;

        this.addSubCommand(new ScanSubCommand());
        this.addSubCommand(new ExecuteSubCommand());
        this.addSubCommand(new RunAliasSubCommand());
        this.addSubCommand(new StatusSubCommand());
        this.addSubCommand(new CancelSubCommand());
        this.addSubCommand(new ReloadSubCommand());
    }

    private static void send(CommandContext context, String message) {
        context.sendMessage(Message.raw("[FixBadMod] " + message));
    }

    private final class ScanSubCommand extends AbstractWorldCommand {
        private ScanSubCommand() {
            super("scan", "Run dry-scan on saved chunks without changing blocks");
        }

        @Override
        protected void execute(CommandContext context, World world, Store<EntityStore> entityStore) {
            WorldSanitizerService.StartResult result = FixBadModCommand.this.sanitizerService.startScan(
                world,
                FixBadModCommand.this.plugin.getConfigSnapshot()
            );
            send(context, result.getMessage());
            if (result.isStarted()) {
                send(
                    context,
                    "When scan is done, run /fixbadmod status --world=" + world.getName() +
                        " then /fixbadmod execute --world=" + world.getName()
                );
            }
        }
    }

    private final class ExecuteSubCommand extends AbstractWorldCommand {
        private ExecuteSubCommand() {
            super("execute", "Apply replacements after a completed dry-scan");
        }

        @Override
        protected void execute(CommandContext context, World world, Store<EntityStore> entityStore) {
            WorldSanitizerService.StartResult result = FixBadModCommand.this.sanitizerService.executePending(
                world,
                FixBadModCommand.this.plugin.getConfigSnapshot()
            );
            send(context, result.getMessage());
        }
    }

    private final class RunAliasSubCommand extends AbstractWorldCommand {
        private RunAliasSubCommand() {
            super("run", "Alias of /fixbadmod execute");
        }

        @Override
        protected void execute(CommandContext context, World world, Store<EntityStore> entityStore) {
            send(context, "Deprecated: use /fixbadmod execute --world=" + world.getName());
            WorldSanitizerService.StartResult result = FixBadModCommand.this.sanitizerService.executePending(
                world,
                FixBadModCommand.this.plugin.getConfigSnapshot()
            );
            send(context, result.getMessage());
        }
    }

    private final class StatusSubCommand extends AbstractWorldCommand {
        private StatusSubCommand() {
            super("status", "Show active/last job status and pending confirmation state");
        }

        @Override
        protected void execute(CommandContext context, World world, Store<EntityStore> entityStore) {
            String worldName = world.getName();
            WorldSanitizerService.JobStatus status = FixBadModCommand.this.sanitizerService.getStatus(worldName);
            WorldSanitizerService.PendingScan pending = FixBadModCommand.this.sanitizerService.getPendingScan(worldName);

            if (status == null && pending == null) {
                send(context, "No scan data for world '" + worldName + "'. Run /fixbadmod scan --world=" + worldName);
                return;
            }

            if (status != null) {
                String state;
                if (status.isRunning()) {
                    state = "running";
                } else if (status.isCancelled()) {
                    state = "cancelled";
                } else {
                    state = "completed";
                }

                String metricLabel = status.getMode() == WorldSanitizerService.JobMode.SCAN ? "matches" : "replaced";
                send(
                    context,
                    String.format(
                        "world=%s mode=%s state=%s progress=%d/%d (%.2f%%) touched=%d failed=%d %s=%d elapsed=%.2fs",
                        status.getWorldName(),
                        status.getMode(),
                        state,
                        status.getProcessedChunks(),
                        status.getTotalChunks(),
                        status.getProgressPercent(),
                        status.getTouchedChunks(),
                        status.getFailedChunks(),
                        metricLabel,
                        status.getTotalMatches(),
                        status.getElapsedSeconds()
                    )
                );
                if (status.isCancelled() && !status.getCancelReason().isBlank()) {
                    send(context, "cancelReason=" + status.getCancelReason());
                }
            }

            if (pending != null) {
                send(
                    context,
                    String.format(
                        "pendingConfirm=true world=%s chunks=%d touched=%d failed=%d matches=%d age=%.2fs",
                        pending.getWorldName(),
                        pending.getTotalChunks(),
                        pending.getTouchedChunks(),
                        pending.getFailedChunks(),
                        pending.getTotalMatches(),
                        pending.getAgeSeconds()
                    )
                );
                send(context, "To apply now: /fixbadmod execute --world=" + worldName);
            } else {
                send(context, "pendingConfirm=false");
            }
        }
    }

    private final class CancelSubCommand extends AbstractWorldCommand {
        private CancelSubCommand() {
            super("cancel", "Cancel running scan/execute job in world");
        }

        @Override
        protected void execute(CommandContext context, World world, Store<EntityStore> entityStore) {
            boolean cancelled = FixBadModCommand.this.sanitizerService.cancel(world.getName(), "Cancelled by command");
            if (!cancelled) {
                send(context, "No running job to cancel in world '" + world.getName() + "'");
                return;
            }

            send(context, "Cancel requested for world '" + world.getName() + "'");
        }
    }

    private final class ReloadSubCommand extends CommandBase {
        private ReloadSubCommand() {
            super("reload", "Reload FixBadMod config from disk");
        }

        @Override
        protected void executeSync(CommandContext context) {
            FixBadModCommand.this.plugin.reloadConfig();
            send(
                context,
                "Config reloaded from " + FixBadModCommand.this.plugin.getConfigPath().toAbsolutePath()
            );
        }
    }
}
