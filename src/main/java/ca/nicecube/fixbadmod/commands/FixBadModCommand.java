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

import java.util.ArrayList;
import java.util.List;

public class FixBadModCommand extends AbstractCommandCollection {
    private static final String PREFIX = "[FixBadMod]";
    private static final String DIVIDER = "------------------------------------------------------------";
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[96m";
    private static final String ANSI_GREEN = "\u001B[92m";
    private static final String ANSI_YELLOW = "\u001B[93m";
    private static final String ANSI_RED = "\u001B[91m";
    private static final String ANSI_GRAY = "\u001B[90m";

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

    private enum Tone {
        INFO,
        SUCCESS,
        WARN,
        ERROR,
        MUTED
    }

    private static void send(CommandContext context, String message) {
        send(context, Tone.INFO, message);
    }

    private static void send(CommandContext context, Tone tone, String message) {
        String fullMessage = PREFIX + " " + message;
        if (context.isPlayer()) {
            Message output = Message.raw(fullMessage);
            applyPlayerColor(output, tone);
            context.sendMessage(output);
            return;
        }

        context.sendMessage(Message.raw(applyAnsiColor(fullMessage, tone)));
    }

    private static void sendSection(CommandContext context, String title, Tone titleTone, List<String> lines) {
        send(context, Tone.MUTED, DIVIDER);
        send(context, titleTone, title);
        for (String line : lines) {
            send(context, Tone.INFO, "  " + line);
        }
        send(context, Tone.MUTED, DIVIDER);
    }

    private static void applyPlayerColor(Message message, Tone tone) {
        switch (tone) {
            case SUCCESS -> message.color("#7CFC7C");
            case WARN -> message.color("#FFD166");
            case ERROR -> message.color("#FF7B7B");
            case MUTED -> message.color("#9BA3AF");
            case INFO -> message.color("#6FD3FF");
        }
    }

    private static String applyAnsiColor(String text, Tone tone) {
        String code = switch (tone) {
            case SUCCESS -> ANSI_GREEN;
            case WARN -> ANSI_YELLOW;
            case ERROR -> ANSI_RED;
            case MUTED -> ANSI_GRAY;
            case INFO -> ANSI_CYAN;
        };

        return code + text + ANSI_RESET;
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
            String worldName = world.getName();
            if (result.isStarted()) {
                sendSection(
                    context,
                    "SCAN STARTED",
                    Tone.SUCCESS,
                    List.of(
                        "World: " + worldName,
                        result.getMessage(),
                        "Next: /fixbadmod status --world=" + worldName,
                        "Then: /fixbadmod execute --world=" + worldName
                    )
                );
                return;
            }

            sendSection(
                context,
                "SCAN NOT STARTED",
                Tone.WARN,
                List.of(
                    "World: " + worldName,
                    result.getMessage()
                )
            );
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
            String worldName = world.getName();
            if (result.isStarted()) {
                sendSection(
                    context,
                    "APPLY STARTED",
                    Tone.SUCCESS,
                    List.of(
                        "World: " + worldName,
                        result.getMessage(),
                        "Use /fixbadmod status --world=" + worldName + " to monitor progress"
                    )
                );
                return;
            }

            sendSection(
                context,
                "APPLY NOT STARTED",
                Tone.WARN,
                List.of(
                    "World: " + worldName,
                    result.getMessage()
                )
            );
        }
    }

    private final class RunAliasSubCommand extends AbstractWorldCommand {
        private RunAliasSubCommand() {
            super("run", "Alias of /fixbadmod execute");
        }

        @Override
        protected void execute(CommandContext context, World world, Store<EntityStore> entityStore) {
            send(context, Tone.WARN, "Deprecated: use /fixbadmod execute --world=" + world.getName());
            WorldSanitizerService.StartResult result = FixBadModCommand.this.sanitizerService.executePending(
                world,
                FixBadModCommand.this.plugin.getConfigSnapshot()
            );
            if (result.isStarted()) {
                send(context, Tone.SUCCESS, result.getMessage());
                return;
            }

            send(context, Tone.WARN, result.getMessage());
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
                sendSection(
                    context,
                    "NO DATA",
                    Tone.WARN,
                    List.of(
                        "World: " + worldName,
                        "No scan data available yet.",
                        "Run: /fixbadmod scan --world=" + worldName
                    )
                );
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
                Tone statusTone = switch (state) {
                    case "running" -> Tone.INFO;
                    case "cancelled" -> Tone.WARN;
                    default -> Tone.SUCCESS;
                };

                List<String> lines = new ArrayList<>();
                lines.add("World: " + status.getWorldName());
                lines.add("Mode: " + status.getMode());
                lines.add("State: " + state);
                lines.add(
                    String.format(
                        "Progress: %d/%d (%.2f%%)",
                        status.getProcessedChunks(),
                        status.getTotalChunks(),
                        status.getProgressPercent()
                    )
                );
                lines.add("Touched chunks: " + status.getTouchedChunks());
                lines.add("Failed chunks: " + status.getFailedChunks());
                lines.add(metricLabel + ": " + status.getTotalMatches());
                lines.add(String.format("Elapsed: %.2fs", status.getElapsedSeconds()));

                if (status.isCancelled() && !status.getCancelReason().isBlank()) {
                    lines.add("Cancel reason: " + status.getCancelReason());
                }

                sendSection(context, "JOB STATUS", statusTone, lines);
            }

            if (pending != null) {
                sendSection(
                    context,
                    "PENDING CONFIRMATION",
                    Tone.WARN,
                    List.of(
                        "World: " + pending.getWorldName(),
                        "Chunks: " + pending.getTotalChunks(),
                        "Touched chunks: " + pending.getTouchedChunks(),
                        "Failed chunks: " + pending.getFailedChunks(),
                        "Matches: " + pending.getTotalMatches(),
                        String.format("Age: %.2fs", pending.getAgeSeconds()),
                        "Run now: /fixbadmod execute --world=" + worldName
                    )
                );
            } else {
                sendSection(
                    context,
                    "PENDING CONFIRMATION",
                    Tone.MUTED,
                    List.of("none")
                );
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
                sendSection(
                    context,
                    "CANCEL",
                    Tone.WARN,
                    List.of("No running job to cancel in world '" + world.getName() + "'")
                );
                return;
            }

            sendSection(
                context,
                "CANCEL",
                Tone.SUCCESS,
                List.of("Cancel requested for world '" + world.getName() + "'")
            );
        }
    }

    private final class ReloadSubCommand extends CommandBase {
        private ReloadSubCommand() {
            super("reload", "Reload FixBadMod config from disk");
        }

        @Override
        protected void executeSync(CommandContext context) {
            FixBadModCommand.this.plugin.reloadConfig();
            sendSection(
                context,
                "RELOAD COMPLETE",
                Tone.SUCCESS,
                List.of(
                    "Config: " + FixBadModCommand.this.plugin.getConfigPath().toAbsolutePath(),
                    "Templates: " + FixBadModCommand.this.plugin.getTemplatesPath().toAbsolutePath()
                )
            );
        }
    }
}
