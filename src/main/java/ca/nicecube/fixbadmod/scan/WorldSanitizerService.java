package ca.nicecube.fixbadmod.scan;

import ca.nicecube.fixbadmod.config.FixBadModConfig;
import com.hypixel.hytale.component.Holder;
import com.hypixel.hytale.component.Ref;
import com.hypixel.hytale.component.RemoveReason;
import com.hypixel.hytale.component.Store;
import com.hypixel.hytale.logger.HytaleLogger;
import com.hypixel.hytale.math.util.ChunkUtil;
import com.hypixel.hytale.server.core.asset.type.blocktype.config.BlockType;
import com.hypixel.hytale.server.core.entity.entities.BlockEntity;
import com.hypixel.hytale.server.core.inventory.ItemStack;
import com.hypixel.hytale.server.core.inventory.container.ItemContainer;
import com.hypixel.hytale.server.core.universe.world.World;
import com.hypixel.hytale.server.core.universe.world.chunk.BlockComponentChunk;
import com.hypixel.hytale.server.core.universe.world.chunk.EntityChunk;
import com.hypixel.hytale.server.core.universe.world.chunk.WorldChunk;
import com.hypixel.hytale.server.core.universe.world.meta.BlockState;
import com.hypixel.hytale.server.core.universe.world.meta.state.ItemContainerBlockState;
import com.hypixel.hytale.server.core.universe.world.storage.ChunkStore;
import com.hypixel.hytale.server.core.universe.world.storage.EntityStore;
import com.hypixel.hytale.server.core.universe.world.storage.GetChunkFlags;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class WorldSanitizerService {
    public enum JobMode {
        SCAN,
        APPLY
    }

    private static final Pattern CHUNK_FILE_PATTERN = Pattern.compile("^(-?\\d+)\\.(-?\\d+)\\.region\\.bin$");
    private static final Pattern UNKNOWN_KEY_PATTERN = Pattern.compile("Unknown key!\\s*([^\\s]+)");
    private static final Set<String> DELETE_REPLACEMENTS = Set.of(
        "__DELETE__",
        "DELETE",
        "REMOVE",
        "EMPTY",
        "AIR",
        "NONE",
        "NULL"
    );

    private final HytaleLogger logger;
    private final Map<String, ScanJob> jobs = new ConcurrentHashMap<>();
    private final Map<String, PendingScan> pendingScans = new ConcurrentHashMap<>();

    public WorldSanitizerService(HytaleLogger logger) {
        this.logger = logger;
    }

    public StartResult startScan(World world, FixBadModConfig config) {
        return this.startInternal(world, config, JobMode.SCAN);
    }

    public StartResult executePending(World world, FixBadModConfig config) {
        Objects.requireNonNull(world, "world");
        Objects.requireNonNull(config, "config");

        String worldName = world.getName();
        ScanJob existing = this.jobs.get(worldName);
        if (existing != null && existing.isRunning()) {
            return StartResult.notStarted("A job is already running in world '" + worldName + "'");
        }

        PendingScan pending = this.pendingScans.get(worldName);
        if (pending == null) {
            return StartResult.notStarted(
                "No confirmed dry-scan for world '" + worldName + "'. Run /fixbadmod scan --world=" + worldName +
                    " first."
            );
        }

        List<CompiledRule> compiledRules = this.compileRules(config);
        if (compiledRules.isEmpty()) {
            return StartResult.notStarted("No valid replacement rule in config. Nothing to run.");
        }

        String currentSignature = this.buildRulesSignature(compiledRules);
        if (!currentSignature.equals(pending.getRulesSignature())) {
            return StartResult.notStarted(
                "Config changed since last scan in world '" + worldName + "'. Re-run /fixbadmod scan --world=" +
                    worldName + " first."
            );
        }

        List<Long> chunkIndexes;
        try {
            chunkIndexes = this.discoverChunkIndexes(world);
        } catch (IOException e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed to enumerate chunk files for world '%s'",
                worldName
            );
            return StartResult.notStarted("Failed to read chunk list for world '" + worldName + "'. Check logs.");
        }

        if (chunkIndexes.isEmpty()) {
            return StartResult.notStarted("No chunk files found in world '" + worldName + "'");
        }

        ScanJob job = new ScanJob(
            world,
            chunkIndexes,
            compiledRules,
            config.getScan().getMaxReplacementsPerChunk(),
            JobMode.APPLY,
            currentSignature
        );
        this.jobs.put(worldName, job);
        this.logger.atInfo().log(
            "[FixBadMod] Started APPLY in world '%s' with %d chunks and %d rules",
            worldName,
            chunkIndexes.size(),
            compiledRules.size()
        );
        this.pump(job);

        return StartResult.started(
            "Execute started in world '" + worldName + "' (" + chunkIndexes.size() + " chunks queued)"
        );
    }

    public JobStatus getStatus(String worldName) {
        ScanJob job = this.jobs.get(worldName);
        if (job == null) {
            return null;
        }

        return job.snapshot();
    }

    public PendingScan getPendingScan(String worldName) {
        return this.pendingScans.get(worldName);
    }

    public boolean cancel(String worldName, String reason) {
        ScanJob job = this.jobs.get(worldName);
        if (job == null || !job.isRunning()) {
            return false;
        }

        job.cancel(reason);
        return true;
    }

    public void cancelAll(String reason) {
        for (ScanJob job : this.jobs.values()) {
            if (job.isRunning()) {
                job.cancel(reason);
            }
        }
    }

    private StartResult startInternal(World world, FixBadModConfig config, JobMode mode) {
        Objects.requireNonNull(world, "world");
        Objects.requireNonNull(config, "config");

        String worldName = world.getName();
        ScanJob existing = this.jobs.get(worldName);
        if (existing != null && existing.isRunning()) {
            return StartResult.notStarted("A job is already running in world '" + worldName + "'");
        }

        List<CompiledRule> compiledRules = this.compileRules(config);
        if (compiledRules.isEmpty()) {
            return StartResult.notStarted("No valid replacement rule in config. Nothing to run.");
        }

        List<Long> chunkIndexes;
        try {
            chunkIndexes = this.discoverChunkIndexes(world);
        } catch (IOException e) {
            this.logger.atWarning().withCause(e).log(
                "[FixBadMod] Failed to enumerate chunk files for world '%s'",
                worldName
            );
            return StartResult.notStarted("Failed to read chunk list for world '" + worldName + "'. Check logs.");
        }

        if (chunkIndexes.isEmpty()) {
            return StartResult.notStarted("No chunk files found in world '" + worldName + "'");
        }

        String rulesSignature = this.buildRulesSignature(compiledRules);
        if (mode == JobMode.SCAN) {
            this.pendingScans.remove(worldName);
        }

        ScanJob job = new ScanJob(
            world,
            chunkIndexes,
            compiledRules,
            config.getScan().getMaxReplacementsPerChunk(),
            mode,
            rulesSignature
        );
        this.jobs.put(worldName, job);
        this.logger.atInfo().log(
            "[FixBadMod] Started %s in world '%s' with %d chunks and %d rules",
            mode,
            worldName,
            chunkIndexes.size(),
            compiledRules.size()
        );
        this.pump(job);

        if (mode == JobMode.SCAN) {
            return StartResult.started(
                "Dry-scan started in world '" + worldName + "' (" + chunkIndexes.size() + " chunks queued)"
            );
        }

        return StartResult.started(
            "Execute started in world '" + worldName + "' (" + chunkIndexes.size() + " chunks queued)"
        );
    }

    private void pump(ScanJob job) {
        long chunkIndex;
        synchronized (job.lock) {
            if (!job.running) {
                return;
            }

            if (job.processing) {
                return;
            }

            Long next = job.queue.pollFirst();
            if (next == null) {
                this.finish(job);
                return;
            }

            job.processing = true;
            chunkIndex = next;
        }

        job.world.getChunkStore()
            .getChunkReferenceAsync(chunkIndex, GetChunkFlags.NO_GENERATE)
            .whenComplete((chunkRef, loadError) -> {
                if (loadError != null) {
                    this.onChunkComplete(job, chunkIndex, ChunkScanResult.failed(loadError));
                    return;
                }

                if (chunkRef == null) {
                    this.onChunkComplete(job, chunkIndex, ChunkScanResult.missing());
                    return;
                }

                job.world.execute(() -> {
                    try {
                        ChunkScanResult result = this.scanChunk(job, chunkIndex);
                        this.onChunkComplete(job, chunkIndex, result);
                    } catch (Throwable scanError) {
                        this.onChunkComplete(job, chunkIndex, ChunkScanResult.failed(scanError));
                    }
                });
            });
    }

    private void onChunkComplete(ScanJob job, long chunkIndex, ChunkScanResult result) {
        synchronized (job.lock) {
            job.processing = false;
            job.processedChunks += 1;
            job.totalMatches += result.matches;

            if (result.failed) {
                job.failedChunks += 1;
                this.logger.atWarning().withCause(result.error).log(
                    "[FixBadMod] Failed scanning chunk %d,%d in world '%s'",
                    ChunkUtil.xOfChunkIndex(chunkIndex),
                    ChunkUtil.zOfChunkIndex(chunkIndex),
                    job.world.getName()
                );
            } else if (result.touched) {
                job.touchedChunks += 1;
            }

            if (job.cancelRequested) {
                this.finish(job);
                return;
            }
        }

        this.pump(job);
    }

    private void finish(ScanJob job) {
        synchronized (job.lock) {
            if (!job.running) {
                return;
            }

            job.running = false;
            job.processing = false;
            job.finishedAt = Instant.now();
        }

        JobStatus snapshot = job.snapshot();
        if (snapshot.isCancelled()) {
            this.logger.atInfo().log(
                "[FixBadMod] %s cancelled in world '%s' after %d/%d chunks, matches=%d",
                snapshot.getMode(),
                snapshot.getWorldName(),
                snapshot.getProcessedChunks(),
                snapshot.getTotalChunks(),
                snapshot.getTotalMatches()
            );
            return;
        }

        if (snapshot.getMode() == JobMode.SCAN) {
            PendingScan pending = new PendingScan(
                snapshot.getWorldName(),
                snapshot.getTotalChunks(),
                snapshot.getTouchedChunks(),
                snapshot.getFailedChunks(),
                snapshot.getTotalMatches(),
                job.rulesSignature,
                Instant.now()
            );
            this.pendingScans.put(snapshot.getWorldName(), pending);
            this.logger.atInfo().log(
                "[FixBadMod] Dry-scan completed in world '%s': chunks=%d touched=%d failed=%d matches=%d (%ss). " +
                    "Awaiting confirmation: /fixbadmod execute --world=%s",
                snapshot.getWorldName(),
                snapshot.getProcessedChunks(),
                snapshot.getTouchedChunks(),
                snapshot.getFailedChunks(),
                snapshot.getTotalMatches(),
                String.format("%.2f", snapshot.getElapsedSeconds()),
                snapshot.getWorldName()
            );
            String matchSummary = this.buildMatchSummary(job.matchBreakdown, 20);
            if (!matchSummary.isBlank()) {
                this.logger.atInfo().log(
                    "[FixBadMod] SCAN match breakdown for world '%s': %s",
                    snapshot.getWorldName(),
                    matchSummary
                );
            }
        } else {
            this.pendingScans.remove(snapshot.getWorldName());
            this.logger.atInfo().log(
                "[FixBadMod] APPLY completed in world '%s': chunks=%d touched=%d failed=%d replaced=%d (%ss)",
                snapshot.getWorldName(),
                snapshot.getProcessedChunks(),
                snapshot.getTouchedChunks(),
                snapshot.getFailedChunks(),
                snapshot.getTotalMatches(),
                String.format("%.2f", snapshot.getElapsedSeconds())
            );
            String matchSummary = this.buildMatchSummary(job.matchBreakdown, 20);
            if (!matchSummary.isBlank()) {
                this.logger.atInfo().log(
                    "[FixBadMod] APPLY match breakdown for world '%s': %s",
                    snapshot.getWorldName(),
                    matchSummary
                );
            }
        }
    }

    private ChunkScanResult scanChunk(ScanJob job, long chunkIndex) {
        WorldChunk chunk = job.world.getChunkStore().getChunkComponent(chunkIndex, WorldChunk.getComponentType());
        if (chunk == null) {
            return ChunkScanResult.missing();
        }

        Map<String, CompiledRule> replacementCache = new HashMap<>();
        boolean[] chunkModified = new boolean[] {false};

        int matches = this.scanEntityChunkBlockEntities(job, chunk, replacementCache, chunkModified);
        int maxReplacements = job.maxReplacementsPerChunk;
        if (maxReplacements > 0 && matches >= maxReplacements) {
            if (job.mode == JobMode.APPLY && chunkModified[0]) {
                chunk.markNeedsSaving();
                BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
                if (blockComponentChunk != null) {
                    blockComponentChunk.markNeedsSaving();
                }
                EntityChunk entityChunk = chunk.getEntityChunk();
                if (entityChunk != null) {
                    entityChunk.markNeedsSaving();
                }
            }
            return ChunkScanResult.touched(matches);
        }

        matches += this.scanBlockComponentEntities(job, chunk, replacementCache, chunkModified);
        maxReplacements = job.maxReplacementsPerChunk;
        if (maxReplacements > 0 && matches >= maxReplacements) {
            if (job.mode == JobMode.APPLY && chunkModified[0]) {
                chunk.markNeedsSaving();
                BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
                if (blockComponentChunk != null) {
                    blockComponentChunk.markNeedsSaving();
                }
                EntityChunk entityChunk = chunk.getEntityChunk();
                if (entityChunk != null) {
                    entityChunk.markNeedsSaving();
                }
            }
            return ChunkScanResult.touched(matches);
        }

        for (int localX = 0; localX < ChunkUtil.SIZE; localX++) {
            for (int localZ = 0; localZ < ChunkUtil.SIZE; localZ++) {
                for (int y = ChunkUtil.MIN_Y; y < ChunkUtil.MIN_Y + ChunkUtil.HEIGHT; y++) {
                    int blockTypeIndex = chunk.getBlock(localX, y, localZ);
                    if (blockTypeIndex <= BlockType.EMPTY_ID) {
                        continue;
                    }

                    matches += this.scanItemContainerState(job, chunk, localX, y, localZ, replacementCache, chunkModified);
                    if (maxReplacements > 0 && matches >= maxReplacements) {
                        if (job.mode == JobMode.APPLY && chunkModified[0]) {
                            chunk.markNeedsSaving();
                            BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
                            if (blockComponentChunk != null) {
                                blockComponentChunk.markNeedsSaving();
                            }
                            EntityChunk entityChunk = chunk.getEntityChunk();
                            if (entityChunk != null) {
                                entityChunk.markNeedsSaving();
                            }
                        }
                        return ChunkScanResult.touched(matches);
                    }

                    BlockType blockType = BlockType.getAssetMap().getAsset(blockTypeIndex);
                    String blockId = "";
                    boolean unknownBlock = blockType == null || blockType.isUnknown();
                    if (blockType != null && !unknownBlock) {
                        String resolvedId = blockType.getId();
                        if (resolvedId != null) {
                            blockId = resolvedId;
                        }
                    }
                    if (blockId.isBlank()) {
                        blockId = this.resolveBlockIdFromState(chunk, localX, y, localZ);
                    }
                    if (blockId == null || blockId.isBlank()) {
                        if (!unknownBlock) {
                            continue;
                        }

                        // Fallback for unresolved unknown palette entries: remove the block to stabilize chunk loading.
                        if (job.mode == JobMode.APPLY) {
                            boolean componentRemoved = this.clearBlockComponentAt(chunk, localX, y, localZ);
                            boolean blockChanged = chunk.setBlock(localX, y, localZ, BlockType.EMPTY.getId());
                            if (componentRemoved || blockChanged) {
                                chunkModified[0] = true;
                            }
                        }
                        this.recordMatch(job, "unknown-block");
                        matches += 1;

                        if (maxReplacements > 0 && matches >= maxReplacements) {
                            if (job.mode == JobMode.APPLY && chunkModified[0]) {
                                chunk.markNeedsSaving();
                                BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
                                if (blockComponentChunk != null) {
                                    blockComponentChunk.markNeedsSaving();
                                }
                                EntityChunk entityChunk = chunk.getEntityChunk();
                                if (entityChunk != null) {
                                    entityChunk.markNeedsSaving();
                                }
                            }
                            return ChunkScanResult.touched(matches);
                        }
                        continue;
                    }

                    CompiledRule replacementRule = replacementCache.computeIfAbsent(
                        blockId,
                        id -> this.resolveRule(id, job.rules)
                    );
                    if (replacementRule == null ||
                        (!replacementRule.isDelete() && replacementRule.getReplacementId().equals(blockId))) {
                        continue;
                    }

                    if (job.mode == JobMode.APPLY) {
                        boolean componentRemoved = this.clearBlockComponentAt(chunk, localX, y, localZ);
                        boolean blockChanged;
                        if (replacementRule.isDelete()) {
                            blockChanged = chunk.setBlock(localX, y, localZ, BlockType.EMPTY.getId());
                        } else {
                            blockChanged = chunk.setBlock(localX, y, localZ, replacementRule.getReplacementId());
                        }

                        if (componentRemoved || blockChanged) {
                            chunkModified[0] = true;
                        }
                    }
                    this.recordMatch(job, "block:" + blockId);
                    matches += 1;

                    if (maxReplacements > 0 && matches >= maxReplacements) {
                        if (job.mode == JobMode.APPLY && chunkModified[0]) {
                            chunk.markNeedsSaving();
                            BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
                            if (blockComponentChunk != null) {
                                blockComponentChunk.markNeedsSaving();
                            }
                            EntityChunk entityChunk = chunk.getEntityChunk();
                            if (entityChunk != null) {
                                entityChunk.markNeedsSaving();
                            }
                        }
                        return ChunkScanResult.touched(matches);
                    }
                }
            }
        }

        if (job.mode == JobMode.APPLY && chunkModified[0]) {
            chunk.markNeedsSaving();
            BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
            if (blockComponentChunk != null) {
                blockComponentChunk.markNeedsSaving();
            }
            EntityChunk entityChunk = chunk.getEntityChunk();
            if (entityChunk != null) {
                entityChunk.markNeedsSaving();
            }
        }

        if (matches > 0) {
            return ChunkScanResult.touched(matches);
        }

        return ChunkScanResult.missing();
    }

    private int scanItemContainerState(
        ScanJob job,
        WorldChunk chunk,
        int localX,
        int y,
        int localZ,
        Map<String, CompiledRule> replacementCache,
        boolean[] chunkModified
    ) {
        if (chunk.getBlockComponentEntity(localX, y, localZ) == null &&
            chunk.getBlockComponentHolder(localX, y, localZ) == null) {
            return 0;
        }

        BlockState blockState;
        try {
            blockState = chunk.getState(localX, y, localZ);
        } catch (Throwable error) {
            String unknownKey = this.extractUnknownKey(error);
            if (unknownKey.isBlank()) {
                return 0;
            }

            String cacheKey = "state-error|" + unknownKey;
            CompiledRule replacementRule = replacementCache.computeIfAbsent(
                cacheKey,
                id -> this.resolveRuleOrAutoDeleteUnknown(unknownKey, job.rules)
            );

            if (job.mode == JobMode.APPLY) {
                boolean componentRemoved = this.clearBlockComponentAt(chunk, localX, y, localZ);
                boolean blockChanged;
                if (replacementRule.isDelete()) {
                    blockChanged = chunk.setBlock(localX, y, localZ, BlockType.EMPTY.getId());
                } else {
                    blockChanged = chunk.setBlock(localX, y, localZ, replacementRule.getReplacementId());
                }
                if (componentRemoved || blockChanged) {
                    chunkModified[0] = true;
                }
            }

            this.recordMatch(job, "state-error:" + unknownKey);
            return 1;
        }

        if (blockState == null) {
            return 0;
        }

        if (!(blockState instanceof ItemContainerBlockState itemContainerBlockState)) {
            return 0;
        }

        ItemContainer container = itemContainerBlockState.getItemContainer();
        if (container == null) {
            return 0;
        }

        int matches = 0;
        boolean removeContainerBlock = false;
        short capacity = container.getCapacity();
        for (short slot = 0; slot < capacity; slot++) {
            ItemStack itemStack;
            try {
                itemStack = container.getItemStack(slot);
            } catch (Throwable ignored) {
                continue;
            }

            if (itemStack == null || itemStack.isEmpty()) {
                continue;
            }

            String itemId = itemStack.getItemId();
            if (itemId == null || itemId.isBlank()) {
                continue;
            }

            String cacheKey = "item|" + itemId;
            CompiledRule replacementRule = replacementCache.computeIfAbsent(
                cacheKey,
                id -> this.resolveRule(itemId, job.rules)
            );
            if (replacementRule == null) {
                continue;
            }

            this.recordMatch(job, "item:" + itemId);
            matches += 1;
            if (replacementRule.isDelete()) {
                removeContainerBlock = true;
            } else if (job.mode == JobMode.APPLY) {
                container.setItemStackForSlot(slot, ItemStack.EMPTY);
                chunkModified[0] = true;
            }
        }

        if (job.mode == JobMode.APPLY && removeContainerBlock) {
            boolean componentRemoved = this.clearBlockComponentAt(chunk, localX, y, localZ);
            boolean blockChanged = chunk.setBlock(localX, y, localZ, BlockType.EMPTY.getId());
            if (componentRemoved || blockChanged) {
                chunkModified[0] = true;
            }
        }

        return matches;
    }

    private int scanBlockComponentEntities(
        ScanJob job,
        WorldChunk chunk,
        Map<String, CompiledRule> replacementCache,
        boolean[] chunkModified
    ) {
        BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
        if (blockComponentChunk == null) {
            return 0;
        }

        List<Integer> holdersToRemove = new ArrayList<>();
        List<Integer> refsToRemove = new ArrayList<>();
        List<Ref<ChunkStore>> refsToRemoveHandle = new ArrayList<>();
        int matches = 0;

        for (Int2ObjectMap.Entry<Holder<ChunkStore>> entry : blockComponentChunk.getEntityHolders().int2ObjectEntrySet()) {
            CompiledRule replacementRule = this.resolveRuleFromHolder(job, entry.getValue(), replacementCache, job.rules);
            if (replacementRule == null) {
                continue;
            }

            matches += 1;
            if (job.mode == JobMode.APPLY) {
                holdersToRemove.add(entry.getIntKey());
            }
        }

        for (Int2ObjectMap.Entry<Ref<ChunkStore>> entry : blockComponentChunk.getEntityReferences().int2ObjectEntrySet()) {
            Ref<ChunkStore> entityRef = entry.getValue();
            CompiledRule replacementRule = this.resolveRuleFromReference(job, entityRef, replacementCache, job.rules);
            if (replacementRule == null) {
                continue;
            }

            matches += 1;
            if (job.mode == JobMode.APPLY) {
                refsToRemove.add(entry.getIntKey());
                refsToRemoveHandle.add(entityRef);
            }
        }

        if (job.mode == JobMode.APPLY) {
            for (Integer key : holdersToRemove) {
                try {
                    blockComponentChunk.removeEntityHolder(key.intValue());
                    chunkModified[0] = true;
                } catch (Throwable ignored) {
                    // Best effort: if a malformed holder cannot be removed, continue with the rest.
                }
            }

            for (int i = 0; i < refsToRemove.size(); i++) {
                int key = refsToRemove.get(i);
                Ref<ChunkStore> entityRef = refsToRemoveHandle.get(i);

                try {
                    @SuppressWarnings("rawtypes")
                    Store store = ((Ref) entityRef).getStore();
                    @SuppressWarnings("rawtypes")
                    Ref rawRef = (Ref) entityRef;
                    store.removeEntity(rawRef, RemoveReason.REMOVE);
                } catch (Throwable ignored) {
                    // If entity removal fails, we still try to detach it from the chunk component map.
                }

                try {
                    blockComponentChunk.removeEntityReference(key, entityRef);
                    chunkModified[0] = true;
                } catch (Throwable ignored) {
                    // Best effort: continue.
                }
            }
        }

        return matches;
    }

    private int scanEntityChunkBlockEntities(
        ScanJob job,
        WorldChunk chunk,
        Map<String, CompiledRule> replacementCache,
        boolean[] chunkModified
    ) {
        EntityChunk entityChunk = chunk.getEntityChunk();
        if (entityChunk == null) {
            return 0;
        }

        Store<EntityStore> entityStore = job.world.getEntityStore().getStore();
        List<Ref<EntityStore>> refsToRemove = new ArrayList<>();
        List<Holder<EntityStore>> holdersToRemove = new ArrayList<>();
        int matches = 0;

        for (Ref<EntityStore> entityRef : entityChunk.getEntityReferences()) {
            CompiledRule replacementRule = this.resolveRuleFromEntityRef(
                job,
                entityRef,
                entityStore,
                replacementCache,
                job.rules
            );
            if (replacementRule == null) {
                continue;
            }

            matches += 1;
            if (job.mode == JobMode.APPLY) {
                refsToRemove.add(entityRef);
            }
        }

        for (Holder<EntityStore> holder : entityChunk.getEntityHolders()) {
            CompiledRule replacementRule = this.resolveRuleFromEntityHolder(job, holder, replacementCache, job.rules);
            if (replacementRule == null) {
                continue;
            }

            matches += 1;
            if (job.mode == JobMode.APPLY) {
                holdersToRemove.add(holder);
            }
        }

        if (job.mode == JobMode.APPLY) {
            for (Ref<EntityStore> entityRef : refsToRemove) {
                try {
                    entityStore.removeEntity(entityRef, RemoveReason.REMOVE);
                } catch (Throwable ignored) {
                    // Continue and attempt to detach reference from the entity chunk.
                }

                try {
                    entityChunk.removeEntityReference(entityRef);
                    chunkModified[0] = true;
                } catch (Throwable ignored) {
                    // Best effort.
                }
            }

            if (!holdersToRemove.isEmpty()) {
                try {
                    Set<Holder<EntityStore>> holdersToRemoveSet = new HashSet<>(holdersToRemove);
                    Holder<EntityStore>[] allHolders = entityChunk.takeEntityHolders();
                    if (allHolders != null) {
                        for (Holder<EntityStore> holder : allHolders) {
                            if (holder == null) {
                                continue;
                            }

                            if (holdersToRemoveSet.contains(holder)) {
                                chunkModified[0] = true;
                                continue;
                            }

                            entityChunk.storeEntityHolder(holder);
                        }
                    }
                } catch (Throwable ignored) {
                    // Best effort.
                }
            }
        }

        return matches;
    }

    private CompiledRule resolveRuleFromHolder(
        ScanJob job,
        Holder<ChunkStore> holder,
        Map<String, CompiledRule> replacementCache,
        List<CompiledRule> rules
    ) {
        if (holder == null) {
            return null;
        }

        try {
            BlockState blockState = BlockState.getBlockState(holder);
            String blockId = this.extractBlockIdFromState(blockState);
            if (blockId.isBlank()) {
                return null;
            }

            String cacheKey = "holder|" + blockId;
            CompiledRule rule = replacementCache.computeIfAbsent(cacheKey, id -> this.resolveRule(blockId, rules));
            if (rule != null) {
                this.recordMatch(job, "blockcomponent-holder:" + blockId);
            }
            return rule;
        } catch (Throwable error) {
            String unknownKey = this.extractUnknownKey(error);
            if (unknownKey.isBlank()) {
                return null;
            }

            String cacheKey = "holder-unknown|" + unknownKey;
            CompiledRule rule = replacementCache.computeIfAbsent(
                cacheKey,
                id -> this.resolveRuleOrAutoDeleteUnknown(unknownKey, rules)
            );
            if (rule != null) {
                this.recordMatch(job, "blockcomponent-holder-unknown:" + unknownKey);
            }
            return rule;
        }
    }

    private CompiledRule resolveRuleFromEntityRef(
        ScanJob job,
        Ref<EntityStore> entityRef,
        Store<EntityStore> entityStore,
        Map<String, CompiledRule> replacementCache,
        List<CompiledRule> rules
    ) {
        if (entityRef == null) {
            return null;
        }

        try {
            BlockEntity blockEntity = entityStore.getComponent(entityRef, BlockEntity.getComponentType());
            if (blockEntity == null) {
                return null;
            }

            String blockId = blockEntity.getBlockTypeKey();
            if (blockId == null || blockId.isBlank()) {
                return null;
            }

            String cacheKey = "entity-ref|" + blockId;
            CompiledRule rule = replacementCache.computeIfAbsent(cacheKey, id -> this.resolveRule(blockId, rules));
            if (rule != null) {
                this.recordMatch(job, "entity-ref:" + blockId);
            }
            return rule;
        } catch (Throwable error) {
            String unknownKey = this.extractUnknownKey(error);
            if (unknownKey.isBlank()) {
                return null;
            }

            String cacheKey = "entity-ref-unknown|" + unknownKey;
            CompiledRule rule = replacementCache.computeIfAbsent(
                cacheKey,
                id -> this.resolveRuleOrAutoDeleteUnknown(unknownKey, rules)
            );
            if (rule != null) {
                this.recordMatch(job, "entity-ref-unknown:" + unknownKey);
            }
            return rule;
        }
    }

    private CompiledRule resolveRuleFromEntityHolder(
        ScanJob job,
        Holder<EntityStore> holder,
        Map<String, CompiledRule> replacementCache,
        List<CompiledRule> rules
    ) {
        if (holder == null) {
            return null;
        }

        try {
            BlockEntity blockEntity = holder.getComponent(BlockEntity.getComponentType());
            if (blockEntity == null) {
                return null;
            }

            String blockId = blockEntity.getBlockTypeKey();
            if (blockId == null || blockId.isBlank()) {
                return null;
            }

            String cacheKey = "entity-holder|" + blockId;
            CompiledRule rule = replacementCache.computeIfAbsent(cacheKey, id -> this.resolveRule(blockId, rules));
            if (rule != null) {
                this.recordMatch(job, "entity-holder:" + blockId);
            }
            return rule;
        } catch (Throwable error) {
            String unknownKey = this.extractUnknownKey(error);
            if (unknownKey.isBlank()) {
                return null;
            }

            String cacheKey = "entity-holder-unknown|" + unknownKey;
            CompiledRule rule = replacementCache.computeIfAbsent(
                cacheKey,
                id -> this.resolveRuleOrAutoDeleteUnknown(unknownKey, rules)
            );
            if (rule != null) {
                this.recordMatch(job, "entity-holder-unknown:" + unknownKey);
            }
            return rule;
        }
    }

    private CompiledRule resolveRuleFromReference(
        ScanJob job,
        Ref<ChunkStore> entityRef,
        Map<String, CompiledRule> replacementCache,
        List<CompiledRule> rules
    ) {
        if (entityRef == null) {
            return null;
        }

        try {
            BlockState blockState = BlockState.getBlockState(entityRef, entityRef.getStore());
            String blockId = this.extractBlockIdFromState(blockState);
            if (blockId.isBlank()) {
                return null;
            }

            String cacheKey = "ref|" + blockId;
            CompiledRule rule = replacementCache.computeIfAbsent(cacheKey, id -> this.resolveRule(blockId, rules));
            if (rule != null) {
                this.recordMatch(job, "blockcomponent-ref:" + blockId);
            }
            return rule;
        } catch (Throwable error) {
            String unknownKey = this.extractUnknownKey(error);
            if (unknownKey.isBlank()) {
                return null;
            }

            String cacheKey = "ref-unknown|" + unknownKey;
            CompiledRule rule = replacementCache.computeIfAbsent(
                cacheKey,
                id -> this.resolveRuleOrAutoDeleteUnknown(unknownKey, rules)
            );
            if (rule != null) {
                this.recordMatch(job, "blockcomponent-ref-unknown:" + unknownKey);
            }
            return rule;
        }
    }

    private CompiledRule resolveRuleOrAutoDeleteUnknown(String unknownKey, List<CompiledRule> rules) {
        CompiledRule configuredRule = this.resolveRule(unknownKey, rules);
        if (configuredRule != null) {
            return configuredRule;
        }

        return this.autoDeleteRule(unknownKey);
    }

    private CompiledRule autoDeleteRule(String unknownKey) {
        return new CompiledRule(RuleMode.EXACT, unknownKey, BlockType.EMPTY.getId(), true);
    }

    private String extractBlockIdFromState(BlockState blockState) {
        if (blockState == null) {
            return "";
        }

        try {
            BlockType blockType = blockState.getBlockType();
            if (blockType == null) {
                return "";
            }

            String blockId = blockType.getId();
            if (blockId == null || blockId.isBlank()) {
                return "";
            }

            return blockId;
        } catch (Throwable ignored) {
            return "";
        }
    }

    private String resolveBlockIdFromState(WorldChunk chunk, int localX, int y, int localZ) {
        BlockState blockState;
        try {
            blockState = chunk.getState(localX, y, localZ);
        } catch (Throwable error) {
            String unknownKey = this.extractUnknownKey(error);
            if (!unknownKey.isBlank()) {
                return unknownKey;
            }
            return "";
        }

        return this.extractBlockIdFromState(blockState);
    }

    private String extractUnknownKey(Throwable throwable) {
        Throwable cursor = throwable;
        while (cursor != null) {
            String message = cursor.getMessage();
            if (message != null) {
                Matcher matcher = UNKNOWN_KEY_PATTERN.matcher(message);
                if (matcher.find()) {
                    String key = matcher.group(1);
                    if (key != null) {
                        key = key.trim();
                        if (!key.isBlank()) {
                            return key;
                        }
                    }
                }
            }

            cursor = cursor.getCause();
        }

        return "";
    }

    private boolean clearBlockComponentAt(WorldChunk chunk, int localX, int y, int localZ) {
        BlockComponentChunk blockComponentChunk = chunk.getBlockComponentChunk();
        if (blockComponentChunk == null) {
            return false;
        }

        int blockIndex = ChunkUtil.indexBlockInColumn(localX, y, localZ);
        boolean changed = false;

        Holder<ChunkStore> holder = blockComponentChunk.getEntityHolder(blockIndex);
        if (holder != null) {
            try {
                blockComponentChunk.removeEntityHolder(blockIndex);
                changed = true;
            } catch (Throwable ignored) {
                // Best effort.
            }
        }

        Ref<ChunkStore> entityRef = blockComponentChunk.getEntityReference(blockIndex);
        if (entityRef != null) {
            try {
                @SuppressWarnings("rawtypes")
                Store store = ((Ref) entityRef).getStore();
                @SuppressWarnings("rawtypes")
                Ref rawRef = (Ref) entityRef;
                store.removeEntity(rawRef, RemoveReason.REMOVE);
            } catch (Throwable ignored) {
                // Continue and attempt to detach from chunk map.
            }

            try {
                blockComponentChunk.removeEntityReference(blockIndex, entityRef);
                changed = true;
            } catch (Throwable ignored) {
                // Best effort.
            }
        }

        return changed;
    }

    private List<Long> discoverChunkIndexes(World world) throws IOException {
        try {
            LongSet indexes = world.getChunkStore().getChunkIndexes();
            if (indexes != null && !indexes.isEmpty()) {
                List<Long> result = new ArrayList<>(indexes.size());
                for (long index : indexes) {
                    result.add(index);
                }

                result.sort(Comparator.naturalOrder());
                return result;
            }
        } catch (Throwable error) {
            this.logger.atWarning().withCause(error).log(
                "[FixBadMod] Could not fetch chunk indexes from ChunkStore for world '%s'. Falling back to file scan.",
                world.getName()
            );
        }

        Path chunksPath = world.getSavePath().resolve("chunks");
        if (!Files.exists(chunksPath)) {
            return List.of();
        }

        List<Long> result = new ArrayList<>();
        try (Stream<Path> stream = Files.list(chunksPath)) {
            stream.filter(Files::isRegularFile)
                .forEach(path -> {
                    Matcher matcher = CHUNK_FILE_PATTERN.matcher(path.getFileName().toString());
                    if (!matcher.matches()) {
                        return;
                    }

                    int chunkX;
                    int chunkZ;
                    try {
                        chunkX = Integer.parseInt(matcher.group(1));
                        chunkZ = Integer.parseInt(matcher.group(2));
                    } catch (NumberFormatException e) {
                        return;
                    }

                    result.add(ChunkUtil.indexChunk(chunkX, chunkZ));
                });
        }

        result.sort(Comparator.naturalOrder());
        return result;
    }

    private List<CompiledRule> compileRules(FixBadModConfig config) {
        List<CompiledRule> compiled = new ArrayList<>();
        for (FixBadModConfig.Rule rule : config.getRules()) {
            if (rule == null || !rule.isEnabled()) {
                continue;
            }

            String match = rule.getMatch();
            if (match == null || match.isBlank()) {
                continue;
            }

            RuleMode mode = RuleMode.from(rule.getMode());
            String replaceWith = rule.getReplaceWith();
            if (this.isDeleteReplacement(replaceWith)) {
                compiled.add(new CompiledRule(mode, match, BlockType.EMPTY.getId(), true));
                continue;
            }

            BlockType replacement = BlockType.fromString(replaceWith);
            if (replacement == null || replacement.isUnknown()) {
                this.logger.atWarning().log(
                    "[FixBadMod] Skipping rule '%s' because replacement '%s' is unknown",
                    match,
                    replaceWith
                );
                continue;
            }

            compiled.add(new CompiledRule(mode, match, replacement.getId(), false));
        }

        return compiled;
    }

    private String buildRulesSignature(List<CompiledRule> rules) {
        StringBuilder sb = new StringBuilder();
        for (CompiledRule rule : rules) {
            sb.append(rule.mode)
                .append('|')
                .append(rule.match)
                .append('|')
                .append(rule.delete)
                .append('|')
                .append(rule.replacementId)
                .append(';');
        }
        return sb.toString();
    }

    private CompiledRule resolveRule(String blockId, List<CompiledRule> rules) {
        for (CompiledRule rule : rules) {
            if (rule.matches(blockId)) {
                return rule;
            }
        }

        return null;
    }

    private void recordMatch(ScanJob job, String matchedKey) {
        if (job == null) {
            return;
        }

        String key = matchedKey == null ? "" : matchedKey.trim();
        if (key.isBlank()) {
            key = "__UNKNOWN__";
        }

        synchronized (job.lock) {
            job.matchBreakdown.merge(key, 1L, Long::sum);
        }
    }

    private String buildMatchSummary(Map<String, Long> matchBreakdown, int limit) {
        if (matchBreakdown == null || matchBreakdown.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        matchBreakdown.entrySet().stream()
            .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
            .limit(limit)
            .forEachOrdered(entry -> {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append('=').append(entry.getValue());
            });

        return sb.toString();
    }

    private boolean isDeleteReplacement(String replaceWith) {
        if (replaceWith == null) {
            return true;
        }

        String normalized = replaceWith.trim();
        if (normalized.isEmpty()) {
            return true;
        }

        return DELETE_REPLACEMENTS.contains(normalized.toUpperCase(Locale.ROOT));
    }

    public static final class StartResult {
        private final boolean started;
        private final String message;

        private StartResult(boolean started, String message) {
            this.started = started;
            this.message = message;
        }

        public static StartResult started(String message) {
            return new StartResult(true, message);
        }

        public static StartResult notStarted(String message) {
            return new StartResult(false, message);
        }

        public boolean isStarted() {
            return this.started;
        }

        public String getMessage() {
            return this.message;
        }
    }

    public static final class PendingScan {
        private final String worldName;
        private final int totalChunks;
        private final int touchedChunks;
        private final int failedChunks;
        private final long totalMatches;
        private final String rulesSignature;
        private final Instant createdAt;

        private PendingScan(
            String worldName,
            int totalChunks,
            int touchedChunks,
            int failedChunks,
            long totalMatches,
            String rulesSignature,
            Instant createdAt
        ) {
            this.worldName = worldName;
            this.totalChunks = totalChunks;
            this.touchedChunks = touchedChunks;
            this.failedChunks = failedChunks;
            this.totalMatches = totalMatches;
            this.rulesSignature = rulesSignature;
            this.createdAt = createdAt;
        }

        public String getWorldName() {
            return this.worldName;
        }

        public int getTotalChunks() {
            return this.totalChunks;
        }

        public int getTouchedChunks() {
            return this.touchedChunks;
        }

        public int getFailedChunks() {
            return this.failedChunks;
        }

        public long getTotalMatches() {
            return this.totalMatches;
        }

        public String getRulesSignature() {
            return this.rulesSignature;
        }

        public double getAgeSeconds() {
            return Duration.between(this.createdAt, Instant.now()).toMillis() / 1000.0d;
        }
    }

    public static final class JobStatus {
        private final String worldName;
        private final JobMode mode;
        private final boolean running;
        private final boolean cancelled;
        private final int totalChunks;
        private final int processedChunks;
        private final int touchedChunks;
        private final int failedChunks;
        private final long totalMatches;
        private final Instant startedAt;
        private final Instant finishedAt;
        private final String cancelReason;

        private JobStatus(
            String worldName,
            JobMode mode,
            boolean running,
            boolean cancelled,
            int totalChunks,
            int processedChunks,
            int touchedChunks,
            int failedChunks,
            long totalMatches,
            Instant startedAt,
            Instant finishedAt,
            String cancelReason
        ) {
            this.worldName = worldName;
            this.mode = mode;
            this.running = running;
            this.cancelled = cancelled;
            this.totalChunks = totalChunks;
            this.processedChunks = processedChunks;
            this.touchedChunks = touchedChunks;
            this.failedChunks = failedChunks;
            this.totalMatches = totalMatches;
            this.startedAt = startedAt;
            this.finishedAt = finishedAt;
            this.cancelReason = cancelReason;
        }

        public String getWorldName() {
            return this.worldName;
        }

        public JobMode getMode() {
            return this.mode;
        }

        public boolean isRunning() {
            return this.running;
        }

        public boolean isCancelled() {
            return this.cancelled;
        }

        public int getTotalChunks() {
            return this.totalChunks;
        }

        public int getProcessedChunks() {
            return this.processedChunks;
        }

        public int getTouchedChunks() {
            return this.touchedChunks;
        }

        public int getFailedChunks() {
            return this.failedChunks;
        }

        public long getTotalMatches() {
            return this.totalMatches;
        }

        public double getProgressPercent() {
            if (this.totalChunks <= 0) {
                return 0.0d;
            }

            return (this.processedChunks * 100.0d) / this.totalChunks;
        }

        public double getElapsedSeconds() {
            Instant end = this.finishedAt != null ? this.finishedAt : Instant.now();
            return Duration.between(this.startedAt, end).toMillis() / 1000.0d;
        }

        public String getCancelReason() {
            return this.cancelReason;
        }
    }

    private static final class ScanJob {
        private final Object lock = new Object();
        private final World world;
        private final ArrayDeque<Long> queue;
        private final List<CompiledRule> rules;
        private final int maxReplacementsPerChunk;
        private final JobMode mode;
        private final String rulesSignature;
        private final Instant startedAt;

        private boolean running = true;
        private boolean processing;
        private boolean cancelRequested;
        private String cancelReason = "";
        private int processedChunks;
        private int touchedChunks;
        private int failedChunks;
        private long totalMatches;
        private final Map<String, Long> matchBreakdown = new HashMap<>();
        private Instant finishedAt;

        private ScanJob(
            World world,
            List<Long> chunkIndexes,
            List<CompiledRule> rules,
            int maxReplacementsPerChunk,
            JobMode mode,
            String rulesSignature
        ) {
            this.world = world;
            this.queue = new ArrayDeque<>(chunkIndexes);
            this.rules = rules;
            this.maxReplacementsPerChunk = maxReplacementsPerChunk;
            this.mode = mode;
            this.rulesSignature = rulesSignature;
            this.startedAt = Instant.now();
        }

        private boolean isRunning() {
            synchronized (this.lock) {
                return this.running;
            }
        }

        private void cancel(String reason) {
            synchronized (this.lock) {
                if (!this.running) {
                    return;
                }

                this.cancelRequested = true;
                this.cancelReason = reason == null ? "" : reason;
            }
        }

        private JobStatus snapshot() {
            synchronized (this.lock) {
                return new JobStatus(
                    this.world.getName(),
                    this.mode,
                    this.running,
                    this.cancelRequested,
                    this.queue.size() + this.processedChunks + (this.processing ? 1 : 0),
                    this.processedChunks,
                    this.touchedChunks,
                    this.failedChunks,
                    this.totalMatches,
                    this.startedAt,
                    this.finishedAt,
                    this.cancelReason
                );
            }
        }
    }

    private static final class CompiledRule {
        private final RuleMode mode;
        private final String match;
        private final String replacementId;
        private final boolean delete;

        private CompiledRule(RuleMode mode, String match, String replacementId, boolean delete) {
            this.mode = mode;
            this.match = match;
            this.replacementId = replacementId;
            this.delete = delete;
        }

        private boolean matches(String blockId) {
            return this.mode.matches(this.match, blockId);
        }

        private String getReplacementId() {
            return this.replacementId;
        }

        private boolean isDelete() {
            return this.delete;
        }
    }

    private enum RuleMode {
        EXACT,
        PREFIX,
        CONTAINS;

        private static RuleMode from(String mode) {
            if (mode == null) {
                return PREFIX;
            }

            return switch (mode.trim().toLowerCase()) {
                case "exact" -> EXACT;
                case "contains" -> CONTAINS;
                default -> PREFIX;
            };
        }

        private boolean matches(String match, String blockId) {
            return switch (this) {
                case EXACT -> blockId.equals(match);
                case PREFIX -> blockId.startsWith(match);
                case CONTAINS -> blockId.contains(match);
            };
        }
    }

    private static final class ChunkScanResult {
        private final boolean touched;
        private final int matches;
        private final boolean failed;
        private final Throwable error;

        private ChunkScanResult(boolean touched, int matches, boolean failed, Throwable error) {
            this.touched = touched;
            this.matches = matches;
            this.failed = failed;
            this.error = error;
        }

        private static ChunkScanResult touched(int matches) {
            return new ChunkScanResult(true, matches, false, null);
        }

        private static ChunkScanResult missing() {
            return new ChunkScanResult(false, 0, false, null);
        }

        private static ChunkScanResult failed(Throwable error) {
            return new ChunkScanResult(false, 0, true, error);
        }
    }
}
