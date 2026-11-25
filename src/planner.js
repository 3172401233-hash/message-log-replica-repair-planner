/**
 * Message-log replica repair planner.
 *
 * All inputs are *simulated* cluster state (offsets, leader epochs, high
 * watermark, segment summaries and failure events). This module is pure: it
 * never touches disks, networks or a real broker. It produces a structured
 * repair plan.
 */

export class PlannerError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'PlannerError';
    this.code = code;
    this.details = details;
  }
}

const isNonNegativeInt = (value) => Number.isSafeInteger(value) && value >= 0;

function makeRange(startOffset, endOffsetExclusive) {
  if (!isNonNegativeInt(startOffset) || !isNonNegativeInt(endOffsetExclusive)) {
    return null;
  }
  if (endOffsetExclusive <= startOffset) return null;
  return { startOffset, endOffsetExclusive };
}

function segmentFileName(baseOffset) {
  return `segment-${String(baseOffset).padStart(20, '0')}.log`;
}

export function normalizeSegments(rawSegments) {
  if (rawSegments === undefined || rawSegments === null) return [];
  if (!Array.isArray(rawSegments)) {
    throw new PlannerError('INVALID_INPUT', 'replica.segments must be an array when provided');
  }

  const segments = rawSegments.map((raw, index) => {
    if (!raw || typeof raw !== 'object') {
      throw new PlannerError('INVALID_INPUT', `segments[${index}] must be an object`);
    }
    const baseOffset = raw.baseOffset ?? raw.base;
    let endOffsetExclusive = raw.endOffsetExclusive ?? raw.endOffset;
    if (endOffsetExclusive === undefined && raw.lastOffset !== undefined) {
      endOffsetExclusive = raw.lastOffset + 1;
    }
    if (!isNonNegativeInt(baseOffset)) {
      throw new PlannerError('INVALID_INPUT', `segments[${index}].baseOffset must be a non-negative safe integer`);
    }
    if (!isNonNegativeInt(endOffsetExclusive) || endOffsetExclusive <= baseOffset) {
      throw new PlannerError(
        'INVALID_INPUT',
        `segments[${index}] must resolve to baseOffset < endOffsetExclusive`,
      );
    }
    return {
      baseOffset,
      endOffsetExclusive,
      checksum: raw.checksum ?? raw.crc ?? null,
      sizeBytes: Number.isSafeInteger(raw.sizeBytes) ? raw.sizeBytes : null,
    };
  });

  segments.sort((a, b) => a.baseOffset - b.baseOffset);
  for (let i = 1; i < segments.length; i += 1) {
    if (segments[i].baseOffset < segments[i - 1].endOffsetExclusive) {
      throw new PlannerError('INVALID_INPUT', 'segment summaries must not overlap', {
        first: segments[i - 1],
        second: segments[i],
      });
    }
  }
  return segments;
}

export function normalizeCheckpoint(rawCheckpoint, leaderEpoch) {
  let checkpoint;
  if (Array.isArray(rawCheckpoint) && rawCheckpoint.length > 0) {
    checkpoint = rawCheckpoint.map((entry, index) => {
      const epoch = entry.epoch;
      const startOffset = entry.startOffset ?? entry.start;
      if (!Number.isSafeInteger(epoch) || epoch < 0) {
        throw new PlannerError('INVALID_INPUT', `epochCheckpoint[${index}].epoch must be a non-negative integer`);
      }
      if (!isNonNegativeInt(startOffset)) {
        throw new PlannerError('INVALID_INPUT', `epochCheckpoint[${index}].startOffset invalid`);
      }
      return { epoch, startOffset };
    });
    checkpoint.sort((a, b) => a.epoch - b.epoch);
    for (let i = 1; i < checkpoint.length; i += 1) {
      if (checkpoint[i].epoch === checkpoint[i - 1].epoch) {
        throw new PlannerError('INVALID_INPUT', 'epochCheckpoint epochs must be unique');
      }
      if (checkpoint[i].startOffset < checkpoint[i - 1].startOffset) {
        throw new PlannerError('INVALID_INPUT', 'epochCheckpoint startOffsets must be non-decreasing');
      }
    }
  } else {
    checkpoint = [{ epoch: Number.isInteger(leaderEpoch) ? leaderEpoch : 0, startOffset: 0 }];
  }
  return checkpoint;
}

export function normalizeReplica(id, raw) {
  if (!raw || typeof raw !== 'object') {
    throw new PlannerError('INVALID_INPUT', `replicas.${id} must be an object`);
  }
  const logEndOffset = raw.logEndOffset ?? raw.leo;
  if (!isNonNegativeInt(logEndOffset)) {
    throw new PlannerError('INVALID_INPUT', `replicas.${id}.logEndOffset must be a non-negative safe integer`);
  }
  const segments = normalizeSegments(raw.segments);
  const epochCheckpoint = normalizeCheckpoint(raw.epochCheckpoint, raw.leaderEpoch);
  const maxEpoch = epochCheckpoint[epochCheckpoint.length - 1].epoch;
  const earliestOffset = isNonNegativeInt(raw.earliestOffset)
    ? raw.earliestOffset
    : (segments[0]?.baseOffset ?? epochCheckpoint[0].startOffset ?? 0);

  return { id, leo: logEndOffset, segments, epochCheckpoint, maxEpoch, earliestOffset };
}

const LEADER_EVENT_TYPES = new Set([
  'leader_elected',
  'leader_election',
  'leader_change',
  'leader_switch',
  'leader_claim',
]);

function eventOrderKey(event, index) {
  if (Number.isFinite(event.sequence)) return event.sequence;
  if (typeof event.at === 'string') {
    const parsed = Date.parse(event.at);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return index;
}

/**
 * Decide which replica is the authoritative source.
 *
 * Leader events are replayed chronologically. A leader event carrying an epoch
 * lower than the highest epoch already observed in the stream is a stale
 * (zombie / rolled-back) event and gets fenced instead of becoming authority.
 */
export function resolveAuthority(input, replicas, options = {}) {
  const rawHistory = Array.isArray(input.leaderHistory) ? input.leaderHistory : [];
  const history = rawHistory.map((entry, index) => {
    const epoch = entry.epoch;
    const leader = entry.leader ?? entry.replica ?? entry.leaderId;
    if (!Number.isSafeInteger(epoch) || epoch < 0) {
      throw new PlannerError('INVALID_INPUT', `leaderHistory[${index}].epoch invalid`);
    }
    if (typeof leader !== 'string' || leader.length === 0) {
      throw new PlannerError('INVALID_INPUT', `leaderHistory[${index}].leader invalid`);
    }
    const startOffset = entry.startOffset ?? entry.start ?? 0;
    if (!isNonNegativeInt(startOffset)) {
      throw new PlannerError('INVALID_INPUT', `leaderHistory[${index}].startOffset invalid`);
    }
    return { epoch, leader, startOffset };
  });
  history.sort((a, b) => a.epoch - b.epoch);

  const warnings = [];
  const fencedEvents = [];
  const replicaStatus = {};

  let authority = null;
  if (history.length > 0) {
    const top = history[history.length - 1];
    authority = { replica: top.leader, epoch: top.epoch, basis: 'leaderHistory' };
  } else {
    let best = null;
    for (const replica of replicas.values()) {
      if (!best || replica.maxEpoch > best.maxEpoch ||
        (replica.maxEpoch === best.maxEpoch && replica.leo > best.leo)) {
        best = replica;
      }
    }
    if (best) authority = { replica: best.id, epoch: best.maxEpoch, basis: 'derivedFromReplicaEpochs' };
  }

  const forcedReplica = options.authoritativeReplica ?? input.authoritativeReplica ?? null;
  const forced = typeof forcedReplica === 'string' && forcedReplica.length > 0;
  if (forced) {
    if (!replicas.has(forcedReplica)) {
      throw new PlannerError('AUTHORITY_NOT_FOUND', `authoritativeReplica "${forcedReplica}" is not in replicas`);
    }
    return {
      authority: {
        replica: forcedReplica,
        epoch: replicas.get(forcedReplica).maxEpoch,
        basis: 'operatorOverride',
      },
      fencedEvents,
      warnings,
      replicaStatus,
      history,
      override: true,
    };
  }

  const events = (Array.isArray(input.events) ? input.events : [])
    .map((event, index) => ({ event, order: eventOrderKey(event, index), index }))
    .sort((a, b) => (a.order - b.order) || (a.index - b.index));

  let maxSeenEventEpoch = -1;
  let eventAuthority = null;
  for (const { event, index } of events) {
    const type = String(event.type ?? event.event ?? '').toLowerCase();
    const replica = event.replica ?? event.broker ?? event.leader ?? event.leaderId ?? null;

    if (LEADER_EVENT_TYPES.has(type)) {
      const epoch = event.epoch;
      if (!Number.isSafeInteger(epoch) || epoch < 0) {
        warnings.push({ code: 'MALFORMED_LEADER_EVENT', index, message: 'leader event ignored: missing integer epoch' });
        continue;
      }
      if (epoch < maxSeenEventEpoch) {
        fencedEvents.push({
          code: 'EPOCH_ROLLBACK_FENCED',
          eventIndex: index,
          sequence: event.sequence ?? null,
          type,
          epoch,
          replica,
          rejectedByEpoch: maxSeenEventEpoch,
          message: `stale leader event with epoch ${epoch} fenced; current observed epoch is ${maxSeenEventEpoch}`,
        });
        continue;
      }
      maxSeenEventEpoch = epoch;
      if (eventAuthority && epoch === eventAuthority.epoch && replica && replica !== eventAuthority.replica) {
        warnings.push({
          code: 'SAME_EPOCH_SPLIT_BRAIN',
          epoch,
          replicas: [eventAuthority.replica, replica],
          message: `two leaders claimed for the same epoch ${epoch}`,
        });
      }
      eventAuthority = { replica, epoch, basis: 'event' };
    } else if (/fail|down|crash|offline|partition/.test(type)) {
      if (replica) replicaStatus[replica] = { state: 'down', lastEvent: type };
    } else if (/recover|back|up|online|restart/.test(type)) {
      if (replica) replicaStatus[replica] = { state: 'recovered', lastEvent: type };
    }
  }

  if (eventAuthority && (!authority || eventAuthority.epoch > authority.epoch)) {
    authority = eventAuthority;
  } else if (
    eventAuthority && authority &&
    eventAuthority.epoch === authority.epoch &&
    eventAuthority.replica && eventAuthority.replica !== authority.replica
  ) {
    warnings.push({
      code: 'AUTHORITY_SOURCE_MISMATCH',
      epoch: authority.epoch,
      leaderHistoryReplica: authority.replica,
      eventReplica: eventAuthority.replica,
      message: 'leaderHistory and failure events disagree about the leader; keeping leaderHistory entry',
    });
  }

  if (!authority || !authority.replica || !replicas.has(authority.replica)) {
    throw new PlannerError('AUTHORITY_NOT_FOUND', 'could not resolve an authoritative replica from input');
  }

  return { authority, fencedEvents, warnings, replicaStatus, history, override: false };
}

/**
 * Kafka-style OffsetsForLeaderEpoch query against the authoritative source.
 *
 * For an older epoch that the source still knows, the safe truncation point is
 * the start offset of the next epoch. For an unknown gap epoch, the first
 * later epoch start is used. Asking for an epoch newer than the source has
 * means the source is stale and cannot serve as donor.
 */
export function offsetsForLeaderEpoch(source, requestedEpoch) {
  const checkpoint = source.epochCheckpoint;
  const latest = checkpoint[checkpoint.length - 1];

  if (requestedEpoch > latest.epoch) {
    return { status: 'EPOCH_NEWER_THAN_SOURCE', endOffsetExclusive: null };
  }

  const index = checkpoint.findIndex((entry) => entry.epoch === requestedEpoch);
  if (index !== -1) {
    if (index === checkpoint.length - 1) {
      return { status: 'OK_CURRENT_EPOCH', endOffsetExclusive: source.leo };
    }
    return { status: 'OK_OLDER_EPOCH', endOffsetExclusive: checkpoint[index + 1].startOffset };
  }

  const firstLater = checkpoint.find((entry) => entry.epoch > requestedEpoch);
  if (firstLater) {
    return { status: 'OK_EPOCH_GAP', endOffsetExclusive: firstLater.startOffset };
  }

  return { status: 'EPOCH_PURGED', endOffsetExclusive: source.earliestOffset };
}

/**
 * Refine divergence using segment summaries. An explicit checksum mismatch on
 * a segment starting at the same base offset proves a tail conflict there,
 * even when epochs are identical.
 */
export function findSegmentConflict(targetSegments, sourceSegments) {
  const sourceByBase = new Map(sourceSegments.map((segment) => [segment.baseOffset, segment]));
  for (const targetSegment of targetSegments) {
    const sourceSegment = sourceByBase.get(targetSegment.baseOffset);
    if (!sourceSegment) continue;
    if (targetSegment.checksum && sourceSegment.checksum &&
      targetSegment.checksum !== sourceSegment.checksum) {
      return {
        conflictOffset: targetSegment.baseOffset,
        targetBaseOffset: targetSegment.baseOffset,
        reason: 'CHECKSUM_MISMATCH',
        targetChecksum: targetSegment.checksum,
        sourceChecksum: sourceSegment.checksum,
      };
    }
  }
  return { conflictOffset: null, reason: 'NO_PROVEN_SEGMENT_CONFLICT' };
}

function findSourceGap(source, startOffset, endOffsetExclusive) {
  if (endOffsetExclusive < startOffset) {
    return makeRange(endOffsetExclusive, startOffset);
  }
  if (source.segments.length === 0) return null;

  let cursor = startOffset;
  for (const segment of source.segments) {
    if (segment.endOffsetExclusive <= cursor) continue;
    if (segment.baseOffset > cursor) {
      return makeRange(cursor, Math.min(segment.baseOffset, endOffsetExclusive));
    }
    cursor = Math.max(cursor, segment.endOffsetExclusive);
    if (cursor >= endOffsetExclusive) return null;
  }
  return cursor < endOffsetExclusive ? makeRange(cursor, endOffsetExclusive) : null;
}

function buildRiskChecks(replicas, committedWatermark) {
  const all = [...replicas.values()];
  const longest = all.reduce((a, b) => (b.leo > a.leo ? b : a));
  const shortest = all.reduce((a, b) => (b.leo < a.leo ? b : a));
  const checks = [];

  if (longest.leo > shortest.leo) {
    const deletedCommittedRange = makeRange(
      shortest.leo,
      Math.min(longest.leo, committedWatermark),
    );
    checks.push({
      id: 'naive-truncate-longer-to-shorter',
      safe: deletedCommittedRange === null,
      longerReplica: longest.id,
      shorterReplica: shortest.id,
      naiveTargetOffset: shortest.leo,
      deletedCommittedRange,
      message: deletedCommittedRange
        ? `truncating ${longest.id} to ${shortest.leo} would delete committed offsets`
        : 'truncating the longer replica to the shorter LEO only removes uncommitted tail',
    });
  }

  const outOfRangeCommittedOffsets = makeRange(shortest.leo, committedWatermark);
  checks.push({
    id: 'keep-shorter-replica-as-leader',
    duplicateConsumptionRisk: outOfRangeCommittedOffsets !== null,
    shorterReplica: shortest.id,
    shorterLogEndOffset: shortest.leo,
    outOfRangeCommittedOffsetRange: outOfRangeCommittedOffsets,
    message: outOfRangeCommittedOffsets
      ? `keeping ${shortest.id} (LEO ${shortest.leo}) can push consumer committed offsets out of range and trigger reset/re-consumption`
      : 'shorter replica is at or above the committed watermark; no out-of-range consumer offsets',
  });

  return checks;
}

/**
 * Build concrete segment operations for one repairable replica.
 *
 * Truncated whole segments are deleted tail-first. Copied chunks are always
 * written to a `.staging` file and published with an atomic rename, so the
 * consumer-visible segment registry never contains a half-written segment.
 * When the first copied chunk only covers the suffix of a source segment, it
 * is merged with the retained target prefix and atomically replaces the target
 * active segment.
 */
function planSegmentOperations(target, source, targetOffset, copyRange) {
  const steps = [];
  const segmentActions = {
    deleteSegmentBases: [],
    truncateSegmentBases: [],
    publishedSegments: [],
  };

  if (targetOffset < target.leo) {
    const affected = target.segments
      .filter((segment) => segment.endOffsetExclusive > targetOffset
        && segment.baseOffset < target.leo)
      .sort((a, b) => b.baseOffset - a.baseOffset);

    for (const segment of affected) {
      if (segment.baseOffset >= targetOffset) {
        steps.push({
          op: 'delete_segment',
          replica: target.id,
          segmentBaseOffset: segment.baseOffset,
          file: segmentFileName(segment.baseOffset),
          removedRange: makeRange(segment.baseOffset, segment.endOffsetExclusive),
        });
        segmentActions.deleteSegmentBases.push(segment.baseOffset);
      } else {
        steps.push({
          op: 'truncate_segment',
          replica: target.id,
          segmentBaseOffset: segment.baseOffset,
          file: segmentFileName(segment.baseOffset),
          fromEndOffset: segment.endOffsetExclusive,
          targetOffset,
          removedRange: makeRange(targetOffset, segment.endOffsetExclusive),
        });
        segmentActions.truncateSegmentBases.push(segment.baseOffset);
      }
    }
  }

  if (copyRange) {
    const chunks = source.segments
      .filter((segment) => segment.endOffsetExclusive > copyRange.startOffset
        && segment.baseOffset < copyRange.endOffsetExclusive)
      .sort((a, b) => a.baseOffset - b.baseOffset);

    let firstChunk = true;
    for (const sourceSegment of chunks) {
      const chunkStart = Math.max(sourceSegment.baseOffset, copyRange.startOffset);
      const chunkEnd = Math.min(sourceSegment.endOffsetExclusive, copyRange.endOffsetExclusive);
      const chunkRange = { startOffset: chunkStart, endOffsetExclusive: chunkEnd };

      const publish = (stageStep) => {
        steps.push(stageStep);
        steps.push({
          op: 'publish_segment',
          replica: target.id,
          atomic: 'rename',
          from: stageStep.stagingFile,
          to: stageStep.finalFile,
          visibility: 'segment enters the consumer-visible registry atomically',
        });
        segmentActions.publishedSegments.push({
          segmentBaseOffset: stageStep.segmentBaseOffset,
          copyRange: chunkRange,
          merged: stageStep.op === 'stage_merged_segment',
        });
      };

      if (firstChunk && chunkStart > sourceSegment.baseOffset) {
        const boundary = [...target.segments]
          .reverse()
          .find((segment) => segment.baseOffset < chunkStart
            && segment.endOffsetExclusive >= chunkStart);

        if (boundary) {
          const finalFile = segmentFileName(boundary.baseOffset);
          publish({
            op: 'stage_merged_segment',
            replica: target.id,
            source: source.id,
            segmentBaseOffset: boundary.baseOffset,
            finalFile,
            stagingFile: `${finalFile}.staging`,
            replacesFile: finalFile,
            retainedPrefixRange: makeRange(boundary.baseOffset, chunkStart),
            copiedRange: chunkRange,
          });
          firstChunk = false;
          continue;
        }
      }

      const segmentBaseOffset = chunkStart === sourceSegment.baseOffset
        ? sourceSegment.baseOffset
        : chunkStart;
      const finalFile = segmentFileName(segmentBaseOffset);
      publish({
        op: 'stage_segment',
        replica: target.id,
        source: source.id,
        segmentBaseOffset,
        finalFile,
        stagingFile: `${finalFile}.staging`,
        sourceSegmentBaseOffset: sourceSegment.baseOffset,
        copiedRange: chunkRange,
        wholeSourceSegment: chunkStart === sourceSegment.baseOffset
          && chunkEnd === sourceSegment.endOffsetExclusive,
      });
      firstChunk = false;
    }
  }

  // A merged stage+publish already performs the boundary segment truncation
  // atomically; drop any redundant in-place truncate for the same file.
  const mergedBases = new Set(
    segmentActions.publishedSegments
      .filter((entry) => entry.merged)
      .map((entry) => entry.segmentBaseOffset),
  );
  const filteredSteps = steps.filter((step) => {
    if (step.op === 'truncate_segment' && mergedBases.has(step.segmentBaseOffset)) {
      return false;
    }
    return true;
  });
  segmentActions.truncateSegmentBases = segmentActions.truncateSegmentBases
    .filter((baseOffset) => !mergedBases.has(baseOffset));

  return { steps: filteredSteps, segmentActions };
}

export function planRepair(input, options = {}) {
  if (!input || typeof input !== 'object') {
    throw new PlannerError('INVALID_INPUT', 'input must be a JSON object');
  }

  const topic = typeof input.topic === 'string' ? input.topic : 'unknown-topic';
  const partition = Number.isSafeInteger(input.partition) ? input.partition : 0;
  const committedWatermark = input.committedWatermark ?? input.highWatermark ?? input.highWaterMark;
  if (!isNonNegativeInt(committedWatermark)) {
    throw new PlannerError('INVALID_INPUT', 'committedWatermark must be a non-negative safe integer');
  }

  const rawReplicas = input.replicas;
  if (!rawReplicas || typeof rawReplicas !== 'object' || Array.isArray(rawReplicas)
    || Object.keys(rawReplicas).length === 0) {
    throw new PlannerError('INVALID_INPUT', 'input.replicas must be a non-empty object keyed by replica id');
  }

  const replicas = new Map();
  for (const [id, rawReplica] of Object.entries(rawReplicas)) {
    replicas.set(id, normalizeReplica(id, rawReplica));
  }

  const resolution = resolveAuthority(input, replicas, options);
  const { authority } = resolution;
  const source = replicas.get(authority.replica);

  if (committedWatermark > source.leo) {
    throw new PlannerError(
      'INVALID_INPUT',
      'committedWatermark cannot exceed the authoritative leader logEndOffset',
      { committedWatermark, authorityLeo: source.leo },
    );
  }

  const riskChecks = buildRiskChecks(replicas, committedWatermark);
  const unrepairable = [];
  const replicaPlans = {};
  const steps = [];

  const zombieInfo = new Map();
  if (!resolution.override) {
    for (const entry of resolution.history) {
      if (entry.epoch < authority.epoch && entry.leader !== authority.replica) {
        const previous = zombieInfo.get(entry.leader) ?? -1;
        zombieInfo.set(entry.leader, Math.max(previous, entry.epoch));
      }
    }
    for (const fenced of resolution.fencedEvents) {
      if (fenced.replica && fenced.replica !== authority.replica) {
        const previous = zombieInfo.get(fenced.replica) ?? -1;
        zombieInfo.set(fenced.replica, Math.max(previous, fenced.epoch));
      }
    }

    for (const [replica, epoch] of [...zombieInfo.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      if (!replicas.has(replica)) continue;
      steps.push({
        op: 'fence_zombie_leader',
        replica,
        epoch,
        reason: `reject all writes/claims from epoch ${epoch}; authority is ${authority.replica}@${authority.epoch}`,
      });
    }
  }

  steps.push({
    op: 'freeze_consumer_visibility',
    visibleOffset: committedWatermark,
    mode: 'freeze-segment-registry',
    reason: 'consumers stay capped at the committed watermark while segments are repaired',
  });

  const nonAuthorityIds = [...replicas.keys()]
    .filter((id) => id !== authority.replica)
    .sort((a, b) => {
      const priorityDiff = Number(zombieInfo.has(b)) - Number(zombieInfo.has(a));
      if (priorityDiff !== 0) return priorityDiff;
      return a.localeCompare(b);
    });

  let plannedReplicaCount = 0;

  for (const id of nonAuthorityIds) {
    const target = replicas.get(id);
    const role = zombieInfo.has(id) ? 'zombie_leader' : 'follower';
    const basePlan = {
      replica: id,
      role,
      source: authority.replica,
      currentLogEndOffset: target.leo,
      currentEpoch: target.maxEpoch,
    };

    // Rule 1: an older-epoch source must never overwrite newer-epoch records.
    if (target.maxEpoch > source.maxEpoch) {
      const firstNewEpoch = target.epochCheckpoint
        .find((entry) => entry.epoch > source.maxEpoch);
      const range = {
        startOffset: firstNewEpoch?.startOffset ?? target.earliestOffset,
        endOffsetExclusive: target.leo,
      };
      const item = {
        replica: id,
        code: 'EPOCH_ROLLBACK',
        range,
        source: authority.replica,
        sourceEpoch: source.maxEpoch,
        targetEpoch: target.maxEpoch,
        remediation: 'elect an authority whose epoch is at least the target epoch, or manually preserve/rebuild the newer-epoch replica',
        message: `${authority.replica}@epoch${source.maxEpoch} cannot overwrite epoch ${target.maxEpoch} records on ${id}`,
      };
      unrepairable.push(item);
      replicaPlans[id] = {
        ...basePlan,
        status: 'unrepairable',
        targetOffset: null,
        catchUpToOffset: null,
        truncateRange: null,
        copyRanges: [],
        blockedBy: item.code,
      };
      continue;
    }

    const epochAnswer = offsetsForLeaderEpoch(source, target.maxEpoch);
    const divergencePoints = [];
    if (epochAnswer.endOffsetExclusive !== null) {
      divergencePoints.push(epochAnswer.endOffsetExclusive);
    } else {
      const item = {
        replica: id,
        code: 'EPOCH_ROLLBACK',
        range: { startOffset: target.earliestOffset, endOffsetExclusive: target.leo },
        source: authority.replica,
        sourceEpoch: source.maxEpoch,
        targetEpoch: target.maxEpoch,
        message: epochAnswer.status,
        remediation: 'use a newer-epoch donor',
      };
      unrepairable.push(item);
      replicaPlans[id] = {
        ...basePlan,
        status: 'unrepairable',
        targetOffset: null,
        catchUpToOffset: null,
        truncateRange: null,
        copyRanges: [],
        blockedBy: item.code,
      };
      continue;
    }

    const segmentConflict = findSegmentConflict(target.segments, source.segments);
    if (segmentConflict.conflictOffset !== null) {
      divergencePoints.push(segmentConflict.conflictOffset);
    }

    // targetOffset is the fork point: truncate down to it, then copy from it.
    const targetOffset = Math.max(0, Math.min(target.leo, ...divergencePoints));

    // Rule 2: only the uncommitted tail (offsets >= high watermark) may be
    // deleted automatically.
    const truncateRange = makeRange(targetOffset, target.leo);
    if (truncateRange && targetOffset < committedWatermark) {
      const committedRange = makeRange(targetOffset, Math.min(target.leo, committedWatermark));
      const item = {
        replica: id,
        code: 'COMMITTED_DIVERGENCE',
        targetOffset,
        range: committedRange ?? { startOffset: targetOffset, endOffsetExclusive: committedWatermark },
        committedWatermark,
        truncateWouldDeleteRange: truncateRange,
        remediation: 'operator-approved full replica rebuild from the authoritative source; automatic tail truncation is forbidden below the watermark',
        message: `safe epoch offset ${targetOffset} is below committedWatermark ${committedWatermark}; truncation would delete committed records`,
      };
      unrepairable.push(item);
      replicaPlans[id] = {
        ...basePlan,
        status: 'unrepairable',
        targetOffset: null,
        candidateTargetOffset: targetOffset,
        catchUpToOffset: null,
        truncateRange: null,
        copyRanges: [],
        blockedBy: item.code,
      };
      continue;
    }

    if (source.leo < targetOffset) {
      const item = {
        replica: id,
        code: 'SOURCE_SHORT_OF_TARGET_OFFSET',
        range: makeRange(source.leo, targetOffset) ?? { startOffset: source.leo, endOffsetExclusive: targetOffset },
        message: 'authoritative source does not contain the agreed divergence offset',
      };
      unrepairable.push(item);
      replicaPlans[id] = { ...basePlan, status: 'unrepairable', targetOffset: null, copyRanges: [], blockedBy: item.code };
      continue;
    }

    const copyRange = makeRange(targetOffset, source.leo);
    const sourceGap = copyRange ? findSourceGap(source, copyRange.startOffset, copyRange.endOffsetExclusive) : null;
    if (sourceGap) {
      const item = {
        replica: id,
        code: 'SOURCE_SEGMENT_GAP',
        range: sourceGap,
        message: 'authoritative source segment summaries do not cover the full copy range',
      };
      unrepairable.push(item);
      replicaPlans[id] = { ...basePlan, status: 'unrepairable', targetOffset: null, copyRanges: [], blockedBy: item.code };
      continue;
    }

    if (truncateRange) {
      steps.push({
        op: 'truncate_log',
        replica: id,
        targetOffset,
        range: truncateRange,
        watermarkSafety: `targetOffset ${targetOffset} >= committedWatermark ${committedWatermark}; only uncommitted tail is removed`,
      });
    }
    const { steps: replicaSteps, segmentActions } = planSegmentOperations(
      target,
      source,
      targetOffset,
      copyRange,
    );
    for (const replicaStep of replicaSteps) steps.push(replicaStep);

    if (truncateRange || copyRange) plannedReplicaCount += 1;

    replicaPlans[id] = {
      ...basePlan,
      status: (truncateRange || copyRange) ? 'repair_planned' : 'noop',
      targetOffset,
      catchUpToOffset: source.leo,
      finalLogEndOffset: source.leo,
      truncateRange,
      copyRanges: copyRange ? [copyRange] : [],
      divergence: {
        leaderEpochQuery: epochAnswer,
        segmentConflict,
      },
      segmentActions,
    };
  }

  replicaPlans[authority.replica] = {
    replica: authority.replica,
    role: 'authoritative_leader',
    source: authority.replica,
    status: 'authoritative',
    currentLogEndOffset: source.leo,
    currentEpoch: source.maxEpoch,
    targetOffset: source.leo,
    catchUpToOffset: source.leo,
    finalLogEndOffset: source.leo,
    truncateRange: null,
    copyRanges: [],
  };

  if (unrepairable.length > 0) {
    steps.push({
      op: 'escalate_manual_intervention',
      codes: unrepairable.map((item) => item.code),
      replicas: unrepairable.map((item) => item.replica),
      reason: 'one or more ranges violate automatic repair safety rules; consumers remain capped at the watermark',
    });
  } else {
    if (plannedReplicaCount > 0 && source.leo > committedWatermark) {
      steps.push({
        op: 'advance_high_watermark',
        fromOffset: committedWatermark,
        toOffset: source.leo,
        simulated: true,
        basis: 'all replicas aligned to authoritative LEO; real HW advancement also depends on acks/min.insync.replicas',
      });
    }
    steps.push({
      op: 'resume_consumers',
      visibleOffset: unrepairable.length === 0
        ? (plannedReplicaCount > 0 ? source.leo : committedWatermark)
        : committedWatermark,
    });
  }

  const order = steps.map((step, index) => ({ seq: index + 1, ...step }));
  const targetOffsets = Object.fromEntries(
    Object.entries(replicaPlans).map(([id, plan]) => [id, plan.targetOffset]),
  );
  const unrepairableRanges = unrepairable.map((item) => ({
    replica: item.replica,
    code: item.code,
    range: item.range,
  }));

  const plan = {
    ok: unrepairable.length === 0,
    simulated: true,
    topic,
    partition,
    authority: {
      replica: authority.replica,
      epoch: authority.epoch,
      basis: authority.basis,
    },
    committedWatermark,
    visibility: {
      frozenDuringRepair: true,
      consumerVisibleOffsetDuringRepair: committedWatermark,
      stagingSuffix: '.staging',
      publishMode: 'atomic-rename',
      rule: 'consumers only read the published segment registry; staging files and truncated tails are never registered',
    },
    riskChecks,
    fencedEvents: resolution.fencedEvents,
    warnings: resolution.warnings,
    replicas: replicaPlans,
    targetOffsets,
    unrepairable,
    unrepairableRanges,
    order,
    repairOrder: order,
    summary: {
      totalReplicas: replicas.size,
      repairPlanned: Object.values(replicaPlans).filter((entry) => entry.status === 'repair_planned').length,
      noop: Object.values(replicaPlans).filter((entry) => entry.status === 'noop').length,
      unrepairable: unrepairable.length,
      steps: order.length,
    },
  };

  return plan;
}
