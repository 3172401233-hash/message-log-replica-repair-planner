import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

import { planRepair, offsetsForLeaderEpoch, normalizeReplica } from '../src/planner.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');

async function loadExample(name) {
  const text = await readFile(path.join(root, 'examples', name), 'utf8');
  return JSON.parse(text);
}

const MUTATING_OPS = new Set([
  'truncate_log',
  'delete_segment',
  'truncate_segment',
  'stage_segment',
  'stage_merged_segment',
  'publish_segment',
]);

test('尾部冲突 tail-conflict: truncates conflicting uncommitted tail and copies authority ranges', async () => {
  const input = await loadExample('tail-conflict.json');
  const plan = planRepair(input);

  assert.equal(plan.ok, true);
  assert.deepEqual(plan.authority, { replica: 'r1', epoch: 1, basis: 'leaderHistory' });

  const r2 = plan.replicas.r2;
  assert.equal(r2.status, 'repair_planned');
  assert.equal(r2.targetOffset, 100);
  assert.equal(r2.catchUpToOffset, 180);
  assert.deepEqual(r2.truncateRange, { startOffset: 100, endOffsetExclusive: 170 });
  assert.deepEqual(r2.copyRanges, [{ startOffset: 100, endOffsetExclusive: 180 }]);
  assert.deepEqual(r2.segmentActions.deleteSegmentBases, [150, 100]);
  assert.equal(r2.segmentActions.publishedSegments.length, 2);

  const truncateStep = plan.order.find((step) => step.op === 'truncate_log' && step.replica === 'r2');
  assert.equal(truncateStep.targetOffset, 100);
  assert.match(truncateStep.watermarkSafety, /committedWatermark 100/);

  const deleteBases = plan.order
    .filter((step) => step.op === 'delete_segment')
    .map((step) => step.segmentBaseOffset);
  assert.deepEqual(deleteBases, [150, 100], 'segments are removed tail-first');
});

test('尾部冲突已提交 tail-conflict-committed: below the watermark is unrepairable and never auto-truncated', async () => {
  const input = await loadExample('tail-conflict.json');
  input.committedWatermark = 120;
  const plan = planRepair(input);

  assert.equal(plan.ok, false);
  const blocked = plan.unrepairable.find((item) => item.code === 'COMMITTED_DIVERGENCE');
  assert.ok(blocked, 'expected a COMMITTED_DIVERGENCE item');
  assert.equal(blocked.replica, 'r2');
  assert.equal(blocked.targetOffset, 100);
  assert.deepEqual(blocked.range, { startOffset: 100, endOffsetExclusive: 120 });
  assert.deepEqual(blocked.truncateWouldDeleteRange, { startOffset: 100, endOffsetExclusive: 170 });

  assert.equal(plan.replicas.r2.status, 'unrepairable');
  const mutatesBlockedReplica = plan.order.some(
    (step) => step.replica === 'r2' && MUTATING_OPS.has(step.op),
  );
  assert.equal(mutatesBlockedReplica, false);
  assert.equal(plan.order.at(-1).op, 'escalate_manual_intervention');
});

test('committed records on the longer replica are preserved; shorter replica catches up', () => {
  const input = {
    topic: 't',
    partition: 0,
    committedWatermark: 140,
    leaderHistory: [{ epoch: 1, leader: 'r1', startOffset: 0 }],
    replicas: {
      r1: {
        logEndOffset: 150,
        epochCheckpoint: [{ epoch: 1, startOffset: 0 }],
        segments: [
          { baseOffset: 0, endOffsetExclusive: 120, checksum: 'a' },
          { baseOffset: 120, endOffsetExclusive: 150, checksum: 'b' },
        ],
      },
      r2: {
        logEndOffset: 120,
        epochCheckpoint: [{ epoch: 1, startOffset: 0 }],
        segments: [{ baseOffset: 0, endOffsetExclusive: 120, checksum: 'a' }],
      },
    },
  };

  const plan = planRepair(input);
  assert.equal(plan.ok, true);
  assert.equal(plan.replicas.r1.status, 'authoritative');
  assert.equal(plan.order.some((step) => step.replica === 'r1'), false);

  const r2 = plan.replicas.r2;
  assert.equal(r2.targetOffset, 120);
  assert.equal(r2.truncateRange, null);
  assert.deepEqual(r2.copyRanges, [{ startOffset: 120, endOffsetExclusive: 150 }]);

  const truncateLongRisk = plan.riskChecks.find((check) => check.id === 'naive-truncate-longer-to-shorter');
  assert.equal(truncateLongRisk.safe, false);
  assert.deepEqual(truncateLongRisk.deletedCommittedRange, { startOffset: 120, endOffsetExclusive: 140 });

  const keepShortRisk = plan.riskChecks.find((check) => check.id === 'keep-shorter-replica-as-leader');
  assert.equal(keepShortRisk.duplicateConsumptionRisk, true);
  assert.deepEqual(keepShortRisk.outOfRangeCommittedOffsetRange, { startOffset: 120, endOffsetExclusive: 140 });
});

test('leader 切换 leader-switch: authority moves to newer epoch and zombie old leader is fenced first', async () => {
  const input = await loadExample('leader-switch.json');
  const plan = planRepair(input);

  assert.equal(plan.ok, true);
  assert.deepEqual(plan.authority, { replica: 'r2', epoch: 2, basis: 'leaderHistory' });
  assert.equal(plan.fencedEvents.length, 1);
  assert.equal(plan.fencedEvents[0].code, 'EPOCH_ROLLBACK_FENCED');
  assert.equal(plan.fencedEvents[0].epoch, 1);

  const firstStep = plan.order[0];
  assert.equal(firstStep.op, 'fence_zombie_leader');
  assert.equal(firstStep.replica, 'r1');
  assert.equal(firstStep.epoch, 1);

  const r1 = plan.replicas.r1;
  assert.equal(r1.role, 'zombie_leader');
  assert.equal(r1.targetOffset, 120);
  assert.equal(r1.catchUpToOffset, 160);
  assert.deepEqual(r1.truncateRange, { startOffset: 120, endOffsetExclusive: 150 });
  assert.deepEqual(r1.copyRanges, [{ startOffset: 120, endOffsetExclusive: 160 }]);

  const r3 = plan.replicas.r3;
  assert.equal(r3.role, 'follower');
  assert.equal(r3.targetOffset, 140);
  assert.equal(r3.truncateRange, null);
  assert.deepEqual(r3.copyRanges, [{ startOffset: 140, endOffsetExclusive: 160 }]);

  const merged = plan.order.find((step) => step.op === 'stage_merged_segment' && step.replica === 'r3');
  assert.ok(merged, 'r3 suffix copy should merge with its active segment');
  assert.equal(merged.segmentBaseOffset, 120);
  assert.deepEqual(merged.retainedPrefixRange, { startOffset: 120, endOffsetExclusive: 140 });
  assert.deepEqual(merged.copiedRange, { startOffset: 140, endOffsetExclusive: 160 });

  // Zombie is repaired before the lagging follower.
  const r1FirstIndex = plan.order.findIndex((step) => step.replica === 'r1' && step.op === 'truncate_log');
  const r3FirstIndex = plan.order.findIndex((step) => step.replica === 'r3' && step.op.startsWith('stage'));
  assert.ok(r1FirstIndex < r3FirstIndex);
});

test('epoch 回退 epoch-rollback: stale-epoch source cannot overwrite newer-epoch records', async () => {
  const input = await loadExample('epoch-rollback.json');

  // Default resolution picks epoch 3 / r2 and plans a safe repair of old r1.
  const safePlan = planRepair(input);
  assert.equal(safePlan.ok, true);
  assert.equal(safePlan.authority.replica, 'r2');
  assert.equal(safePlan.authority.epoch, 3);
  assert.equal(safePlan.fencedEvents.some((event) => event.code === 'EPOCH_ROLLBACK_FENCED' && event.epoch === 2), true);
  assert.equal(safePlan.replicas.r1.targetOffset, 120);
  assert.deepEqual(safePlan.replicas.r1.truncateRange, { startOffset: 120, endOffsetExclusive: 160 });

  // Forcing the old epoch leader as authority must be rejected, not executed.
  const forced = planRepair(input, { authoritativeReplica: 'r1' });
  assert.equal(forced.ok, false);
  assert.equal(forced.authority.basis, 'operatorOverride');

  const blocked = forced.unrepairable.find((item) => item.code === 'EPOCH_ROLLBACK');
  assert.ok(blocked);
  assert.equal(blocked.replica, 'r2');
  assert.equal(blocked.sourceEpoch, 2);
  assert.equal(blocked.targetEpoch, 3);
  assert.deepEqual(blocked.range, { startOffset: 120, endOffsetExclusive: 170 });

  const mutatesNewEpochReplica = forced.order.some(
    (step) => step.replica === 'r2' && MUTATING_OPS.has(step.op),
  );
  assert.equal(mutatesNewEpochReplica, false);
  assert.equal(forced.order.at(-1).op, 'escalate_manual_intervention');
});

test('consumer visibility: freeze first, stage then atomic publish, resume last', async () => {
  for (const name of ['leader-switch.json', 'tail-conflict.json']) {
    const plan = planRepair(await loadExample(name));
    assert.equal(plan.ok, true, name);

    const freezeIndex = plan.order.findIndex((step) => step.op === 'freeze_consumer_visibility');
    assert.equal(freezeIndex, name === 'leader-switch.json' ? 1 : 0, `${name}: freeze follows fencing`);
    assert.equal(plan.order[freezeIndex].visibleOffset, plan.committedWatermark);

    const pendingStaging = new Map();
    for (let i = 0; i < plan.order.length; i += 1) {
      const step = plan.order[i];
      if (step.op === 'stage_segment' || step.op === 'stage_merged_segment') {
        assert.ok(i > freezeIndex, 'staging must happen after visibility freeze');
        assert.match(step.stagingFile, /\.staging$/);
        assert.notEqual(step.stagingFile, step.finalFile);
        pendingStaging.set(`${step.replica}:${step.stagingFile}`, i);
      }
      if (step.op === 'publish_segment') {
        const key = `${step.replica}:${step.from}`;
        assert.ok(pendingStaging.has(key), `publish without matching stage: ${key}`);
        assert.equal(step.atomic, 'rename');
        assert.doesNotMatch(step.to, /\.staging$/);
        assert.ok(pendingStaging.get(key) < i, 'stage must precede publish');
        pendingStaging.delete(key);
      }
    }
    assert.equal(pendingStaging.size, 0, 'every staged segment must be published');

    const last = plan.order.at(-1);
    assert.equal(last.op, 'resume_consumers');
    for (const publish of plan.order.filter((step) => step.op === 'publish_segment')) {
      assert.ok(plan.order.indexOf(last) > plan.order.indexOf(publish));
    }
    assert.equal(plan.visibility.publishMode, 'atomic-rename');
  }
});

test('leader epoch query returns next epoch start for old epochs and LEO for current', () => {
  const source = normalizeReplica('r2', {
    logEndOffset: 170,
    epochCheckpoint: [
      { epoch: 1, startOffset: 0 },
      { epoch: 2, startOffset: 90 },
      { epoch: 3, startOffset: 120 },
    ],
  });

  assert.deepEqual(
    offsetsForLeaderEpoch(source, 2),
    { status: 'OK_OLDER_EPOCH', endOffsetExclusive: 120 },
  );
  assert.deepEqual(
    offsetsForLeaderEpoch(source, 3),
    { status: 'OK_CURRENT_EPOCH', endOffsetExclusive: 170 },
  );
  assert.equal(offsetsForLeaderEpoch(source, 9).status, 'EPOCH_NEWER_THAN_SOURCE');
  assert.deepEqual(
    offsetsForLeaderEpoch(source, 0),
    { status: 'OK_EPOCH_GAP', endOffsetExclusive: 0 },
  );
});

test('CLI emits JSON and maps blocked plans to exit code 3', () => {
  const cli = path.join(root, 'src', 'cli.js');
  const node = process.execPath;

  const success = spawnSync(node, [cli, path.join(root, 'examples', 'leader-switch.json')], { encoding: 'utf8' });
  assert.equal(success.status, 0, success.stderr);
  const plan = JSON.parse(success.stdout);
  assert.equal(plan.ok, true);
  assert.equal(plan.simulated, true);

  const blocked = spawnSync(
    node,
    [cli, path.join(root, 'examples', 'epoch-rollback.json'), '--authority', 'r1'],
    { encoding: 'utf8' },
  );
  assert.equal(blocked.status, 3, blocked.stderr);
  const blockedPlan = JSON.parse(blocked.stdout);
  assert.equal(blockedPlan.ok, false);
  assert.ok(blockedPlan.unrepairable.some((item) => item.code === 'EPOCH_ROLLBACK'));

  const invalid = spawnSync(node, [cli, path.join(root, 'examples', 'does-not-exist.json')], { encoding: 'utf8' });
  assert.equal(invalid.status, 2);
});
