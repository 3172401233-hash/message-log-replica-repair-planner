#!/usr/bin/env node
import { readFile } from 'node:fs/promises';

import { planRepair, PlannerError } from './planner.js';

function parseArgs(argv) {
  const args = { inputPath: null, authoritativeReplica: null };
  const positional = [];
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--authority' || token === '--authoritative-replica') {
      args.authoritativeReplica = argv[i + 1] ?? null;
      i += 1;
    } else if (token === '-h' || token === '--help') {
      args.help = true;
    } else {
      positional.push(token);
    }
  }
  args.inputPath = positional[0] ?? null;
  return args;
}

const USAGE = `Usage: node src/cli.js <input.json> [--authority <replicaId>]

Reads a simulated replica-state document and prints a JSON repair plan.
If no path is given, JSON is read from stdin.

Exit codes:
  0  plan produced with no unrepairable ranges
  2  invalid input
  3  plan produced but contains unrepairable ranges
  1  unexpected failure
`;

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(USAGE);
    return 0;
  }

  let text;
  try {
    if (args.inputPath) {
      text = await readFile(args.inputPath, 'utf8');
    } else if (!process.stdin.isTTY) {
      text = await readStdin();
    } else {
      process.stderr.write(USAGE);
      return 2;
    }
  } catch (error) {
    process.stderr.write(`Cannot read input: ${error.message}\n`);
    return 2;
  }

  let input;
  try {
    input = JSON.parse(text);
  } catch (error) {
    process.stderr.write(`Invalid JSON input: ${error.message}\n`);
    return 2;
  }

  try {
    const plan = planRepair(input, {
      authoritativeReplica: args.authoritativeReplica ?? undefined,
    });
    process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
    return plan.ok ? 0 : 3;
  } catch (error) {
    if (error instanceof PlannerError) {
      process.stderr.write(`[${error.code}] ${error.message}\n`);
      return 2;
    }
    throw error;
  }
}

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((error) => {
    process.stderr.write(`Unexpected failure: ${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
