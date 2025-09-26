/**
 * Distributed Job Queue (worker_queue.ts)
 *
 * - Minimal Redis-backed job queue + worker + scheduler.
 * - Uses Redis lists (LPUSH / RPOP) and a sorted set for scheduling.
 *
 * Requirements:
 *   npm install ioredis
 *
 * Usage:
 *   - Run a worker:   ts-node src/worker_queue.ts worker
 *   - Enqueue a job:  ts-node src/worker_queue.ts enqueue '{"type":"send_email","to":"a@b","body":"hi"}'
 *   - Schedule job:   ts-node src/worker_queue.ts schedule 10 '{"type":"cleanup"}'  (delay seconds)
 */
import Redis from 'ioredis';
import { setTimeout } from 'timers/promises';

const redis = new Redis(); // default localhost

const QUEUE_KEY = 'djq:queue';
const SCHEDULE_KEY = 'djq:schedule';
const PROCESSING_KEY = 'djq:processing';

async function enqueue(job: any) {
  const payload = JSON.stringify({ id: Date.now() + ':' + Math.random().toString(36).slice(2), job });
  await redis.lpush(QUEUE_KEY, payload);
  console.log('Enqueued job', payload);
}

async function schedule(delaySeconds: number, job: any) {
  const at = Date.now() + delaySeconds * 1000;
  const payload = JSON.stringify({ id: Date.now() + ':' + Math.random().toString(36).slice(2), job });
  await redis.zadd(SCHEDULE_KEY, at.toString(), payload);
  console.log('Scheduled job at', new Date(at).toISOString(), payload);
}

async function pollSchedule() {
  while (true) {
    const now = Date.now();
    // fetch scheduled jobs whose score <= now
    const list = await redis.zrangebyscore(SCHEDULE_KEY, 0, now, 'LIMIT', 0, 10);
    if (list.length > 0) {
      const pipeline = redis.pipeline();
      for (const item of list) {
        pipeline.lpush(QUEUE_KEY, item);
        pipeline.zrem(SCHEDULE_KEY, item);
      }
      await pipeline.exec();
      console.log('Moved scheduled jobs to queue:', list.length);
    }
    await setTimeout(1000);
  }
}

async function workerLoop(workerName = 'worker1') {
  console.log('Worker started:', workerName);
  while (true) {
    // RPOP to get job tail (pop oldest)
    const raw = await redis.rpoplpush(QUEUE_KEY, PROCESSING_KEY); // move to processing atomically
    if (!raw) {
      await setTimeout(500);
      continue;
    }
    try {
      const payload = JSON.parse(raw);
      console.log(`[${workerName}] processing`, payload.id, payload.job);
      // Simulate processing based on job.type
      await handleJob(payload.job);
      // on success, remove from processing
      await redis.lrem(PROCESSING_KEY, 1, raw);
      console.log(`[${workerName}] done`, payload.id);
    } catch (err) {
      console.error('Job error', err);
      // leave in processing for potential requeue / retry later
      await setTimeout(1000);
    }
  }
}

async function handleJob(job: any) {
  switch (job.type) {
    case 'send_email':
      // simulate email sending
      await setTimeout(300);
      console.log('Sent email to', job.to);
      break;
    case 'cleanup':
      await setTimeout(200);
      console.log('Cleanup done');
      break;
    default:
      // generic work
      await setTimeout(100);
      console.log('Processed job type', job.type);
  }
}

async function requeueStuckProcessing(timeoutMs = 60000) {
  // Move jobs stuck in processing back to queue if processing too long
  while (true) {
    const len = await redis.llen(PROCESSING_KEY);
    if (len > 0) {
      const items = await redis.lrange(PROCESSING_KEY, 0, -1);
      for (const item of items) {
        try {
          const payload = JSON.parse(item);
          // Basic heuristic: id includes timestamp at start before colon
          const created = parseInt(String(payload.id).split(':')[0] || '0', 10);
          if (Date.now() - created > timeoutMs) {
            await redis.lrem(PROCESSING_KEY, 1, item);
            await redis.lpush(QUEUE_KEY, item);
            console.log('Requeued stuck job', payload.id);
          }
        } catch (e) { /* ignore */ }
      }
    }
    await setTimeout(5000);
  }
}

// CLI
async function main() {
  const args = process.argv.slice(2);
  const cmd = args[0];
  if (cmd === 'enqueue') {
    const job = JSON.parse(args[1] || '{}');
    await enqueue(job);
    process.exit(0);
  } else if (cmd === 'schedule') {
    const delay = parseInt(args[1] || '10', 10);
    const job = JSON.parse(args[2] || '{}');
    await schedule(delay, job);
    process.exit(0);
  } else if (cmd === 'worker') {
    const name = args[1] || 'worker1';
    // run worker + schedule poll + requeue watchers
    pollSchedule();
    requeueStuckProcessing();
    workerLoop(name);
  } else {
    console.log('Usage: node worker_queue.js [enqueue <jobJson> | schedule <seconds> <jobJson> | worker <name>]');
    process.exit(0);
  }
}

main();
