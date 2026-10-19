// CI-only launcher: synthetic data, loopback publishing, the actual Free budget.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const exec = promisify(execFile);
export async function startContainer(image, key) {
  const work = await mkdtemp(join(tmpdir(), 'noyaru-docker-test-'));
  const envFile = join(work, 'private.env');
  await writeFile(envFile, `NOYARU_WP_DEMO_URL=https://wordpress.noyaru.test\nNOYARU_WP_DEMO_KEY=${key}\n`, { mode: 0o600 });
  let id;
  try {
    ({ stdout: id } = await exec('docker', ['run', '--detach', '--memory', '480m', '--memory-swap', '480m',
      '--cpus', '0.1', '--pids-limit', '128', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
      '--read-only', '--tmpfs', '/tmp:rw,nosuid,nodev,noexec,size=64m',
      '--env-file', envFile, '--publish', '127.0.0.1::10000', image]));
    id = id.trim();
    async function publishedBase() {
      const { stdout } = await exec('docker', ['port', id, '10000/tcp']);
      assert.match(stdout.trim(), /^127\.0\.0\.1:\d+$/);
      return 'http://' + stdout.trim();
    }
    return {
      base: await publishedBase(),
      async restart() {
        await exec('docker', ['restart', id]);
        // Docker may assign a new ephemeral host port when restarting the container.
        return publishedBase();
      },
      async metrics() {
        const { stdout: limits } = await exec('docker', ['inspect', '--format',
          '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.NanoCpus}} {{.State.OOMKilled}}', id]);
        const [memory, swap, cpu, oom] = limits.trim().split(' ');
        assert.equal(Number(memory), 480 * 1024 * 1024); // below even 512 MB in decimal units
        assert.equal(swap, memory);
        assert.equal(Number(cpu), 100000000);
        assert.equal(oom, 'false');
        const { stdout: peak } = await exec('docker', ['exec', id, 'cat', '/sys/fs/cgroup/memory.peak']);
        assert.ok(Number(peak) > 0 && Number(peak) < Number(memory));
        console.log(JSON.stringify({ memory_limit_bytes: Number(memory), memory_peak_bytes: Number(peak),
          cpu_limit: 0.1, swap: false, oom_killed: false }));
      },
      async stop() {
        // This image logs phase names only; PHP output and credentials are never logged.
        try {
          const logs = await exec('docker', ['logs', id]);
          console.log(logs.stdout + logs.stderr);
        } catch { /* preserve the test failure */ }
        try { await exec('docker', ['rm', '--force', id]); } finally { await rm(work, { recursive: true, force: true }); }
      },
    };
  } catch (error) {
    if (id) await exec('docker', ['rm', '--force', id]).catch(() => {});
    await rm(work, { recursive: true, force: true });
    throw error;
  }
}
