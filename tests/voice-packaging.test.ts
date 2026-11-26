import { afterEach, expect, it, vi } from 'vitest';
import path from 'node:path';
import { existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';

vi.mock('node:fs', async (original) => ({
  ...(await original<typeof import('node:fs')>()),
  existsSync: vi.fn(),
}));
vi.mock('node:child_process', async (original) => ({
  ...(await original<typeof import('node:child_process')>()),
  execFileSync: vi.fn(),
}));
afterEach(() => {
  vi.clearAllMocks();
});

it('prepares Portal Desktop without building or shipping a Python voice runtime', async () => {
  vi.mocked(existsSync).mockReturnValue(true);
  const config = (await import('../forge.config')).default;
  const resources = config.packagerConfig!.extraResource;
  expect(resources).toContain(
    path.resolve('resources', process.platform === 'win32' ? 'heart-portal.exe' : 'heart-portal'),
  );
  expect(resources).toContain(path.resolve('resources/portal-runtime'));
  expect(resources).not.toContain(path.resolve('resources/audio-gateway'));
  await (config.hooks!.prePackage as () => Promise<void>)();
  const scripts = vi.mocked(execFileSync).mock.calls.map(([, args]) => args?.[0]);
  expect(scripts).toContain('scripts/check-desktop-dependencies.mjs');
  expect(scripts).not.toContain('scripts/build-voice.mjs');
  expect(scripts).not.toContain('scripts/prepare-gateway.mjs');
});
