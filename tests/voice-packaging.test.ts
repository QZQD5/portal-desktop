import { afterEach, expect, it, vi } from 'vitest';
import path from 'node:path';
import { existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';

vi.mock('node:fs', async original => ({ ...await original<typeof import('node:fs')>(), existsSync: vi.fn() }));
vi.mock('node:child_process', async original => ({ ...await original<typeof import('node:child_process')>(), execFileSync: vi.fn() }));
afterEach(() => { vi.unstubAllEnvs(); vi.clearAllMocks(); });

it.skipIf(process.platform !== 'win32').each(['absent', 'prepared', 'failed'])(
  'requires the Gateway runtime and handles the %s build', async mode => {
    vi.resetModules();
    let ready = mode === 'prepared';
    const voiceDirectory = path.resolve('resources/audio-gateway');
    const binary = path.resolve('resources/heart-portal.exe');
    vi.mocked(existsSync).mockImplementation(file => {
      const value = String(file);
      if (value === binary) return true;
      if (value === path.join(voiceDirectory, 'AudioAgentGateway.exe')) return ready;
      if (value === path.join(voiceDirectory, 'gateway', '__main__.py')) return false;
      if (value === path.join(voiceDirectory, 'AudioAgentGateway')) return false;
      return true;
    });
    vi.mocked(execFileSync).mockImplementation((command, args) => {
      if (Array.isArray(args) && args[0] === 'scripts/check-desktop-dependencies.mjs') return Buffer.alloc(0);
      if (mode === 'failed') throw new Error('Fixture voice build failed');
      ready = true;
      return Buffer.alloc(0);
    });
    const config = (await import('../forge.config')).default;
    const prepare = config.hooks!.prePackage as () => Promise<void>;
    expect(config.packagerConfig!.extraResource).toContain(binary);
    expect(config.packagerConfig!.extraResource).toContain(voiceDirectory);
    if (mode === 'failed') await expect(prepare()).rejects.toThrow('Fixture voice build failed');
    else if (mode === 'absent') await expect(prepare()).rejects.toThrow('Python Gateway runtime missing');
    else await prepare();
    expect(execFileSync).toHaveBeenCalledWith(
      process.execPath,
      ['scripts/build-voice.mjs', '--source', path.resolve('audio-agent-gateway')],
      expect.objectContaining({ stdio: 'inherit' }),
    );
  },
);
