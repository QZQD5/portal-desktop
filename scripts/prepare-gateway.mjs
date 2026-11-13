import { access, cp, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve('audio-agent-gateway');
const modulePath = path.join(root, 'gateway', '__main__.py');
await access(modulePath).catch(() => {
  throw new Error('Missing embedded Gateway at audio-agent-gateway/.');
});

if (process.argv.includes('--package')) {
  const destination = path.resolve('resources/audio-gateway');
  await rm(destination, { recursive: true, force: true });
  await mkdir(destination, { recursive: true });
  await cp(root, destination, {
    recursive: true,
    filter: (source) => {
      const relative = path.relative(root, source);
      if (!relative || relative === '.') return true;
      const parts = relative.split(path.sep);
      return !parts.some(part => ['.venv', '.git', '__pycache__', '.pytest_cache', '.ruff_cache', '.runtime',
        'test-results', 'node_modules', 'scripts'].includes(part) || part.endsWith('.egg-info'));
    },
  });
  console.log('Prepared packaged Python Gateway in resources/audio-gateway');
} else {
  console.log('Using embedded Python Gateway at audio-agent-gateway/');
}
