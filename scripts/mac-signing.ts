import { closeSync, openSync, readSync, statSync } from 'node:fs';

const machOMagic = new Set(['feedface', 'cefaedfe', 'feedfacf', 'cffaedfe', 'cafebabe', 'bebafeca', 'cafebabf', 'bfbafeca']);

export function ignoreMacSigningFile(file: string) {
  // Portal and the frozen Gateway are signed by their builders before their hashes are recorded.
  if (file.endsWith('/Contents/Resources/heart-portal') || file.includes('/Contents/Resources/audio-gateway/')) return true;
  if (statSync(file).isDirectory()) return false;
  // osx-sign also discovers binary data (.pak, images, ASAR). These are sealed
  // by their containing bundle; separate signatures use nonportable xattrs.
  const descriptor = openSync(file, 'r');
  try {
    const magic = Buffer.alloc(4);
    return readSync(descriptor, magic, 0, magic.length, 0) !== 4 || !machOMagic.has(magic.toString('hex'));
  } finally { closeSync(descriptor); }
}
