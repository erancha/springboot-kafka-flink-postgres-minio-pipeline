// Stages the backend schema into public/ for local dev/test/build. Inside Docker the backend source
// is absent (the frontend build context excludes ../backend), so this no-ops and relies on the copy
// scripts/build.sh stages instead.
import { existsSync, mkdirSync, copyFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const source = resolve(here, '../../backend/src/main/resources/eventtype/event-payload-schema.json');
const dest = resolve(here, '../public/event-payload-schema.json');

if (!existsSync(source)) {
  process.exit(0);
}
mkdirSync(dirname(dest), { recursive: true });
copyFileSync(source, dest);
console.log(`synced payload schema -> ${dest}`);
