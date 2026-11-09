/**
 * Version compatibility (core §3.5).
 * @module version
 */

import type { Version } from './control/messages.js';

/** Core §3.5: 0.x needs major and minor equal; ≥1.x needs major equal. */
export function versionsCompatible(a: Version, b: Version): boolean {
  if (a[0] !== b[0]) return false;
  return a[0] !== 0 || a[1] === b[1];
}
