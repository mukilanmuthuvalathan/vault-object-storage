import { timingSafeEqual } from 'node:crypto';

export function hasValidApiKey(authorization, expectedKey) {
  if (!expectedKey) return true;
  const supplied = typeof authorization === 'string' && authorization.startsWith('Bearer ')
    ? authorization.slice(7)
    : '';
  const expected = Buffer.from(expectedKey);
  const actual = Buffer.from(supplied);
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}
