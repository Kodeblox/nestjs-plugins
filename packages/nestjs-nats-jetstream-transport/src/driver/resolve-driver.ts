/**
 * Lazy NATS driver resolution (spec 001, section 4.2.4).
 *
 * This module never statically imports either driver package: availability
 * is probed at startup (when `connect()` / `listen()` runs). The probe is
 * injectable, which keeps this unit-testable without a server.
 *
 * Interim limitation (handback finding 1): `client.ts` / `server.ts` still
 * statically import the legacy driver package, so on a legacy install the
 * process already evaluates that static require at load time. Until the
 * adapter phase removes those static imports (dynamic `import()` /
 * `loadPackage` per spec 001, section 4.2 rule 1), the 'neither installed'
 * and mismatch errors below are advisory-only there — they become
 * load-bearing once the adapter lands and the modern cell enforces
 * `scripts/check-no-static-nats` against dist.
 */

export type NatsDriverOption = 'legacy' | 'modern';

export const LEGACY_DRIVER_PACKAGE = 'nats';
/**
 * The modern connection package. Modern *detection* is keyed on this package
 * alone (spec 001, section 4.2.4): it provides `connect`, so an install with
 * only `@nats-io/jetstream` and no transport is not a usable modern driver.
 */
export const MODERN_CONNECT_PACKAGE = '@nats-io/transport-node';
export const MODERN_DRIVER_PACKAGES = [
  '@nats-io/transport-node',
  '@nats-io/jetstream',
] as const;

export interface NatsDriverResolution {
  driver: NatsDriverOption;
  legacyAvailable: boolean;
  modernAvailable: boolean;
  /**
   * Present when both drivers are installed and no explicit option picked
   * one. Resolution still succeeds (modern wins); callers should log this
   * so the ambiguity is visible at startup.
   */
  notice?: string;
}

export type DriverAvailabilityProbe = (packageName: string) => boolean;

declare const require: { resolve(request: string): string } | undefined;

function defaultProbe(packageName: string): boolean {
  try {
    if (typeof require === 'undefined' || !require.resolve) {
      return false;
    }
    require.resolve(packageName);
    return true;
  } catch {
    return false;
  }
}

function installationHint(): string {
  return (
    `Install one driver generation: ` +
    `'${LEGACY_DRIVER_PACKAGE}@^2' (legacy, for Nest <=11) or ` +
    `'${MODERN_DRIVER_PACKAGES.join(', ')}' (modern, for Nest 12).`
  );
}

export function resolveNatsDriver(
  explicit?: NatsDriverOption,
  isAvailable: DriverAvailabilityProbe = defaultProbe,
): NatsDriverResolution {
  const legacyAvailable = isAvailable(LEGACY_DRIVER_PACKAGE);
  const modernAvailable = isAvailable(MODERN_CONNECT_PACKAGE);

  if (explicit === 'legacy') {
    if (!legacyAvailable) {
      throw new Error(
        `NATS driver mismatch: options.driver is 'legacy' but package ` +
          `'${LEGACY_DRIVER_PACKAGE}' is not installed. Install it with ` +
          `'npm install ${LEGACY_DRIVER_PACKAGE}@^2', or switch options.driver ` +
          `to 'modern'.`,
      );
    }
    return { driver: 'legacy', legacyAvailable, modernAvailable };
  }

  if (explicit === 'modern') {
    if (!modernAvailable) {
      throw new Error(
        `NATS driver mismatch: options.driver is 'modern' but ` +
          `'${MODERN_CONNECT_PACKAGE}' is not installed. Install it with ` +
          `'npm install ${MODERN_DRIVER_PACKAGES.join(' ')}', or switch ` +
          `options.driver to 'legacy'.`,
      );
    }
    return { driver: 'modern', legacyAvailable, modernAvailable };
  }

  if (modernAvailable && legacyAvailable) {
    return {
      driver: 'modern',
      legacyAvailable,
      modernAvailable,
      notice:
        `Both NATS driver generations are installed ` +
        `('${LEGACY_DRIVER_PACKAGE}' and '${MODERN_DRIVER_PACKAGES[0]}'). ` +
        `Using 'modern'. Set options.driver explicitly to silence this notice.`,
    };
  }
  if (modernAvailable) {
    return { driver: 'modern', legacyAvailable, modernAvailable };
  }
  if (legacyAvailable) {
    return { driver: 'legacy', legacyAvailable, modernAvailable };
  }
  throw new Error(`No NATS driver found. ${installationHint()}`);
}
