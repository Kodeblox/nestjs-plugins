nestjs-plugins

## Packages

- [nestjs-nats-jetstream-transport](https://github.com/Kodeblox/nestjs-plugins/tree/master/packages/nestjs-nats-jetstream-transport)

## Releasing

The only publishable package is `@kodeblox/nestjs-nats-jetstream-transport` in `packages/nestjs-nats-jetstream-transport`.

### One-time step after the package rename

Lerna derives the next version from the commits since the last tag that matches the package name.
The rename from `@nestjs-plugins/...` to `@kodeblox/...` left the new name without any tag, so Lerna falls back to the entire history of the package folder: a single `fix:` commit bumps `2.2.7` to `2.3.0` instead of `2.2.8`.

Create the missing baseline tag once, on a commit whose `package.json` still says `2.2.7`:

```bash
git tag -a @kodeblox/nestjs-nats-jetstream-transport@2.2.7 -m "@kodeblox/nestjs-nats-jetstream-transport@2.2.7"
git push origin @kodeblox/nestjs-nats-jetstream-transport@2.2.7
```

### Version bumps

`Bump versions` (`.github/workflows/pr-close.yaml`) runs on every push to `master` that touches the package.
It runs `lerna version --conventional-commits` and pushes the version commit and its tag, so the bump is derived from the commit messages: `fix:` is a patch, `feat:` is a minor, `feat!:` or `BREAKING CHANGE:` is a major.

### Publish

Create a GitHub Release for the version tag that the bump produced, or run the `Publish` workflow manually from the Actions tab.

The `Publish` workflow installs with `npm ci`, builds `dist`, and runs `npm stage publish` in the package directory on Node 24, which submits the version to npm's staging area instead of publishing it directly.
A maintainer then reviews and approves it with 2FA, either on npmjs.com under Staged Packages, or from the CLI:

```bash
npm stage list @kodeblox/nestjs-nats-jetstream-transport
npm stage view <stage-id>
npm stage approve <stage-id>   # prompts for the 2FA code
```

The version only becomes installable after that approval. `publishConfig.access` is `public`, and npm refuses to stage a version that already exists on the registry.

Authentication is whichever of these is configured:

1. **Trusted publishing (preferred).** On npmjs.com open the package, then Settings -> Trusted publishing -> Add trusted publisher -> GitHub Actions:

   - Organization or user: `Kodeblox`
   - Repository: `nestjs-plugins`
   - Workflow filename: `release.yaml`
   - Environment: leave empty
   - Allowed actions: leave `npm publish` unchecked and use `npm stage publish` only. Publishers created after Sep 3 2026 default to this, and it is the stricter option since every version needs a human approval.

   The workflow already requests `id-token: write`, so no secret is needed.
   Note that `npm stage list`, `view`, `approve` and `reject` cannot use OIDC, since approving requires proof of presence, so those stay interactive.

2. **Granular access token (fallback).** Create a granular access token on npmjs.com with Read and write access to the `@kodeblox` scope and Bypass 2FA enabled, then store it as the `NPM_TOKEN` repository secret:

   ```bash
   gh secret set NPM_TOKEN --repo Kodeblox/nestjs-plugins
   ```

   npm prefers trusted publishing when it is configured and falls back to `NPM_TOKEN` otherwise.
   npm is deprecating bypass-2FA tokens with direct-publish access: direct publishing with a granular token stops working in January 2027, so plan on migrating to trusted publishing.
   Once trusted publishing is confirmed working, npm recommends opening the package's Settings -> Publishing access and disallowing token publishing.

### Publish from your machine

```bash
npm run pack:jetstream   # builds and prints the exact tarball contents
npm login

# either publish directly
npm run publish:jetstream

# or stage it and approve it yourself with 2FA
cd packages/nestjs-nats-jetstream-transport
npm stage publish
npm stage approve <stage-id>
```

`npm stage publish` requires npm CLI 11.15.0 or later, and `npm stage list|view|approve|reject` always require interactive authentication.
