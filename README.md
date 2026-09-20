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

1. Create an npm automation token that can publish to the `@kodeblox` scope and store it as the `NPM_TOKEN` repository secret.
2. Create a GitHub Release for the version tag that the bump produced, or run the `Publish` workflow manually from the Actions tab.

The `Publish` workflow installs with `npm ci` and runs `npm publish --workspace=@kodeblox/nestjs-nats-jetstream-transport`.
`prepack` builds `dist` first, `publishConfig.access` is `public`, and npm refuses to overwrite a version that already exists on the registry.

### Publish from your machine

```bash
npm run pack:jetstream   # builds and prints the exact tarball contents
npm login
npm run publish:jetstream
```
