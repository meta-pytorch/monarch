# Publishing Monarch

All PyPI uploads use `.github/workflows/publish_release.yml`. PyPI's provenance
checks compare the source repository and publishing workflow filename between
releases, including nightlies. Keep uploads in this workflow so switching between
nightly and versioned builds does not repeatedly trigger a change-of-provenance warning.
Trusted publishing and attestations remain enabled for both kinds of release.

## Trusted publisher migration

Before landing this workflow change, a PyPI project owner must add the following
GitHub trusted publisher at
<https://pypi.org/manage/project/torchmonarch/settings/publishing/>:

| Field | Value |
| --- | --- |
| Owner | `meta-pytorch` |
| Repository | `monarch` |
| Workflow filename | `publish_release.yml` |
| Environment | `nightly` |

Keep the existing `publish_release.yml` / `stable` publisher. The separate GitHub
`stable` and `nightly` environments retain their existing protection rules.
PyPI's change-of-provenance comparison uses the workflow filename, not the
environment, so these publishers produce the same comparison identity.

After the first successful nightly upload through `publish_release.yml`, remove
the obsolete `wheels.yml` / `nightly` trusted publisher from PyPI. The old workflow
has been removed from this repository.

Existing releases and their signed provenance cannot be rewritten by this
workflow change. Historical warnings remain; a transition warning can also
appear if a new release is compared against an old `wheels.yml` upload.
Subsequent releases compared against `publish_release.yml` uploads use a
consistent workflow identity. Explain the intentional migration in the next
release notes.

Suggested release note:

> Monarch's nightly and versioned releases now publish through the same GitHub
> Actions workflow, `publish_release.yml`, preserving the publishing identity of
> versioned releases. Earlier nightlies used `wheels.yml`, which caused PyPI to
> flag the workflow identity change when comparing them with versioned releases.
> This is an intentional publishing configuration change. Trusted publishing and
> signed attestations remain enabled.

See [PyPI's provenance documentation](https://docs.pypi.org/using-pypi/package-security/provenance/)
and [its comparison implementation](https://github.com/pypi/warehouse/blob/main/warehouse/attestations/models.py).

## Nightly builds

The workflow runs daily at midnight UTC. To run it manually, select **Build and
publish monarch wheels** in GitHub Actions and leave `version` empty.

Nightly builds use nightly PyTorch, publish wheels under the `nightly`
environment, and update the `monarch-nightly` container image. Pull requests and
workflow changes on `main` build wheels without publishing.

## Versioned releases

Run **Build and publish monarch wheels** manually with:

- `version`: the exact release version, such as `0.7.0rc1` or `0.7.0`.
- `is_prerelease`: checked for release candidates and other prereleases.

Versioned builds use pinned stable PyTorch and publish wheels under the `stable`
environment. After PyPI publishing succeeds, the workflow publishes the versioned
container image. When `is_prerelease` is false, it also updates the container's
`latest` tag and creates the `docs/v<version>` branch.

The existing CLI workflow filename for versioned releases is unchanged:

```sh
gh workflow run publish_release.yml --repo meta-pytorch/monarch --ref main \
  -f version=0.7.0rc1 -f is_prerelease=true
```
