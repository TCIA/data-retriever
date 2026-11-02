# Configure and publish TDR on PyPI

This is the setup/runbook for `.github/workflows/python-release.yml`, displayed
as **Publish Python release** in GitHub Actions. It does not publish on merge,
tag push, pull request, or desktop release. An authorized maintainer must start
it manually from `main`, select a merged version tag and choose the index.
The default index is TestPyPI. Production also needs the approval rule configured
below; YAML naming an environment alone does not create an approval rule.

Merge the packaging and controlled HTTP hardening PRs first, then this workflow
PR. This PR is initially stacked on the packaging branch; retarget it to `main`
after that dependency merges. No external account changes or uploads are made
by merging these files.

## One-time steps outside this repository

### 1. Establish package ownership

Choose two trusted project owners and identify the release maintainers. Each
person should create their own [PyPI account](https://pypi.org/account/register/),
verify their email, enable two-factor authentication and save recovery codes.
Create separate accounts on [TestPyPI](https://test.pypi.org/account/register/).
Those sites have separate accounts, projects and publishing configuration.

Confirm that **tcia-data-retriever** is available on both indexes, or that TCIA
already controls it. The distribution name is the value in `python/pyproject.toml`;
it is distinct from the import name `tcia_data_retriever`. If another party owns
it, stop and resolve ownership/name changes before configuring publishing.

You can use either individual collaborators or a PyPI organization:

- Without an organization, add a second trusted person as project **Owner** and
  other release developers as **Maintainers**, under project **Manage → Collaborators**.
  An Owner manages permissions; a Maintainer can upload releases.
- With a TCIA organization, create a release-maintainers team, assign it the
  appropriate project role and retain at least two trusted administrators.
  Organization accounts require an application; community eligibility and any
  applicable subscription are determined by PyPI. Pending-publisher first
  creation may initially be tied to its configuring user; transfer the created
  project into the approved organization if needed.

For a new project, configure a pending publisher as described in step 3. It
creates the project on first upload and does not reserve the name beforehand.
After first publication, add the remaining owners/team promptly. For an existing
project, only a project owner can configure its Trusted Publisher.

### 2. Configure GitHub release access and environments

A repository administrator must do this in **TCIA/data-retriever → Settings**:

1. Give the designated developers the repository access needed to run Actions.
   Set branch protection/rulesets on `main` to require reviewed PRs and passing
   checks. Limit changes to the publishing workflow through review policy.
2. Create two environments under **Environments**, spelled exactly `testpypi`
   and `pypi`. Create them explicitly before running the workflow.
3. For **pypi**, enable **Required reviewers** and select trusted release
   reviewers or the release team. Enable **Prevent self-review** so the person
   starting the release cannot also approve it. Disable administrator bypass
   where available. With this configuration, have at least two available people.
   GitHub's reviewer list normally requires approval by one listed reviewer,
   not approval by every member of a selected team.
4. For both environments, choose **Selected branches and tags** and permit the
   **branch `main` only**. The workflow runs from `main` and checks out the
   supplied tag for builds; its deployment ref is still `main`. Do not configure
   only `python-v*` as the environment's allowed deployment tag rule.
5. Protect tags matching **python-v*** with a repository tag ruleset: restrict
   tag creation to release maintainers and prevent updates/deletion. Keep
   release tags immutable. This prefix avoids the existing desktop `v*` trigger.
6. Check organization Actions policy permits the actions used in the workflow,
   including `pypa/gh-action-pypi-publish`. The workflow grants `id-token: write`
   only to the selected publishing job. No PyPI API-token secret is required.

Required reviewers are available for this public repository under GitHub's
documented plan rules. Recheck availability if the repository becomes private.
If your account/plan cannot configure the required approval rule, do not treat
this YAML as an approval gate; resolve that configuration before production use.

### 3. Register Trusted Publishers on both indexes

For a new project, use the account **Publishing** page:

- PyPI: <https://pypi.org/manage/account/publishing/>
- TestPyPI: <https://test.pypi.org/manage/account/publishing/>

For an existing project, use **Manage → Publishing → Add a new publisher**.
Choose GitHub and enter these exact values:

| Field | PyPI | TestPyPI |
| --- | --- | --- |
| Project name (new/pending only) | `tcia-data-retriever` | `tcia-data-retriever` |
| GitHub owner | `TCIA` | `TCIA` |
| Repository | `data-retriever` | `data-retriever` |
| Workflow filename | `python-release.yml` | `python-release.yml` |
| Environment | `pypi` | `testpypi` |

The authorized workflow is the top-level **python-release.yml**, which performs
the upload. Do not enter `python-wheels.yml`, `release.yml`, its display name,
or the full `.github/workflows/` path. Do not leave the environment field empty.
No shared password or manually generated PyPI token is needed.

Review configured publishers when a maintainer leaves; removing their individual
PyPI role does not remove a workflow's publishing authority. Review GitHub team,
workflow-editing and environment-reviewer access at the same time.

## First release and subsequent releases

1. Confirm the reviewed changes are merged into `main`, including the publishing
   workflow. The workflow cannot be manually launched normally until it exists
   on the default branch. Check the platform wheel CI and complete the bounded
   Colab and authorized controlled-download smoke checks described in the package
   release notes. Fixture tests alone do not verify live controlled access.
2. Choose a canonical PEP 440 version, such as `1.0.0rc1` for a release candidate.
   Create an annotated tag such as `python-v1.0.0rc1` on the reviewed merged
   commit and push it. The workflow rejects tags absent from the repository,
   commits outside `origin/main`, dev/local versions and noncanonical spellings.
   The tag must include this release-validation implementation.
3. Open **Actions → Publish Python release → Run workflow**. Choose branch
   **main**, enter that tag and choose **testpypi**. This builds and tests all four
   wheels, validates release identity, hashes and package metadata, and uploads
   only after all platform jobs succeed. Download `validated-release-wheels`
   and `release-checksums` from that run to inspect the artifacts.
4. Test installation from TestPyPI in a fresh environment:

   ```bash
   python -m pip install --index-url https://test.pypi.org/simple/ --only-binary=:all: --no-deps tcia-data-retriever==1.0.0rc1
   tcia-data-retriever --version
   ```

   The package has no runtime Python dependencies, so no extra package index is
   required. Check the printed version and `build_info()` source revision.
5. For production, run **Publish Python release** again from **main**, select
   the **same immutable tag**, and choose **pypi**. This is a fresh native build
   and full test run from that source commit; it does not promote or download
   the prior TestPyPI artifacts, so the two runs are not claimed byte-identical.
   Inspect this run's validated artifacts/checksums and version/source revision
   before a second person approves the `pypi` environment deployment. The upload
   uses these exact tested production-run artifacts without rebuilding them in
   the privileged upload job.
6. Verify production installation in a fresh environment:

   ```bash
   python -m pip install --only-binary=:all: tcia-data-retriever==1.0.0rc1
   tcia-data-retriever --version
   ```

   Confirm the four files and provenance on the PyPI project page. Release
   candidates require an explicit version or `--pre`; choose a final version
   such as `1.0.0` and a new tag when ready for default stable installs. Once
   compatible wheels are published, proceed with the separately tracked
   `tcia_utils` optional-extra integration.

Neither index permits overwriting an existing distribution filename. The workflow
does not use `skip-existing`: an attempted duplicate fails visibly. If an upload
partially succeeds, inspect index files before retrying; normally make a corrected
version and new tag rather than deleting/reusing files or moving a tag. Never
increment the package version while silently keeping the old tag/source identity.

Each release is currently roughly 360 MB across four wheels. The existing
95,000,000-byte per-wheel budget remains enforced; monitor the project's total
storage as versions accumulate and request a PyPI increase when justified.
Keep the publication job logs, artifact checksums and source tag as the release
record. GitHub Actions artifact retention is temporary; the index retains the
published distribution files.

## Troubleshooting

- **No Run workflow button:** merge the workflow to the default branch first
  and check repository Actions permissions.
- **Pending approval never appears:** ensure the `pypi` environment has required
  reviewers configured; an environment name in YAML alone is not protection.
- **OIDC/trusted-publisher rejection:** compare owner, repository, workflow
  filename and environment exactly with step 3; PyPI and TestPyPI are separate.
- **Tag not merged / invalid version:** use an immutable `python-v...` tag on a
  reviewed commit already reachable from `main`.
- **Duplicate filename:** check whether that index already contains the release;
  choose a new version for corrections rather than silently skipping files.
- **Unsupported platform / missing wheel:** the workflow requires exactly Linux
  x86-64, Windows x86-64 and both macOS architectures before any publication.

## Official setup references

- [PyPI collaborators and roles](https://docs.pypi.org/project-management/managing-collaborators/)
- [PyPI organization roles](https://docs.pypi.org/organization-accounts/roles-entities/)
- [Creating a project with a pending Trusted Publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
- [Publishing with Trusted Publishers](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
- [Trusted Publishing security considerations](https://docs.pypi.org/trusted-publishers/security-model/)
- [GitHub deployment environments and reviewers](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)
- [PyPI storage limits](https://docs.pypi.org/project-management/storage-limits/)
