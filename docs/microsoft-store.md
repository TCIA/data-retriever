# Microsoft Store publishing

The [Microsoft Store workflow](../.github/workflows/microsoft-store.yml) builds the
Windows x64 Wails app, sets `main.distChannel=msstore`, packages an unsigned MSIX,
and optionally submits it to Store certification. The Store signs accepted
packages. A successful workflow submission does not mean certification has passed
or that the app is live.

## One-time Partner Center setup

1. Register a Windows developer account and reserve/create the **MSIX** app in
   Partner Center. Copy the exact **Package/Identity/Name**,
   **Package/Identity/Publisher**, **Package/Properties/PublisherDisplayName**, and
   Store product ID from the app's Product identity page. An existing MSI/EXE
   listing is a different publishing route.
2. Complete the listing, screenshots, privacy policy, age ratings, availability,
   and certification notes. Explain the `runFullTrust` capability: this desktop
   app downloads user-selected imaging data to a user-selected folder.
3. Associate a Microsoft Entra tenant with Partner Center, register an Entra
   application, add it under Partner Center's account user management with the
   Manager role, and create its client secret. Store automation requires an app
   already published and live; complete the first submission manually using the
   workflow's MSIX artifact. Follow Microsoft's
   [prerequisites](https://learn.microsoft.com/en-us/windows/apps/publish/msstore-dev-cli/github-actions).
4. Create a GitHub Actions environment named `microsoft-store`. Configure required
   reviewers if publication should require approval. Allow `main` and the release
   tag patterns used by maintainers in its deployment branch/tag rules.

## GitHub configuration

Set these **repository Actions variables** (identity is needed by the build job,
which does not use the publishing environment):

| Variable | Value |
| --- | --- |
| `MSSTORE_IDENTITY_NAME` | Partner Center's exact Package/Identity/Name |
| `MSSTORE_PUBLISHER` | Partner Center's exact Package/Identity/Publisher, including `CN=` |
| `MSSTORE_PUBLISHER_DISPLAY_NAME` | Partner Center's exact publisher display name |
| `MSSTORE_PRODUCT_ID` | Store product ID for this MSIX app |
| `MSSTORE_PUBLISH_ENABLED` | `true` to submit stable GitHub releases automatically; leave unset initially |

Set these **environment secrets** in `microsoft-store`:

| Secret | Value |
| --- | --- |
| `MSSTORE_TENANT_ID` | Entra tenant ID |
| `MSSTORE_SELLER_ID` | Partner Center seller ID |
| `MSSTORE_CLIENT_ID` | Entra application/client ID |
| `MSSTORE_CLIENT_SECRET` | Entra client secret **value**, not its ID |

No signing certificate is needed for Store submission. No credentials are used
by the package job or pull request runs. Rotate the Entra secret before expiry.

## Build, test, and submit

1. Run **Microsoft Store** from the Actions tab on `main`, enter a version such
   as `1.2.3.0`, and leave **publish** unchecked. Download the
   `microsoft-store-1.2.3.0` artifact. Without identity variables, build-only runs
   use a development identity; those packages are not suitable for Partner Center.
2. Use a separate locally signed copy to test installation, launch, manifest
   selection, downloads to a chosen folder, and uninstall on Windows x64. Test
   on a clean system with WebView2 as well as an upgrade from the live package.
   The unsigned artifact cannot be installed directly. Do not add a development
   certificate or signed test package to Store submissions.
3. For the initial listing, upload the unsigned MSIX manually in Partner Center.
   For later updates, run on `main` with **publish** checked; the publishing job
   uses the `microsoft-store` environment and submits to certification. Inspect
   Partner Center for certification results and availability.
4. Once configured and tested, set `MSSTORE_PUBLISH_ENABLED=true` to submit on
   published stable GitHub releases. Use numeric `vMAJOR.MINOR.PATCH` tags;
   the workflow maps these to `MAJOR.MINOR.PATCH.0`. Prereleases are skipped.

Versions must increase relative to the live Store version, each component must
be at most 65535, and the fourth component must be zero. Re-running a release
with the same version can be rejected. Avoid concurrent manual submissions in
Partner Center while automation runs; the workflow serializes its own runs.
Manual publishing is restricted to `main`. Merging the PR does not submit an app.

Packaging changes trigger a build-only PR check, including Windows SDK MakeAppx
schema/semantic validation. MakeAppx validation does not replace installation
smoke tests, Windows App Certification Kit checks, or Store certification.

## Runtime and package scope

The package targets Windows 10 version 2004 or newer, x64. It reuses
`build/appicon.png` for Store package assets. Wails is installed at the version
in `go.mod`, and the frontend uses its committed npm lockfile.

The app requires the [Microsoft WebView2 Runtime](https://v2.wails.io/docs/guides/windows/).
This build uses `-webview2 error` to report a missing runtime instead of trying
to install one inside the Store app. Document the runtime prerequisite in the
Store listing and test on the intended Windows versions. WebView2 is not bundled
in the MSIX. The app retains its in-app manifest picker; this package does not
register Windows file associations.

For a local build from a Windows developer shell with Go, Node, Wails, and the
Windows SDK installed:

```powershell
npm ci --prefix frontend
wails build -platform windows/amd64 -clean -webview2 error -ldflags '-X main.distChannel=msstore -X main.version=1.2.3.0'
./scripts/windows/package-store.ps1 -Version 1.2.3.0 `
  -IdentityName 'YOUR.PARTNER.CENTER.NAME' `
  -Publisher 'CN=YOUR-PARTNER-CENTER-PUBLISHER' `
  -PublisherDisplayName 'Your publisher display name'
```

Publishing uses Microsoft's maintained
[Store CLI setup action](https://github.com/microsoft/microsoft-store-apppublisher)
and [`msstore publish`](https://learn.microsoft.com/en-us/windows/apps/publish/msstore-dev-cli/commands#publish-command).
The older `microsoft/store-submission` action is deprecated.
