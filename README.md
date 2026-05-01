# Picturious

Picturious is a folder-first picture library manager for Windows, built with a Rust core and a Tauri desktop shell.

## Shape

- `crates/picturious-core`: library/domain code for roots, folders, inherited metadata, and later indexing/database logic.
- `src-tauri`: native desktop app wrapper and Rust commands exposed to the UI.
- `ui`: static HTML/CSS/JS frontend embedded by Tauri. No Node-based frontend build is required at this stage.

## Root Data

Each picture root owns its own SQLite database:

```text
PictureRoot/
  .picturious/
    root.sqlite
```

The database stores metadata only. Images and generated thumbnails are not stored in SQLite.

Current tables:

- `meta`: schema version and stable root id.
- `folders`: folder rows, parent path, and optional selected thumbnail image id.
- `images`: indexed image files, optional dimensions, file size, and modified time.
- `keywords`, `people`, `folder_keywords`, `folder_people`: controlled metadata tables for the folder inheritance model.

Picturious keeps the list of known root paths in the app config directory. At runtime the Rust core opens every connected root database and merges the results in memory, so disconnected external drives simply disappear from the combined library view.

Scans run in the background. The scanner writes one folder at a time, emits progress events, and lets the UI refresh visible folders while the scan is still running. Scanning is intentionally metadata-only: it records paths, file sizes, and modified times without decoding every image.

Removing a root from the app only removes it from the known-root list. The `.picturious/root.sqlite` file and pictures remain untouched.

Thumbnails are generated lazily for visible content only. Image tiles decode images from the folder currently open. Folder tiles decode one representative image: the selected folder image, otherwise the first direct image, otherwise the representative image from the first child folder. Generated thumbnails are cached in memory with an LRU-style byte limit, and no thumbnail files are written into picture roots.

JPEG thumbnails use libjpeg-turbo through the `turbojpeg` crate. On Windows, the native libjpeg-turbo build needs NASM for SIMD; a portable NASM can be placed on `PATH` or pointed to with `CMAKE_ASM_NASM_COMPILER`.

## First Run

Tauri dependencies are Rust crates. If they are not cached locally yet, fetch/build once with network access:

```powershell
$env:CARGO_NET_OFFLINE='false'
cargo fetch
```

Install the Tauri CLI once:

```powershell
$env:CARGO_NET_OFFLINE='false'
cargo install tauri-cli --version "^2"
```

Then run the app:

```powershell
cargo tauri dev
```

If the CLI does not find the app from the workspace root, run the same command from `src-tauri`.

## Windows Installer

Picturious uses Tauri's NSIS bundler for release installers. The installer is configured as a per-machine Windows installer, so the default install location is under `Program Files`, with Start Menu integration, optional desktop shortcut creation, and an uninstaller entry.

Build the installer from the workspace root:

```powershell
cargo tauri build --bundles nsis
```

The release artifact is written below:

```text
target/release/bundle/nsis/
```

Upload the generated `*-setup.exe` file to a GitHub Release. Until the installer is code-signed, Windows SmartScreen may warn users that the publisher is unknown.
