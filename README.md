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

The database stores metadata and user-created 3D asset thumbnails only. Original image, 3DGS, and GLB files stay in the picture root. Regular image thumbnails are generated on demand and cached in memory; captured 3D thumbnails are stored in SQLite because they represent a manually chosen camera view.

Current tables:

- `meta`: schema version and stable root id.
- `folders`: folder rows, parent path, and optional selected thumbnail image id.
- `images`: indexed image files, optional dimensions, file size, and modified time.
- `keywords`, `people`, `folder_keywords`, `folder_people`: controlled metadata tables for the folder inheritance model.
- `ratings`, `folder_ratings`, `image_keywords`, `image_people`, `image_ratings`: controlled metadata tables for ratings and image metadata.
- `splat_thumbnails`: captured thumbnails and camera state for 3DGS and GLB assets.

Picturious keeps the list of known root paths in the app config directory. At runtime the Rust core opens every connected root database and merges the results in memory, so disconnected external drives simply disappear from the combined library view.

Scans run in the background. The scanner writes one folder at a time, emits progress events, and lets the UI refresh visible folders while the scan is still running. Scanning is intentionally metadata-only: it records paths, file sizes, and modified times without decoding every image.

Removing a root from the app only removes it from the known-root list. The `.picturious/root.sqlite` file and pictures remain untouched.

Thumbnails are generated lazily for visible content only. Image tiles decode images from the folder currently open. Folder tiles decode one representative image: the selected folder image, otherwise the first direct image, otherwise the representative image from the first child folder. Generated image thumbnails are cached in memory with an LRU-style byte limit, and no thumbnail files are written into picture roots.

JPEG thumbnails use libjpeg-turbo through the `turbojpeg` crate. On Windows, the native libjpeg-turbo build needs NASM for SIMD; a portable NASM can be placed on `PATH` or pointed to with `CMAKE_ASM_NASM_COMPILER`.

## 3D Assets

Picturious indexes and opens 3D Gaussian Splatting assets alongside regular images. Supported 3DGS inputs include `.spz`, `.sog`, `.ply`, `.compressed.ply`, `.splat`, `.ksplat`, `.rad`, `.meta.json`, and `.lod-meta.json`. Picturious also indexes and opens `.glb` models.

The built-in viewer uses the bundled PlayCanvas runtime. Opening a 3DGS or GLB item switches the fullscreen viewer from the image element to a PlayCanvas canvas. GLB files are shown with simple studio lighting, while 3DGS files use the splat loader. Very large raw `.ply` files are guarded by a memory-safe path and may ask to be converted to SPZ, SOG, or compressed PLY for embedded viewing.

3D viewer controls:

- `T`: capture the current view as the asset thumbnail.
- `F`: frame the current asset in view.
- `R`: reset the current asset view.
- `O`: cycle 3DGS orientation presets.

The thumbnail capture workflow is manual by design. Open a 3DGS or GLB asset, move the camera to the desired composition, adjust 3DGS orientation if needed, then press `T`. Picturious captures the PlayCanvas canvas as a JPEG thumbnail and stores it in that root's `.picturious/root.sqlite` database. The grid tile updates immediately, and future thumbnail requests use the stored capture instead of the generic 3D placeholder.

Captured 3D thumbnails also store camera restoration data. The saved state records the camera position, focus point, field of view, asset kind, and the selected 3DGS orientation preset. When the asset is opened again, Picturious loads the stored camera state before falling back to the default framed view. If the source asset's modified timestamp changes, the stored thumbnail and camera state are treated as stale and removed so the next view starts fresh.

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
