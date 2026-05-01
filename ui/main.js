const tauri = window.__TAURI__;
const invoke = tauri?.core?.invoke;
const listen = tauri?.event?.listen;
const convertFileSrc = tauri?.core?.convertFileSrc;
const BASE_TILE_SIZE = 188;
const THUMBNAIL_PIXEL_SIZE = 450;
const STREAM_ITEMS_PER_FRAME = 16;

const gridNode = document.querySelector("#content-grid");
const statusNode = document.querySelector("#status");
const titleNode = document.querySelector("#view-title");
const breadcrumbsNode = document.querySelector("#breadcrumbs");
const addRootButton = document.querySelector("#add-root-button");
const settingsButton = document.querySelector("#settings-button");
const scanButton = document.querySelector("#scan-button");
const upButton = document.querySelector("#up-button");
const thumbScaleInput = document.querySelector("#thumb-scale");
const viewer = document.querySelector("#viewer");
const viewerImage = document.querySelector("#viewer-image");
const viewerCloseHotspot = document.querySelector("#viewer-close-hotspot");
const thumbContextMenu = document.querySelector("#thumb-context-menu");
const settingsDialog = document.querySelector("#settings-dialog");
const settingsCloseButton = document.querySelector("#settings-close-button");
const upscaleFullscreenInput = document.querySelector("#upscale-fullscreen");
const slideshowSpeedInput = document.querySelector("#slideshow-speed");
const slideshowSpeedValue = document.querySelector("#slideshow-speed-value");
const addExternalViewerButton = document.querySelector("#add-external-viewer-button");
const externalViewersList = document.querySelector("#external-viewers-list");

const state = {
  roots: [],
  currentRootId: null,
  currentPath: "",
  currentView: null,
  atRootOverview: true,
  viewerIndex: 0,
  activeScans: new Set(),
  viewScrollPositions: new Map(),
  pendingScrollRestore: null,
  viewGeneration: 0,
  folderRequestId: 0,
  activeFolderRequestId: null,
  folderLoading: false,
  streamRenderQueue: [],
  streamRenderScheduled: false,
  streamFinishedPayload: null,
  viewerGeneration: 0,
  imageUrlCache: new Map(),
  lastWheelAt: 0,
  contextMenuImage: null,
  contextMenuRoot: null,
  slideshowTimer: null,
  slideshowActive: false,
  settings: {
    upscale_fullscreen_images: false,
    slideshow_speed_seconds: 3,
    external_viewers: [],
  },
  thumbScale: 1,
  tileSize: BASE_TILE_SIZE,
  thumbScaleSaveTimer: null,
};

const thumbnailQueue = {
  active: 0,
  maxActive: 3,
  items: [],
};
const observedThumbs = new Set();
const thumbnailObserver =
  "IntersectionObserver" in window
    ? new IntersectionObserver(handleThumbnailIntersection, {
        root: null,
        rootMargin: "700px 0px",
        threshold: 0.01,
      })
    : null;

addRootButton.addEventListener("click", addRoot);
settingsButton.addEventListener("click", openSettingsDialog);
scanButton.addEventListener("click", scanCurrentRoot);
upButton.addEventListener("click", openParentFolder);
thumbScaleInput.addEventListener("input", handleThumbScaleInput);
settingsCloseButton.addEventListener("click", closeSettingsDialog);
upscaleFullscreenInput.addEventListener("change", handleSettingsInput);
slideshowSpeedInput.addEventListener("input", handleSlideshowSpeedInput);
slideshowSpeedInput.addEventListener("change", handleSettingsInput);
addExternalViewerButton.addEventListener("click", addExternalViewer);
viewerCloseHotspot.addEventListener("click", closeViewer);
viewer.addEventListener("wheel", handleViewerWheel, { passive: false });
document.addEventListener("fullscreenchange", handleBrowserFullscreenChange);
document.addEventListener("contextmenu", handleDocumentContextMenu);
document.addEventListener("click", handleDocumentClick);
window.addEventListener("blur", hideThumbContextMenu);
window.addEventListener("resize", hideThumbContextMenu);
thumbContextMenu.addEventListener("click", handleThumbContextAction);

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !thumbContextMenu.classList.contains("hidden")) {
    hideThumbContextMenu();
    return;
  }

  if (viewer.classList.contains("hidden")) {
    return;
  }

  if (event.key === "Escape") {
    closeViewer();
  } else if (event.key === "ArrowLeft") {
    stopSlideshow();
    moveViewer(-1);
  } else if (event.key === "ArrowRight") {
    stopSlideshow();
    moveViewer(1);
  } else if (event.key === " " || event.key === "Spacebar") {
    event.preventDefault();
    toggleSlideshow();
  }
});

init().catch(showError);

async function init() {
  if (!invoke) {
    applyThumbScale(1);
    statusNode.textContent = "Static preview. Run with Tauri to call the Rust core.";
    renderStaticPreview();
    return;
  }

  await loadAppSettings();
  await wireScanEvents();
  await refreshOverview();
  openRootOverview();
}

async function loadAppSettings() {
  try {
    const settings = await invoke("app_settings");
    state.settings = normalizeAppSettings(settings);
    applyThumbScale(settings?.thumb_scale ?? 1);
    applyViewerUpscaleSetting();
    renderSettingsDialog();
  } catch (error) {
    console.warn("Could not load app settings", error);
    state.settings = normalizeAppSettings(null);
    applyThumbScale(1);
    applyViewerUpscaleSetting();
    renderSettingsDialog();
  }
}

async function wireScanEvents() {
  if (!listen) {
    return;
  }

  await listen("folder-view-started", ({ payload }) => {
    if (payload.request_id !== state.activeFolderRequestId) {
      return;
    }

    startStreamedFolderView(payload.view);
  });

  await listen("folder-view-batch", ({ payload }) => {
    if (payload.request_id !== state.activeFolderRequestId || !state.currentView) {
      return;
    }

    appendStreamedFolderBatch(payload);
  });

  await listen("folder-view-finished", ({ payload }) => {
    if (payload.request_id !== state.activeFolderRequestId || !state.currentView) {
      return;
    }

    finishStreamedFolderView(payload);
  });

  await listen("folder-view-error", ({ payload }) => {
    if (payload.request_id !== state.activeFolderRequestId) {
      return;
    }

    state.folderLoading = false;
    showError(payload.message);
  });

  await listen("scan-progress", ({ payload }) => {
    const wasActive = state.activeScans.has(payload.root_id);
    state.activeScans.add(payload.root_id);
    if (!wasActive) {
      renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
    }
    if (payload.root_id === state.currentRootId) {
      setStatus(
        `Scanning: ${payload.folders_seen} folders, ${payload.images_seen} images`,
      );
    } else if (state.atRootOverview) {
      setStatus(
        `Scanning: ${payload.folders_seen} folders, ${payload.images_seen} images`,
      );
    }
  });

  await listen("scan-finished", async ({ payload }) => {
    state.activeScans.delete(payload.root_id);
    await refreshOverview();
    if (payload.root_id === state.currentRootId) {
      await refreshCurrentFolder({ keepStatus: true });
      setStatus(
        `Scan complete: ${payload.folders_seen} folders, ${payload.images_seen} images`,
      );
    } else if (state.atRootOverview) {
      renderRootOverview({ keepStatus: true, keepScroll: true });
      setStatus(
        `Scan complete: ${payload.folders_seen} folders, ${payload.images_seen} images`,
      );
    }
  });

  await listen("scan-error", ({ payload }) => {
    state.activeScans.delete(payload.root_id);
    renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
    if (payload.root_id === state.currentRootId) {
      setStatus(payload.message);
    } else if (state.atRootOverview) {
      setStatus(payload.message);
    }
  });
}

async function refreshOverview() {
  const overview = await invoke("library_overview");
  state.roots = overview.roots;
  renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
}

async function addRoot() {
  if (!invoke) {
    return;
  }

  const path = await invoke("pick_root_folder");
  if (!path) {
    return;
  }

  setStatus("Adding root...");
  const root = await invoke("add_root", { path });
  await refreshOverview();
  openRootOverview({ keepStatus: true });
  await startScan(root.id);
}

async function scanCurrentRoot() {
  if (!invoke || !state.currentRootId) {
    return;
  }

  await startScan(state.currentRootId);
}

async function startScan(rootId) {
  const root = state.roots.find((item) => item.id === rootId);
  if (!root?.connected) {
    return;
  }

  state.activeScans.add(rootId);
  pauseThumbnailWorkForRoot(rootId);
  renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
  scanButton.disabled = true;
  setStatus(`Scanning ${root.display_name}...`);
  let started;
  try {
    started = await invoke("start_scan", { rootId });
  } catch (error) {
    state.activeScans.delete(rootId);
    renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
    throw error;
  }
  if (!started) {
    setStatus(`${root.display_name} is already scanning`);
  }
}

function pauseThumbnailWorkForRoot(rootId) {
  thumbnailQueue.items = thumbnailQueue.items.filter((job) => job.rootId !== rootId);
  resetThumbnailWork();
}

async function removeRoot(rootId) {
  const root = state.roots.find((item) => item.id === rootId);
  if (!root) {
    return;
  }

  const confirmed = window.confirm(
    `Remove ${root.display_name} from Picturious? The root database and pictures are not deleted.`,
  );
  if (!confirmed) {
    return;
  }

  const overview = await invoke("remove_root", { rootId });
  state.roots = overview.roots;
  state.activeScans.delete(rootId);

  if (state.currentRootId === rootId) {
    openRootOverview({ keepStatus: true });
    setStatus("Root removed");
    return;
  }

  if (state.atRootOverview) {
    renderRootOverview({ keepStatus: true, keepScroll: true });
    setStatus("Root removed");
  }
}

async function openFolder(rootId, relativePath, options = {}) {
  const root = state.roots.find((item) => item.id === rootId);
  if (!root?.connected) {
    renderEmptyState("Root is not connected");
    return;
  }

  rememberCurrentScrollPosition();

  state.currentRootId = rootId;
  state.currentPath = relativePath ?? "";
  state.atRootOverview = false;
  const requestId = ++state.folderRequestId;
  state.activeFolderRequestId = requestId;
  state.folderLoading = true;
  resetStreamRenderQueue();
  prepareScrollRestore(rootId, state.currentPath, options);
  state.currentView = {
    root_id: rootId,
    root_display_name: root.display_name,
    folder_id: null,
    relative_path: state.currentPath,
    parent_relative_path: parentPathFor(state.currentPath),
    folders: [],
    images: [],
  };
  state.imageUrlCache.clear();
  resetThumbnailWork();
  renderPendingFolderView(state.currentView, options);
  await nextFrame();
  if (requestId !== state.activeFolderRequestId) {
    return;
  }

  invoke("stream_folder_view", {
    rootId,
    relativePath: state.currentPath,
    requestId,
  }).catch((error) => {
    if (requestId === state.activeFolderRequestId) {
      state.folderLoading = false;
      showError(error);
    }
  });
}

async function refreshCurrentFolder(options = {}) {
  if (!state.currentRootId) {
    return;
  }

  try {
    await openFolder(state.currentRootId, state.currentPath, options);
  } catch (error) {
    if (!options.quiet) {
      showError(error);
    }
  }
}

function openParentFolder() {
  if (state.atRootOverview) {
    return;
  }

  if (!state.currentRootId || !state.currentView || state.currentView.parent_relative_path === null) {
    openRootOverview();
    return;
  }

  openFolder(state.currentRootId, state.currentView.parent_relative_path ?? "").catch(showError);
}

function openRootOverview(options = {}) {
  rememberCurrentScrollPosition();
  state.atRootOverview = true;
  state.currentRootId = null;
  state.currentPath = "";
  state.currentView = null;
  state.activeFolderRequestId = null;
  state.folderLoading = false;
  resetStreamRenderQueue();
  resetThumbnailWork();
  prepareScrollRestore(null, "", options);
  renderRootOverview(options);
}

function renderRootOverviewIfVisible(options = {}) {
  if (state.atRootOverview) {
    renderRootOverview(options);
  }
}

function renderRootOverview(options = {}) {
  state.atRootOverview = true;
  titleNode.textContent = "Picturious";
  if (!options.keepStatus) {
    setStatus(rootOverviewStatus());
  }
  upButton.disabled = true;
  scanButton.disabled = true;
  breadcrumbsNode.replaceChildren();

  if (state.roots.length === 0) {
    renderEmptyState("No roots", { keepBreadcrumbs: true });
    return;
  }

  const nodes = state.roots.map(renderRootCard);
  gridNode.replaceChildren(...nodes);
  if (options.resetScroll) {
    gridNode.scrollTop = 0;
    gridNode.scrollLeft = 0;
  } else {
    restorePendingScrollPosition();
  }
}

function renderRootCard(root) {
  const card = document.createElement("article");
  card.className = "tile root-tile";
  card.tabIndex = root.connected ? 0 : -1;
  card.title = root.path;
  card.dataset.rootId = root.id;
  card.dataset.connected = String(root.connected);
  card.innerHTML = `
    <div class="thumb root-thumb">
      <span>${escapeHtml(initials(root.display_name))}</span>
    </div>
    <div class="tile-body">
      <h3>${escapeHtml(root.display_name)}</h3>
      <p>${escapeHtml(rootStatus(root))}</p>
    </div>
  `;

  const thumb = card.querySelector(".thumb");
  sizeTile(card);
  thumb.title = root.path;
  if (root.connected && root.thumbnail_image_id) {
    requestThumbnailWhenVisible(root.id, root.thumbnail_image_id, thumb, THUMBNAIL_PIXEL_SIZE);
  }

  if (root.connected) {
    card.addEventListener("click", () => {
      openFolder(root.id, "").catch(showError);
    });
    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        openFolder(root.id, "").catch(showError);
      }
    });
  }

  return card;
}

function rootOverviewStatus() {
  if (state.roots.length === 0) {
    return "Add a picture root to begin";
  }

  const connectedCount = state.roots.filter((root) => root.connected).length;
  return `${connectedCount} of ${state.roots.length} roots connected`;
}

function rootStatus(root) {
  if (state.activeScans.has(root.id)) {
    return "Scanning";
  }
  if (!root.connected) {
    return "Missing";
  }
  return `${root.folder_count} folders, ${root.image_count} images`;
}

function renderFolderView(view, options = {}) {
  const root = currentRoot();
  const title = view.relative_path || root.display_name;
  titleNode.textContent = title;
  if (!options.keepStatus) {
    setStatus(
      `${view.folders.length} folders, ${view.images.length} images in this folder`,
    );
  }
  upButton.disabled = false;
  scanButton.disabled = state.activeScans.has(view.root_id);
  renderBreadcrumbs(view);

  const nodes = [
    ...view.folders.map(renderFolderCard),
    ...view.images.map((image, index) => renderImageCard(image, index)),
  ];

  if (nodes.length === 0) {
    renderEmptyState("Empty folder", { keepBreadcrumbs: true });
  } else {
    gridNode.replaceChildren(...nodes);
  }
}

function renderPendingFolderView(view, options = {}) {
  const root = currentRoot();
  titleNode.textContent = view.relative_path || root.display_name;
  if (!options.keepStatus) {
    const scanning = state.activeScans.has(view.root_id);
    setStatus(scanning ? "Loading indexed folders while scan continues..." : "Loading folder...");
  }
  upButton.disabled = false;
  scanButton.disabled = state.activeScans.has(view.root_id);
  renderBreadcrumbs(view);
  gridNode.replaceChildren();
  restorePendingScrollPosition();
}

function startStreamedFolderView(header) {
  resetStreamRenderQueue();
  state.currentView = {
    ...header,
    folders: [],
    images: [],
  };
  state.currentPath = header.relative_path;
  titleNode.textContent = header.relative_path || header.root_display_name;
  upButton.disabled = false;
  scanButton.disabled = state.activeScans.has(header.root_id);
  renderBreadcrumbs(state.currentView);
  gridNode.replaceChildren();
  const scanning = state.activeScans.has(header.root_id);
  setStatus(scanning ? "Loading indexed folders while scan continues..." : "Loading folder...");
  restorePendingScrollPosition();
}

function appendStreamedFolderBatch(payload) {
  for (const folder of payload.folders ?? []) {
    state.streamRenderQueue.push({ type: "folder", item: folder });
  }

  for (const image of payload.images ?? []) {
    state.streamRenderQueue.push({ type: "image", item: image });
  }

  scheduleStreamRender();
}

function finishStreamedFolderView(payload) {
  state.folderLoading = false;
  if (state.streamRenderQueue.length > 0) {
    state.streamFinishedPayload = payload;
    scheduleStreamRender();
    return;
  }

  completeStreamedFolderView(payload);
}

function resetStreamRenderQueue() {
  state.streamRenderQueue = [];
  state.streamFinishedPayload = null;
}

function scheduleStreamRender() {
  if (state.streamRenderScheduled) {
    return;
  }

  state.streamRenderScheduled = true;
  requestAnimationFrame(flushStreamRenderQueue);
}

function flushStreamRenderQueue() {
  state.streamRenderScheduled = false;
  if (!state.currentView) {
    resetStreamRenderQueue();
    return;
  }

  const fragment = document.createDocumentFragment();
  let rendered = 0;
  while (rendered < STREAM_ITEMS_PER_FRAME && state.streamRenderQueue.length > 0) {
    const next = state.streamRenderQueue.shift();
    if (next.type === "folder") {
      state.currentView.folders.push(next.item);
      fragment.append(renderFolderCard(next.item));
    } else {
      const index = state.currentView.images.length;
      state.currentView.images.push(next.item);
      fragment.append(renderImageCard(next.item, index));
    }
    rendered += 1;
  }

  if (fragment.childNodes.length > 0) {
    gridNode.append(fragment);
    restorePendingScrollPosition();
    setStatus(
      `${state.currentView.folders.length} folders, ${state.currentView.images.length} images loaded`,
    );
  }

  if (state.streamRenderQueue.length > 0) {
    scheduleStreamRender();
    return;
  }

  if (state.streamFinishedPayload) {
    const payload = state.streamFinishedPayload;
    state.streamFinishedPayload = null;
    completeStreamedFolderView(payload);
  }
}

function completeStreamedFolderView(payload) {
  if (state.currentView.folders.length === 0 && state.currentView.images.length === 0) {
    renderEmptyState("Empty folder", { keepBreadcrumbs: true });
  }
  restorePendingScrollPosition();
  setStatus(`${payload.folder_count} folders, ${payload.image_count} images in this folder`);
}

function handleThumbScaleInput() {
  applyThumbScale(Number(thumbScaleInput.value));
  scheduleThumbScaleSave();
}

function applyThumbScale(value) {
  const scale = clampThumbScale(value);
  const tileSize = Math.round(BASE_TILE_SIZE * scale);
  state.thumbScale = scale;
  state.tileSize = tileSize;
  document.documentElement.style.setProperty("--tile-width", `${tileSize}px`);
  document.documentElement.style.setProperty("--thumb-height", `${tileSize}px`);
  gridNode.querySelectorAll(".tile").forEach((tile) => sizeTile(tile));
  if (Math.abs(Number(thumbScaleInput.value) - scale) > 0.001) {
    thumbScaleInput.value = String(scale);
  }
}

function sizeTile(tile) {
  tile.style.width = `${state.tileSize}px`;
  tile.style.minWidth = `${state.tileSize}px`;
  tile.style.maxWidth = `${state.tileSize}px`;
  tile.style.flexBasis = `${state.tileSize}px`;
  const thumb = tile.querySelector(".thumb");
  if (thumb) {
    thumb.style.height = `${state.tileSize}px`;
    thumb.style.minHeight = `${state.tileSize}px`;
  }
}

function scheduleThumbScaleSave() {
  if (!invoke) {
    return;
  }

  if (state.thumbScaleSaveTimer) {
    window.clearTimeout(state.thumbScaleSaveTimer);
  }

  state.thumbScaleSaveTimer = window.setTimeout(() => {
    state.thumbScaleSaveTimer = null;
    invoke("save_thumb_scale", { thumbScale: state.thumbScale }).catch(showError);
  }, 300);
}

function normalizeAppSettings(settings) {
  return {
    upscale_fullscreen_images: Boolean(settings?.upscale_fullscreen_images),
    slideshow_speed_seconds: clampSlideshowSpeed(
      Number(settings?.slideshow_speed_seconds ?? 3),
    ),
    external_viewers: Array.isArray(settings?.external_viewers)
      ? settings.external_viewers
          .filter((viewer) => viewer?.id && viewer?.path)
          .map((viewer) => ({
            id: String(viewer.id),
            name: String(viewer.name || "External viewer"),
            path: String(viewer.path),
          }))
      : [],
  };
}

function openSettingsDialog() {
  renderSettingsDialog();
  if (!settingsDialog.open) {
    settingsDialog.showModal();
  }
}

function closeSettingsDialog() {
  settingsDialog.close();
}

function renderSettingsDialog() {
  upscaleFullscreenInput.checked = state.settings.upscale_fullscreen_images;
  slideshowSpeedInput.value = String(state.settings.slideshow_speed_seconds);
  slideshowSpeedValue.value = `${state.settings.slideshow_speed_seconds.toFixed(1)} s`;
  externalViewersList.replaceChildren(
    ...state.settings.external_viewers.map(renderExternalViewerRow),
  );
  if (state.settings.external_viewers.length === 0) {
    const empty = document.createElement("div");
    empty.className = "external-viewer-empty";
    empty.textContent = "No external viewers";
    externalViewersList.append(empty);
  }
}

function renderExternalViewerRow(viewer) {
  const row = document.createElement("div");
  row.className = "external-viewer-row";
  row.title = viewer.path;

  const label = document.createElement("div");
  label.innerHTML = `
    <strong>${escapeHtml(viewer.name)}</strong>
    <span>${escapeHtml(viewer.path)}</span>
  `;

  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.textContent = "Remove";
  removeButton.addEventListener("click", () => {
    state.settings.external_viewers = state.settings.external_viewers.filter(
      (item) => item.id !== viewer.id,
    );
    saveSettingsPreferences().catch(showError);
    renderSettingsDialog();
  });

  row.append(label, removeButton);
  return row;
}

function handleSlideshowSpeedInput() {
  state.settings.slideshow_speed_seconds = clampSlideshowSpeed(
    Number(slideshowSpeedInput.value),
  );
  slideshowSpeedValue.value = `${state.settings.slideshow_speed_seconds.toFixed(1)} s`;
}

function handleSettingsInput() {
  state.settings.upscale_fullscreen_images = upscaleFullscreenInput.checked;
  state.settings.slideshow_speed_seconds = clampSlideshowSpeed(
    Number(slideshowSpeedInput.value),
  );
  applyViewerUpscaleSetting();
  saveSettingsPreferences().catch(showError);
  if (state.slideshowActive) {
    scheduleSlideshow();
  }
}

async function addExternalViewer() {
  if (!invoke) {
    return;
  }

  const viewer = await invoke("pick_external_viewer");
  if (!viewer) {
    return;
  }

  state.settings.external_viewers = [
    ...state.settings.external_viewers.filter((item) => item.id !== viewer.id),
    viewer,
  ];
  await saveSettingsPreferences();
  renderSettingsDialog();
}

async function saveSettingsPreferences() {
  if (!invoke) {
    return;
  }

  const saved = await invoke("save_app_preferences", {
    preferences: {
      upscale_fullscreen_images: state.settings.upscale_fullscreen_images,
      slideshow_speed_seconds: state.settings.slideshow_speed_seconds,
      external_viewers: state.settings.external_viewers,
    },
  });
  state.settings = normalizeAppSettings(saved);
  applyViewerUpscaleSetting();
  renderSettingsDialog();
}

function applyViewerUpscaleSetting() {
  viewer.dataset.upscale = String(state.settings.upscale_fullscreen_images);
}

function clampSlideshowSpeed(value) {
  if (!Number.isFinite(value)) {
    return 3;
  }
  return Math.min(10, Math.max(0.1, value));
}

function clampThumbScale(value) {
  if (!Number.isFinite(value)) {
    return 1;
  }
  return Math.min(2, Math.max(0.5, value));
}

function viewKey(rootId, relativePath) {
  if (rootId === null) {
    return "root-overview";
  }
  return `${rootId}:${relativePath ?? ""}`;
}

function rememberCurrentScrollPosition() {
  if (state.atRootOverview) {
    state.viewScrollPositions.set(viewKey(null, ""), {
      left: gridNode.scrollLeft,
      top: gridNode.scrollTop,
    });
    return;
  }

  if (!state.currentRootId || !state.currentView) {
    return;
  }

  state.viewScrollPositions.set(viewKey(state.currentRootId, state.currentPath), {
    left: gridNode.scrollLeft,
    top: gridNode.scrollTop,
  });
}

function prepareScrollRestore(rootId, relativePath, options = {}) {
  const key = viewKey(rootId, relativePath);
  const saved = options.resetScroll ? null : state.viewScrollPositions.get(key);
  state.pendingScrollRestore = {
    key,
    left: saved?.left ?? 0,
    top: saved?.top ?? 0,
  };
}

function restorePendingScrollPosition() {
  if (!state.pendingScrollRestore) {
    return;
  }

  const key = state.atRootOverview
    ? viewKey(null, "")
    : viewKey(state.currentRootId, state.currentPath);
  if (key !== state.pendingScrollRestore.key) {
    return;
  }

  gridNode.scrollLeft = state.pendingScrollRestore.left;
  gridNode.scrollTop = state.pendingScrollRestore.top;
}

function nextFrame() {
  return new Promise((resolve) => requestAnimationFrame(resolve));
}

function renderBreadcrumbs(view) {
  const root = currentRoot();
  const crumbs = [
    { label: "Picturious", path: null, rootOverview: true },
    { label: root.display_name, path: "" },
  ];
  const parts = view.relative_path.split("/").filter(Boolean);
  let path = "";
  for (const part of parts) {
    path = path ? `${path}/${part}` : part;
    crumbs.push({ label: part, path });
  }

  breadcrumbsNode.replaceChildren(
    ...crumbs.map((crumb, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = crumb.label;
      button.disabled = index === crumbs.length - 1;
      button.addEventListener("click", () => {
        if (crumb.rootOverview) {
          openRootOverview();
        } else {
          openFolder(view.root_id, crumb.path).catch(showError);
        }
      });
      return button;
    }),
  );
}

function renderFolderCard(folder) {
  const card = document.createElement("article");
  card.className = "tile folder-tile";
  card.tabIndex = 0;
  card.title = fullFolderPath(folder);
  card.innerHTML = `
    <div class="thumb">
      <span>${escapeHtml(initials(folder.name))}</span>
    </div>
    <div class="tile-body">
      <h3>${escapeHtml(folder.name)}</h3>
      <p>${folder.image_count} images &middot; ${folder.child_folder_count} folders</p>
      ${renderTags(folder)}
    </div>
  `;

  const thumb = card.querySelector(".thumb");
  sizeTile(card);
  thumb.title = fullFolderPath(folder);
  if (folder.thumbnail_image_id) {
    requestThumbnailWhenVisible(
      folder.root_id,
      folder.thumbnail_image_id,
      thumb,
      THUMBNAIL_PIXEL_SIZE,
    );
  }

  card.addEventListener("click", () => {
    openFolder(folder.root_id, folder.relative_path).catch(showError);
  });
  card.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      openFolder(folder.root_id, folder.relative_path).catch(showError);
    }
  });
  return card;
}

function renderImageCard(image, index) {
  const card = document.createElement("article");
  card.className = "tile image-tile";
  card.tabIndex = 0;
  card.title = fullImagePath(image);
  card.innerHTML = `
    <div class="thumb image-thumb" data-image-id="${image.id}">
      <span>${escapeHtml(initials(image.file_name))}</span>
    </div>
    <div class="tile-body image-body">
      <h3>${escapeHtml(image.file_name)}</h3>
    </div>
  `;

  const thumb = card.querySelector(".thumb");
  sizeTile(card);
  thumb.title = fullImagePath(image);
  requestThumbnailWhenVisible(image.root_id, image.id, thumb, THUMBNAIL_PIXEL_SIZE);

  card.addEventListener("click", () => openViewer(index));
  card.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      openViewer(index);
    }
  });

  return card;
}

function renderTags(folder) {
  const tags = [
    ...folder.inherited_keywords,
    ...folder.direct_keywords,
    ...folder.inherited_people,
    ...folder.direct_people,
  ];
  if (tags.length === 0) {
    return "";
  }

  return `<div class="tags">${tags
    .map((tag) => `<span>${escapeHtml(tag)}</span>`)
    .join("")}</div>`;
}

async function setCurrentFolderCover(image) {
  if (!state.currentView) {
    return;
  }

  await invoke("set_folder_thumbnail", {
    rootId: image.root_id,
    folderId: state.currentView.folder_id,
    imageId: image.id,
  });
  setStatus(`Cover set to ${image.file_name}`);
}

function handleDocumentContextMenu(event) {
  event.preventDefault();
  const rootTile = event.target.closest(".root-tile");
  if (rootTile) {
    const root = rootById(rootTile.dataset.rootId);
    if (!root) {
      hideThumbContextMenu();
      return;
    }

    state.contextMenuRoot = root;
    state.contextMenuImage = null;
    showContextMenu(
      [{ action: "remove-root", label: "Remove root" }],
      event.clientX,
      event.clientY,
    );
    return;
  }

  const thumb = event.target.closest(".image-thumb");
  if (!thumb) {
    hideThumbContextMenu();
    return;
  }

  const image = imageById(Number(thumb.dataset.imageId));
  if (!image) {
    hideThumbContextMenu();
    return;
  }

  state.contextMenuImage = image;
  state.contextMenuRoot = null;
  showContextMenu(imageContextMenuItems(), event.clientX, event.clientY);
}

function handleDocumentClick(event) {
  if (!thumbContextMenu.contains(event.target)) {
    hideThumbContextMenu();
  }
}

async function handleThumbContextAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  const image = state.contextMenuImage;
  const root = state.contextMenuRoot;
  const viewerId = button.dataset.viewerId;
  hideThumbContextMenu();

  try {
    if (action === "remove-root" && root) {
      await removeRoot(root.id);
    } else if (action === "set-cover" && image) {
      await setCurrentFolderCover(image);
    } else if (action === "rotate-right" && image) {
      await rotateImage(image, "right");
    } else if (action === "rotate-left" && image) {
      await rotateImage(image, "left");
    } else if (action === "show-explorer" && image) {
      await invoke("show_image_in_explorer", {
        rootId: image.root_id,
        imageId: image.id,
      });
    } else if (action === "recycle" && image) {
      await moveImageToRecycleBin(image);
    } else if (action === "open-with" && image && viewerId) {
      await invoke("open_image_with", {
        rootId: image.root_id,
        imageId: image.id,
        viewerId,
      });
    }
  } catch (error) {
    showError(error);
  }
}

function imageContextMenuItems() {
  const items = [
    { action: "set-cover", label: "Set as cover" },
    { action: "rotate-right", label: "Rotate right" },
    { action: "rotate-left", label: "Rotate left" },
    { action: "show-explorer", label: "Show in Explorer" },
    { action: "recycle", label: "Move to recycle bin" },
  ];

  for (const viewer of state.settings.external_viewers) {
    items.push({
      action: "open-with",
      label: `Open with ${viewer.name}`,
      viewerId: viewer.id,
    });
  }

  return items;
}

function showContextMenu(items, x, y) {
  thumbContextMenu.replaceChildren(
    ...items.map((item) => {
      const button = document.createElement("button");
      button.type = "button";
      button.dataset.action = item.action;
      if (item.viewerId) {
        button.dataset.viewerId = item.viewerId;
      }
      button.textContent = item.label;
      return button;
    }),
  );
  thumbContextMenu.classList.remove("hidden");
  const { width, height } = thumbContextMenu.getBoundingClientRect();
  const left = Math.min(x, window.innerWidth - width - 8);
  const top = Math.min(y, window.innerHeight - height - 8);
  thumbContextMenu.style.left = `${Math.max(8, left)}px`;
  thumbContextMenu.style.top = `${Math.max(8, top)}px`;
}

function hideThumbContextMenu() {
  thumbContextMenu.classList.add("hidden");
  state.contextMenuImage = null;
  state.contextMenuRoot = null;
}

async function rotateImage(image, direction) {
  setStatus(`Rotating ${image.file_name}...`);
  await invoke("rotate_image", {
    rootId: image.root_id,
    imageId: image.id,
    direction,
  });
  state.imageUrlCache.clear();
  await refreshCurrentFolder({ keepStatus: true });
  setStatus(`Rotated ${image.file_name}`);
}

async function moveImageToRecycleBin(image) {
  setStatus(`Moving ${image.file_name} to recycle bin...`);
  await invoke("move_image_to_recycle_bin", {
    rootId: image.root_id,
    imageId: image.id,
  });
  state.imageUrlCache.clear();
  await refreshCurrentFolder({ keepStatus: true });
  setStatus(`Moved ${image.file_name} to recycle bin`);
}

function imageById(imageId) {
  return state.currentView?.images.find((image) => image.id === imageId) ?? null;
}

function rootById(rootId) {
  return state.roots.find((root) => root.id === rootId) ?? null;
}

function fullFolderPath(folder) {
  const root = state.roots.find((item) => item.id === folder.root_id);
  if (!root?.path) {
    return folder.relative_path;
  }

  const separator = root.path.includes("/") && !root.path.includes("\\") ? "/" : "\\";
  const rootPath = root.path.replace(/[\\/]+$/, "");
  const relativePath = folder.relative_path.replaceAll("/", separator);
  return relativePath ? `${rootPath}${separator}${relativePath}` : rootPath;
}

function fullImagePath(image) {
  const root = state.roots.find((item) => item.id === image.root_id);
  if (!root?.path) {
    return image.relative_path;
  }

  const separator = root.path.includes("/") && !root.path.includes("\\") ? "/" : "\\";
  const rootPath = root.path.replace(/[\\/]+$/, "");
  return `${rootPath}${separator}${image.relative_path.replaceAll("/", separator)}`;
}

function resetThumbnailWork() {
  state.viewGeneration += 1;
  thumbnailQueue.items = [];
  if (thumbnailObserver) {
    for (const target of observedThumbs) {
      thumbnailObserver.unobserve(target);
    }
  }
  observedThumbs.clear();
}

function requestThumbnailWhenVisible(rootId, imageId, target, size) {
  if (state.activeScans.has(rootId)) {
    return;
  }

  const generation = state.viewGeneration;
  target.dataset.rootId = rootId;
  target.dataset.imageId = String(imageId);
  target.dataset.thumbSize = String(size);
  target.dataset.generation = String(generation);

  if (!thumbnailObserver) {
    queueThumbnail(rootId, imageId, target, size);
    return;
  }

  observedThumbs.add(target);
  thumbnailObserver.observe(target);
}

function handleThumbnailIntersection(entries) {
  for (const entry of entries) {
    if (!entry.isIntersecting) {
      continue;
    }

    const target = entry.target;
    thumbnailObserver.unobserve(target);
    observedThumbs.delete(target);

    const generation = Number(target.dataset.generation);
    if (generation !== state.viewGeneration) {
      continue;
    }

    queueThumbnail(
      target.dataset.rootId,
      Number(target.dataset.imageId),
      target,
      Number(target.dataset.thumbSize),
    );
  }
}

function queueThumbnail(rootId, imageId, target, size) {
  if (state.activeScans.has(rootId)) {
    return;
  }

  const generation = state.viewGeneration;
  target.dataset.imageId = String(imageId);
  target.dataset.generation = String(generation);
  thumbnailQueue.items.push({ rootId, imageId, target, size, generation });
  pumpThumbnailQueue();
}

function pumpThumbnailQueue() {
  while (
    thumbnailQueue.active < thumbnailQueue.maxActive &&
    thumbnailQueue.items.length > 0
  ) {
    const job = thumbnailQueue.items.shift();
    if (state.activeScans.has(job.rootId)) {
      continue;
    }
    if (
      job.generation !== state.viewGeneration ||
      job.target.dataset.imageId !== String(job.imageId)
    ) {
      continue;
    }

    thumbnailQueue.active += 1;
    loadThumbnail(job)
      .catch((error) => {
        const message = String(error);
        if (
          job.generation === state.viewGeneration &&
          !state.activeScans.has(job.rootId) &&
          !message.includes("paused while scanning")
        ) {
          job.target.classList.add("failed");
        }
      })
      .finally(() => {
        thumbnailQueue.active -= 1;
        pumpThumbnailQueue();
      });
  }
}

async function loadThumbnail({ rootId, imageId, target, size, generation }) {
  if (state.activeScans.has(rootId)) {
    return;
  }

  target.dataset.imageId = String(imageId);
  const thumbnail = await invoke("thumbnail", { rootId, imageId, size });
  if (
    generation !== state.viewGeneration ||
    target.dataset.imageId !== String(imageId)
  ) {
    return;
  }
  target.replaceChildren();
  target.style.backgroundImage = `url("${thumbnail.data_url}")`;
  target.style.backgroundSize = "contain";
  target.style.backgroundPosition = "center";
  target.style.backgroundRepeat = "no-repeat";
  target.classList.add("loaded");
}

function openViewer(index) {
  state.viewerIndex = index;
  viewer.classList.remove("hidden");
  viewer.focus({ preventScroll: true });
  applyViewerUpscaleSetting();
  enterViewerFullscreen().catch(showError);
  renderViewerImage().catch(showError);
}

async function renderViewerImage() {
  const image = state.currentView?.images[state.viewerIndex];
  if (!image) {
    closeViewer();
    return;
  }

  const generation = ++state.viewerGeneration;
  viewerImage.alt = image.file_name;
  const source = await imageSourceFor(image);
  if (generation !== state.viewerGeneration) {
    return;
  }

  viewerImage.src = source;
  preloadNeighborImages(generation);
}

async function imageSourceFor(image) {
  const cacheKey = `${image.root_id}:${image.id}:${image.modified_unix_ms}`;
  const cached = state.imageUrlCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  let source;
  if (convertFileSrc) {
    const path = await invoke("image_file_path", {
      rootId: image.root_id,
      imageId: image.id,
    });
    source = withCacheBuster(convertFileSrc(path), image.modified_unix_ms);
  } else {
    const preview = await invoke("thumbnail", {
      rootId: image.root_id,
      imageId: image.id,
      size: 1800,
    });
    source = preview.data_url;
  }

  state.imageUrlCache.set(cacheKey, source);
  return source;
}

function preloadNeighborImages(generation) {
  const images = state.currentView?.images ?? [];
  if (images.length < 2 || !convertFileSrc) {
    return;
  }

  for (const offset of [-1, 1]) {
    const index = (state.viewerIndex + offset + images.length) % images.length;
    const image = images[index];
    imageSourceFor(image)
      .then((source) => {
        if (generation !== state.viewerGeneration) {
          return;
        }
        const preload = new Image();
        preload.src = source;
      })
      .catch(() => {});
  }
}

function toggleSlideshow() {
  if (state.slideshowActive) {
    stopSlideshow();
  } else {
    startSlideshow();
  }
}

function startSlideshow() {
  const images = state.currentView?.images ?? [];
  if (images.length < 2) {
    return;
  }

  state.slideshowActive = true;
  scheduleSlideshow();
}

function stopSlideshow() {
  state.slideshowActive = false;
  if (state.slideshowTimer) {
    window.clearTimeout(state.slideshowTimer);
    state.slideshowTimer = null;
  }
}

function scheduleSlideshow() {
  if (state.slideshowTimer) {
    window.clearTimeout(state.slideshowTimer);
  }
  if (!state.slideshowActive || viewer.classList.contains("hidden")) {
    state.slideshowTimer = null;
    return;
  }

  state.slideshowTimer = window.setTimeout(() => {
    state.slideshowTimer = null;
    if (!state.slideshowActive || viewer.classList.contains("hidden")) {
      return;
    }
    moveViewer(1, { keepSlideshow: true });
    scheduleSlideshow();
  }, state.settings.slideshow_speed_seconds * 1000);
}

function withCacheBuster(source, modifiedUnixMs) {
  const separator = source.includes("?") ? "&" : "?";
  return `${source}${separator}v=${encodeURIComponent(modifiedUnixMs)}`;
}

function handleViewerWheel(event) {
  if (viewer.classList.contains("hidden")) {
    return;
  }

  event.preventDefault();
  const delta =
    Math.abs(event.deltaX) > Math.abs(event.deltaY)
      ? event.deltaX
      : event.deltaY;
  if (Math.abs(delta) < 12) {
    return;
  }

  const now = Date.now();
  if (now - state.lastWheelAt < 180) {
    return;
  }

  state.lastWheelAt = now;
  moveViewer(delta > 0 ? 1 : -1);
}

async function enterViewerFullscreen() {
  const browserFullscreen = requestBrowserFullscreen();
  const windowFullscreen = invoke
    ? invoke("set_viewer_fullscreen", { fullscreen: true })
    : Promise.resolve();

  const [windowResult] = await Promise.allSettled([
    windowFullscreen,
    browserFullscreen,
  ]);
  if (windowResult.status === "rejected") {
    throw windowResult.reason;
  }
}

async function exitViewerFullscreen() {
  if (document.fullscreenElement) {
    await document.exitFullscreen().catch(() => {});
  }
  if (invoke) {
    await invoke("set_viewer_fullscreen", { fullscreen: false });
  }
}

async function requestBrowserFullscreen() {
  if (document.fullscreenElement || !viewer.requestFullscreen) {
    return;
  }

  await viewer.requestFullscreen().catch(() => {});
}

function handleBrowserFullscreenChange() {
  if (!document.fullscreenElement && !viewer.classList.contains("hidden")) {
    closeViewer();
  }
}

function moveViewer(delta, options = {}) {
  const images = state.currentView?.images ?? [];
  if (images.length === 0) {
    return;
  }
  if (!options.keepSlideshow) {
    stopSlideshow();
  }
  state.viewerIndex = (state.viewerIndex + delta + images.length) % images.length;
  renderViewerImage().catch(showError);
}

function closeViewer() {
  stopSlideshow();
  state.viewerGeneration += 1;
  viewer.classList.add("hidden");
  viewerImage.removeAttribute("src");
  exitViewerFullscreen().catch(showError);
}

function renderEmptyState(message, options = {}) {
  if (!options.keepBreadcrumbs) {
    breadcrumbsNode.replaceChildren();
  }
  gridNode.replaceChildren(emptyNode(message));
}

function emptyNode(message) {
  const node = document.createElement("div");
  node.className = "empty-state";
  node.textContent = message;
  return node;
}

function renderStaticPreview() {
  state.roots = [
    {
      id: "preview",
      display_name: "Preview Root",
      path: "D:\\Pictures",
      connected: false,
      folder_count: 0,
      image_count: 0,
      thumbnail_image_id: null,
    },
  ];
  openRootOverview();
}

function currentRoot() {
  return state.roots.find((root) => root.id === state.currentRootId) ?? state.roots[0];
}

function parentPathFor(relativePath) {
  const normalized = String(relativePath || "").replaceAll("\\", "/").replace(/^\/+|\/+$/g, "");
  if (!normalized) {
    return null;
  }
  const parts = normalized.split("/");
  parts.pop();
  return parts.join("/");
}

function setStatus(message) {
  statusNode.textContent = message;
}

function showError(error) {
  const message = String(error);
  setStatus(message);
  console.error(error);
}

function initials(value) {
  return String(value || "P").slice(0, 2).toUpperCase();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
