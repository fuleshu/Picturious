const tauri = window.__TAURI__;
const invoke = tauri?.core?.invoke;
const listen = tauri?.event?.listen;
const convertFileSrc = tauri?.core?.convertFileSrc;
const BASE_TILE_SIZE = 188;
const THUMBNAIL_PIXEL_SIZE = 450;
const STREAM_ITEMS_PER_FRAME = 16;
const VIEWER_CURSOR_HIDE_DELAY_MS = 3000;
const MAX_FOLDER_VIEW_CACHE_ENTRIES = 80;
const MAX_THUMBNAIL_DATA_CACHE_ENTRIES = 700;
const RATING_OPTIONS = [
  { value: "unhappy", label: ":(" },
  { value: "neutral", label: ":|" },
  { value: "happy", label: ":)" },
];

const gridNode = document.querySelector("#content-grid");
const statusNode = document.querySelector("#status");
const busyIndicator = document.querySelector("#busy-indicator");
const busyText = document.querySelector("#busy-text");
const titleNode = document.querySelector("#view-title");
const metadataBar = document.querySelector("#metadata-bar");
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
const slideshowLoopInput = document.querySelector("#slideshow-loop");
const slideshowSpeedInput = document.querySelector("#slideshow-speed");
const slideshowSpeedNumberInput = document.querySelector("#slideshow-speed-number");
const slideshowIgnoreSmallerInput = document.querySelector("#slideshow-ignore-smaller");
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
  scanProgressText: new Map(),
  viewScrollPositions: new Map(),
  pendingScrollRestore: null,
  viewGeneration: 0,
  folderRequestId: 0,
  activeFolderRequestId: null,
  folderLoading: false,
  folderViewCache: new Map(),
  streamRenderQueue: [],
  streamRenderScheduled: false,
  streamFinishedPayload: null,
  validationPatchTimer: null,
  visibleValidationTimer: null,
  visibleValidationActive: false,
  validatedVisibleKeys: new Set(),
  viewerGeneration: 0,
  imageUrlCache: new Map(),
  thumbnailDataCache: new Map(),
  lastWheelAt: 0,
  contextMenuImage: null,
  contextMenuFolder: null,
  contextMenuRoot: null,
  metadataMode: "edit",
  currentFolderMeta: null,
  metadataLoading: false,
  metadataRequestId: 0,
  peopleOptions: [],
  peopleOptionsLoaded: false,
  personDropdownOpen: false,
  personSearch: "",
  slideshowTimer: null,
  slideshowActive: false,
  slideshowEnded: false,
  slideshowPlaylist: null,
  slideshowSkipAttempts: 0,
  viewerCursorTimer: null,
  imageDimensionCache: new Map(),
  settings: {
    upscale_fullscreen_images: false,
    slideshow_speed_seconds: 3,
    slideshow_loop: false,
    slideshow_ignore_smaller_than: 0,
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
metadataBar.addEventListener("click", (event) => {
  handleMetadataBarClick(event).catch(showError);
});
metadataBar.addEventListener("input", handleMetadataBarInput);
metadataBar.addEventListener("keydown", (event) => {
  handleMetadataBarKeydown(event).catch(showError);
});
upscaleFullscreenInput.addEventListener("change", handleSettingsInput);
slideshowLoopInput.addEventListener("change", handleSettingsInput);
slideshowSpeedInput.addEventListener("input", handleSlideshowSpeedInput);
slideshowSpeedInput.addEventListener("change", handleSettingsInput);
slideshowSpeedNumberInput.addEventListener("input", handleSlideshowSpeedNumberInput);
slideshowSpeedNumberInput.addEventListener("change", handleSettingsInput);
slideshowIgnoreSmallerInput.addEventListener("change", handleSettingsInput);
addExternalViewerButton.addEventListener("click", addExternalViewer);
viewerCloseHotspot.addEventListener("click", closeViewer);
viewer.addEventListener("wheel", handleViewerWheel, { passive: false });
viewer.addEventListener("mousemove", handleViewerMouseMove);
document.addEventListener("fullscreenchange", handleBrowserFullscreenChange);
document.addEventListener("contextmenu", handleDocumentContextMenu);
document.addEventListener("click", handleDocumentClick);
window.addEventListener("blur", hideThumbContextMenu);
window.addEventListener("resize", hideThumbContextMenu);
gridNode.addEventListener("scroll", () => scheduleVisibleFolderValidation(250), {
  passive: true,
});
thumbContextMenu.addEventListener("click", handleThumbContextAction);

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !thumbContextMenu.classList.contains("hidden")) {
    hideThumbContextMenu();
    return;
  }

  if (event.key === "Escape" && state.personDropdownOpen) {
    closePersonDropdown();
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
  } else if (event.key === "Home") {
    event.preventDefault();
    jumpToFirstViewerImage();
  } else if (event.key === " " || event.key === "Spacebar") {
    event.preventDefault();
    if (event.repeat) {
      return;
    }
    toggleSlideshow();
  } else if (event.key.toLowerCase() === "r" && state.slideshowActive) {
    event.preventDefault();
    randomizeCurrentSlideshow();
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
    updateBusyIndicator();
    showError(payload.message);
  });

  await listen("folder-validated", ({ payload }) => {
    if (payload.changed) {
      invalidateFolderCachesForChanges(payload.root_id, [payload.relative_path]);
    }

    if (
      payload.root_id === state.currentRootId &&
      payload.changed &&
      folderValidationAffectsCurrentView(payload.relative_path)
    ) {
      scheduleCurrentFolderPatch();
    }
  });

  await listen("folder-validation-finished", ({ payload }) => {
    if (payload.request_id !== state.activeFolderRequestId) {
      return;
    }

    state.visibleValidationActive = false;
    updateBusyIndicator();

    const changedPaths = payload.changed_paths ?? [];
    invalidateFolderCachesForChanges(payload.root_id, changedPaths);
    if (changedPaths.some(folderValidationIsCurrentFolder)) {
      state.validatedVisibleKeys.clear();
    }
    if (changedPaths.some(folderValidationAffectsCurrentView)) {
      scheduleCurrentFolderPatch();
    }
  });

  await listen("folder-validation-error", ({ payload }) => {
    if (payload.request_id !== state.activeFolderRequestId) {
      return;
    }

    state.visibleValidationActive = false;
    state.validatedVisibleKeys.clear();
    updateBusyIndicator();
    console.warn(payload.message);
  });

  await listen("scan-progress", ({ payload }) => {
    const wasActive = state.activeScans.has(payload.root_id);
    state.activeScans.add(payload.root_id);
    state.scanProgressText.set(
      payload.root_id,
      `Scanning ${payload.folders_seen} folders`,
    );
    updateBusyIndicator();
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
    state.scanProgressText.delete(payload.root_id);
    clearValidationPatchTimer();
    updateBusyIndicator();
    invalidateFolderViewCache(payload.root_id);
    invalidateThumbnailDataCache(payload.root_id);
    await refreshOverview();
    if (payload.root_id === state.currentRootId) {
      scanButton.disabled = false;
      resumeDeferredThumbnails();
      scheduleVisibleFolderValidation(100);
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
    state.scanProgressText.delete(payload.root_id);
    clearValidationPatchTimer();
    updateBusyIndicator();
    if (payload.root_id === state.currentRootId) {
      scanButton.disabled = false;
      resumeDeferredThumbnails();
    }
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
  state.peopleOptionsLoaded = false;
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

  await startScan(state.currentRootId, state.currentPath);
}

async function startScan(rootId, relativePath = "") {
  const root = state.roots.find((item) => item.id === rootId);
  if (!root?.connected) {
    return;
  }

  const scanTarget = relativePath ? `${root.display_name}/${relativePath}` : root.display_name;
  state.activeScans.add(rootId);
  state.scanProgressText.set(rootId, `Scanning ${scanTarget}`);
  updateBusyIndicator();
  pauseThumbnailWorkForRoot(rootId);
  renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
  scanButton.disabled = true;
  setStatus(`Scanning ${scanTarget}...`);
  let started;
  try {
    started = await invoke("start_scan", { rootId, relativePath });
  } catch (error) {
    state.activeScans.delete(rootId);
    state.scanProgressText.delete(rootId);
    updateBusyIndicator();
    renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
    throw error;
  }
  if (!started) {
    setStatus(`${root.display_name} is already scanning`);
    state.scanProgressText.delete(rootId);
    updateBusyIndicator();
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
  invalidateFolderViewCache(rootId);
  invalidateThumbnailDataCache(rootId);

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
    clearMetadataSelection();
    renderEmptyState("Root is not connected");
    return;
  }

  rememberCurrentScrollPosition();

  state.currentRootId = rootId;
  state.currentPath = relativePath ?? "";
  state.atRootOverview = false;
  const requestId = ++state.folderRequestId;
  state.activeFolderRequestId = requestId;
  state.folderLoading = false;
  state.visibleValidationActive = false;
  state.validatedVisibleKeys.clear();
  clearValidationPatchTimer();
  clearVisibleValidationTimer();
  updateBusyIndicator();
  resetStreamRenderQueue();
  prepareScrollRestore(rootId, state.currentPath, options);
  resetThumbnailWork();
  clearMetadataSelection();

  const cachedView = options.forceReload
    ? null
    : cachedFolderView(rootId, state.currentPath);
  if (cachedView) {
    state.currentView = cachedView;
    renderFolderView(cachedView, options);
    restorePendingScrollPosition();
    updateBusyIndicator();
    scheduleVisibleFolderValidation(100);
    return;
  }

  state.folderLoading = true;
  updateBusyIndicator();
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
      state.visibleValidationActive = false;
      updateBusyIndicator();
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

function folderValidationIsCurrentFolder(relativePath) {
  if (state.atRootOverview || !state.currentView) {
    return false;
  }

  const normalizedPath = normalizeRelativePath(relativePath);
  return normalizedPath === state.currentPath;
}

function folderValidationAffectsCurrentView(relativePath) {
  if (folderValidationIsCurrentFolder(relativePath)) {
    return true;
  }

  if (state.atRootOverview || !state.currentView) {
    return false;
  }

  const normalizedPath = normalizeRelativePath(relativePath);
  if (parentPathFor(normalizedPath) === state.currentPath) {
    return true;
  }

  return state.currentView.folders.some(
    (folder) => pathContainsPath(folder.relative_path, normalizedPath),
  );
}

function scheduleCurrentFolderPatch() {
  if (state.folderLoading || state.validationPatchTimer) {
    return;
  }

  state.validationPatchTimer = window.setTimeout(() => {
    state.validationPatchTimer = null;
    patchCurrentFolderFromDb({ keepStatus: true }).catch(console.warn);
  }, 120);
}

function clearValidationPatchTimer() {
  if (!state.validationPatchTimer) {
    return;
  }

  window.clearTimeout(state.validationPatchTimer);
  state.validationPatchTimer = null;
}

function clearVisibleValidationTimer() {
  if (!state.visibleValidationTimer) {
    return;
  }

  window.clearTimeout(state.visibleValidationTimer);
  state.visibleValidationTimer = null;
}

async function patchCurrentFolderFromDb(options = {}) {
  if (!invoke || state.atRootOverview || !state.currentRootId || !state.currentView) {
    return;
  }

  const rootId = state.currentRootId;
  const relativePath = state.currentPath;
  const requestId = state.activeFolderRequestId;
  const scrollLeft = gridNode.scrollLeft;
  const scrollTop = gridNode.scrollTop;
  const view = await invoke("folder_view", { rootId, relativePath });
  if (
    requestId !== state.activeFolderRequestId ||
    state.atRootOverview ||
    state.currentRootId !== rootId ||
    state.currentPath !== relativePath
  ) {
    return;
  }

  patchFolderViewInPlace(view, options);
  gridNode.scrollLeft = scrollLeft;
  gridNode.scrollTop = scrollTop;
  cacheFolderView(state.currentView);
  resumeDeferredThumbnails();
  scheduleVisibleFolderValidation(100);
}

function scheduleVisibleFolderValidation(delay = 200) {
  if (
    !invoke ||
    state.atRootOverview ||
    !state.currentRootId ||
    state.folderLoading ||
    state.visibleValidationActive
  ) {
    return;
  }

  if (state.visibleValidationTimer) {
    window.clearTimeout(state.visibleValidationTimer);
  }

  state.visibleValidationTimer = window.setTimeout(() => {
    state.visibleValidationTimer = null;
    startVisibleFolderValidation().catch(console.warn);
  }, delay);
}

async function startVisibleFolderValidation() {
  if (
    state.atRootOverview ||
    !state.currentRootId ||
    !state.currentView ||
    state.folderLoading ||
    state.visibleValidationActive
  ) {
    return;
  }

  const paths = visibleFolderValidationPaths();
  if (paths.length === 0) {
    return;
  }

  const rootId = state.currentRootId;
  const requestId = state.activeFolderRequestId;
  const currentPath = state.currentPath;
  state.visibleValidationActive = true;
  updateBusyIndicator();

  try {
    await invoke("validate_folder_view", {
      rootId,
      relativePath: currentPath,
      visibleRelativePaths: paths.filter((path) => path !== currentPath),
      requestId,
    });
  } catch (error) {
    if (requestId === state.activeFolderRequestId) {
      state.visibleValidationActive = false;
      state.validatedVisibleKeys.clear();
      updateBusyIndicator();
    }
    console.warn(error);
  }
}

function visibleFolderValidationPaths() {
  const paths = [];
  const addPath = (path) => {
    const normalized = String(path || "").replaceAll("\\", "/").replace(/^\/+|\/+$/g, "");
    const key = viewKey(state.currentRootId, normalized);
    if (state.validatedVisibleKeys.has(key)) {
      return;
    }
    state.validatedVisibleKeys.add(key);
    paths.push(normalized);
  };

  addPath(state.currentPath);

  const gridRect = gridNode.getBoundingClientRect();
  const margin = 160;
  for (const tile of gridNode.querySelectorAll(".folder-tile")) {
    const rect = tile.getBoundingClientRect();
    const intersects =
      rect.bottom >= gridRect.top - margin &&
      rect.top <= gridRect.bottom + margin &&
      rect.right >= gridRect.left - margin &&
      rect.left <= gridRect.right + margin;
    if (intersects) {
      addPath(tile.dataset.folderPath);
    }
  }

  return paths;
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
  state.visibleValidationActive = false;
  state.validatedVisibleKeys.clear();
  clearValidationPatchTimer();
  clearVisibleValidationTimer();
  updateBusyIndicator();
  resetStreamRenderQueue();
  resetThumbnailWork();
  clearMetadataSelection();
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
  renderMetadataBar();
  breadcrumbsNode.replaceChildren();

  if (state.roots.length === 0) {
    renderEmptyState("No roots", { keepBreadcrumbs: true });
    return;
  }

  const nodes = sortedRoots().map(renderRootCard);
  gridNode.replaceChildren(...nodes);
  if (options.resetScroll) {
    gridNode.scrollTop = 0;
    gridNode.scrollLeft = 0;
  } else {
    restorePendingScrollPosition();
  }
}

function sortedRoots() {
  return [...state.roots].sort((left, right) =>
    left.display_name.localeCompare(right.display_name, undefined, {
      sensitivity: "base",
    }),
  );
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

function clearMetadataSelection() {
  state.currentFolderMeta = null;
  state.metadataLoading = false;
  state.metadataRequestId += 1;
  state.personDropdownOpen = false;
  state.personSearch = "";
  renderMetadataBar();
}

function currentFolderMetadataTarget() {
  if (state.atRootOverview || !state.currentView?.folder_id) {
    return null;
  }

  return {
    rootId: state.currentView.root_id,
    folderId: Number(state.currentView.folder_id),
    relativePath: state.currentView.relative_path ?? "",
    displayName: state.currentView.relative_path || state.currentView.root_display_name,
  };
}

async function loadCurrentFolderMetadata() {
  const target = currentFolderMetadataTarget();
  if (!target) {
    clearMetadataSelection();
    return;
  }

  if (!invoke) {
    state.currentFolderMeta = normalizeFolderMetadata(null, target);
    state.metadataLoading = false;
    renderMetadataBar();
    return;
  }

  const requestId = ++state.metadataRequestId;
  state.metadataLoading = true;
  renderMetadataBar();
  try {
    const metadata = await invoke("folder_metadata", {
      rootId: target.rootId,
      folderId: target.folderId,
    });
    const currentTarget = currentFolderMetadataTarget();
    if (
      requestId !== state.metadataRequestId ||
      !currentTarget ||
      currentTarget.rootId !== target.rootId ||
      currentTarget.folderId !== target.folderId
    ) {
      return;
    }
    state.currentFolderMeta = normalizeFolderMetadata(metadata, target);
    mergePeopleOptions([
      ...state.currentFolderMeta.people,
      ...state.currentFolderMeta.inherited_people,
    ]);
  } finally {
    const currentTarget = currentFolderMetadataTarget();
    if (
      requestId === state.metadataRequestId &&
      currentTarget &&
      currentTarget.rootId === target.rootId &&
      currentTarget.folderId === target.folderId
    ) {
      state.metadataLoading = false;
      renderMetadataBar();
    }
  }
}

function normalizeFolderMetadata(metadata, target) {
  return {
    root_id: metadata?.root_id ?? target?.rootId ?? "",
    folder_id: Number(metadata?.folder_id ?? target?.folderId ?? 0),
    relative_path: metadata?.relative_path ?? target?.relativePath ?? "",
    rating: metadata?.rating ?? null,
    inherited_rating: metadata?.inherited_rating ?? null,
    people: Array.isArray(metadata?.people)
      ? metadata.people
          .filter((person) => Number.isFinite(Number(person?.id)) && person?.name)
          .map((person) => ({
            id: Number(person.id),
            name: String(person.name),
          }))
      : [],
    inherited_people: Array.isArray(metadata?.inherited_people)
      ? metadata.inherited_people
          .filter((person) => Number.isFinite(Number(person?.id)) && person?.name)
          .map((person) => ({
            id: Number(person.id),
            name: String(person.name),
          }))
      : [],
  };
}

function renderMetadataBar(options = {}) {
  const target = currentFolderMetadataTarget();
  const metadata = target ? state.currentFolderMeta : null;
  const editDisabled = !target || state.metadataMode !== "edit";
  const disabledAttr = editDisabled ? " disabled" : "";
  const targetLabel = target ? `Folder: ${target.displayName}` : "No folder";
  const targetTitle = target?.relativePath ?? "";
  const rating = metadata?.rating ?? null;
  const inheritedRating = metadata?.inherited_rating ?? null;
  const people = metadata?.people ?? [];
  const inheritedPeople = metadata?.inherited_people ?? [];
  const personDropdown = state.personDropdownOpen && !editDisabled
    ? renderPersonDropdownHtml()
    : "";

  metadataBar.innerHTML = `
    <div class="metadata-mode-tabs" role="tablist" aria-label="Metadata mode">
      <button type="button" data-metadata-mode="search" disabled>Search</button>
      <button type="button" data-metadata-mode="edit" data-active="${state.metadataMode === "edit"}">Edit</button>
    </div>
    <div class="metadata-target" title="${escapeHtml(targetTitle)}">${escapeHtml(targetLabel)}</div>
    <div class="rating-toggle-group" role="group" aria-label="Rating">
      ${RATING_OPTIONS.map((option) => {
        const active = rating === option.value;
        const inheritedActive = !rating && inheritedRating === option.value;
        const title = inheritedActive
          ? `${option.value} inherited from a parent folder`
          : option.value;
        return `<button class="rating-toggle" type="button" data-rating="${option.value}" data-active="${active}" data-inherited-active="${inheritedActive}" aria-pressed="${active || inheritedActive}" title="${title}"${disabledAttr}>${option.label}</button>`;
      }).join("")}
    </div>
    <div class="people-editor">
      <span class="metadata-label">Person:</span>
      <div class="person-chips">
        ${people.map(renderPersonChipHtml).join("")}
        ${inheritedPeople.map(renderInheritedPersonChipHtml).join("")}
      </div>
      <button class="person-add-button" type="button" data-action="toggle-person-dropdown" title="Add person" aria-label="Add person"${disabledAttr}>+</button>
      ${personDropdown}
    </div>
  `;

  updatePersonDropdownOptions();
  if (options.focusPersonInput && state.personDropdownOpen) {
    requestAnimationFrame(() => {
      const input = metadataBar.querySelector(".person-search-field");
      input?.focus({ preventScroll: true });
      input?.select();
    });
  }
}

function renderPersonChipHtml(person) {
  return `
    <span class="person-chip">
      <span title="${escapeHtml(person.name)}">${escapeHtml(person.name)}</span>
      <button type="button" data-action="remove-person" data-person-id="${person.id}" title="Remove person" aria-label="Remove person">x</button>
    </span>
  `;
}

function renderInheritedPersonChipHtml(person) {
  return `
    <span class="person-chip" data-inherited="true" title="Inherited from a parent folder">
      <span>${escapeHtml(person.name)}</span>
    </span>
  `;
}

function renderPersonDropdownHtml() {
  return `
    <div class="person-dropdown">
      <input class="person-search-field" type="text" value="${escapeHtml(state.personSearch)}" placeholder="Name" aria-label="Person name" />
      <div class="person-options" role="listbox"></div>
    </div>
  `;
}

function updatePersonDropdownOptions() {
  const optionsNode = metadataBar.querySelector(".person-options");
  if (!optionsNode) {
    return;
  }

  const metadata = state.currentFolderMeta;
  const assignedNames = new Set([
    ...(metadata?.people ?? []).map((person) => normalizedPersonName(person.name)),
    ...(metadata?.inherited_people ?? []).map((person) => normalizedPersonName(person.name)),
  ]);
  const query = state.personSearch.trim().toLowerCase();
  const options = state.peopleOptions
    .filter((person) => !assignedNames.has(normalizedPersonName(person.name)))
    .filter((person) => !query || person.name.toLowerCase().includes(query))
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
    );

  optionsNode.replaceChildren(
    ...options.map((person) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "person-option";
      button.dataset.personName = person.name;
      button.textContent = person.name;
      return button;
    }),
  );
}

async function openPersonDropdown() {
  const target = currentFolderMetadataTarget();
  if (!target) {
    return;
  }

  state.personDropdownOpen = true;
  state.personSearch = "";
  if (!state.peopleOptionsLoaded) {
    state.peopleOptions = [];
  }
  renderMetadataBar({ focusPersonInput: true });
  await loadPeopleOptions();
}

function closePersonDropdown() {
  if (!state.personDropdownOpen) {
    return;
  }
  state.personDropdownOpen = false;
  state.personSearch = "";
  renderMetadataBar();
}

async function loadPeopleOptions() {
  if (!invoke || state.peopleOptionsLoaded) {
    return;
  }

  const options = await invoke("metadata_people");
  state.peopleOptionsLoaded = true;
  state.peopleOptions = normalizePeopleOptions(options);
  renderMetadataBar({ focusPersonInput: true });
}

function normalizePeopleOptions(options) {
  return Array.isArray(options)
    ? options
        .filter((person) => Number.isFinite(Number(person?.id)) && person?.name)
        .map((person) => ({
          id: Number(person.id),
          name: String(person.name),
        }))
    : [];
}

function mergePeopleOptions(people) {
  if (!Array.isArray(people) || people.length === 0) {
    return;
  }

  const existing = new Set(
    state.peopleOptions.map((person) => normalizedPersonName(person.name)),
  );
  for (const person of people) {
    const key = normalizedPersonName(person.name);
    if (!existing.has(key)) {
      state.peopleOptions.push({
        id: Number(person.id),
        name: String(person.name),
      });
      existing.add(key);
    }
  }
}

function normalizedPersonName(name) {
  return String(name || "").trim().toLowerCase();
}

async function handleMetadataBarClick(event) {
  const modeButton = event.target.closest("button[data-metadata-mode]");
  if (modeButton && !modeButton.disabled) {
    state.metadataMode = modeButton.dataset.metadataMode;
    renderMetadataBar();
    return;
  }

  const ratingButton = event.target.closest("button[data-rating]");
  if (ratingButton && !ratingButton.disabled) {
    await setCurrentFolderRating(ratingButton.dataset.rating);
    return;
  }

  const actionButton = event.target.closest("button[data-action]");
  if (actionButton && !actionButton.disabled) {
    const action = actionButton.dataset.action;
    if (action === "toggle-person-dropdown") {
      if (state.personDropdownOpen) {
        closePersonDropdown();
      } else {
        await openPersonDropdown();
      }
      return;
    }
    if (action === "remove-person") {
      await removeCurrentFolderPerson(Number(actionButton.dataset.personId));
      return;
    }
  }

  const personOption = event.target.closest(".person-option");
  if (personOption) {
    await addCurrentFolderPerson(personOption.dataset.personName);
  }
}

function handleMetadataBarInput(event) {
  if (!event.target.classList.contains("person-search-field")) {
    return;
  }
  state.personSearch = event.target.value;
  updatePersonDropdownOptions();
}

async function handleMetadataBarKeydown(event) {
  if (!event.target.classList.contains("person-search-field")) {
    return;
  }

  if (event.key === "Escape") {
    event.preventDefault();
    closePersonDropdown();
    return;
  }

  if (event.key === "Enter") {
    event.preventDefault();
    await addCurrentFolderPerson(event.target.value);
  }
}

async function setCurrentFolderRating(rating) {
  const target = currentFolderMetadataTarget();
  if (!invoke || !target) {
    return;
  }

  const currentRating = state.currentFolderMeta?.rating ?? null;
  const nextRating = currentRating === rating ? null : rating;
  const metadata = await invoke("set_folder_rating", {
    rootId: target.rootId,
    folderId: target.folderId,
    rating: nextRating,
  });
  applyCurrentFolderMetadata(metadata, target);
}

async function addCurrentFolderPerson(name) {
  const target = currentFolderMetadataTarget();
  const cleanName = String(name || "").trim();
  if (!invoke || !target || !cleanName) {
    return;
  }

  const metadata = await invoke("add_folder_person", {
    rootId: target.rootId,
    folderId: target.folderId,
    name: cleanName,
  });
  applyCurrentFolderMetadata(metadata, target);
  state.personSearch = "";
  mergePeopleOptions(state.currentFolderMeta?.people ?? []);
  closePersonDropdown();
  invalidateFolderViewCache(target.rootId);
  await patchCurrentFolderFromDb({ keepStatus: true });
}

async function removeCurrentFolderPerson(personId) {
  const target = currentFolderMetadataTarget();
  if (!invoke || !target || !Number.isFinite(personId)) {
    return;
  }

  const metadata = await invoke("remove_folder_person", {
    rootId: target.rootId,
    folderId: target.folderId,
    personId,
  });
  applyCurrentFolderMetadata(metadata, target);
  invalidateFolderViewCache(target.rootId);
  await patchCurrentFolderFromDb({ keepStatus: true });
}

function applyCurrentFolderMetadata(metadata, target) {
  const currentTarget = currentFolderMetadataTarget();
  if (
    !currentTarget ||
    currentTarget.rootId !== target.rootId ||
    currentTarget.folderId !== target.folderId
  ) {
    return;
  }
  state.currentFolderMeta = normalizeFolderMetadata(metadata, target);
  state.metadataLoading = false;
  mergePeopleOptions(state.currentFolderMeta.people);
  renderMetadataBar();
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
  loadCurrentFolderMetadata().catch(showError);
}

function patchFolderViewInPlace(view, options = {}) {
  const root = currentRoot();
  titleNode.textContent = view.relative_path || root.display_name;
  if (!options.keepStatus) {
    setStatus(
      `${view.folders.length} folders, ${view.images.length} images in this folder`,
    );
  }
  upButton.disabled = false;
  scanButton.disabled = state.activeScans.has(view.root_id);
  renderBreadcrumbs(view);
  state.currentView = cloneFolderView(view);

  const desired = [
    ...view.folders.map((folder) => ({
      key: folderItemKey(folder),
      signature: folderSummarySignature(folder),
      render: () => renderFolderCard(folder),
    })),
    ...view.images.map((image) => ({
      key: imageItemKey(image),
      signature: imageSummarySignature(image),
      render: () => renderImageCard(image),
    })),
  ];

  if (desired.length === 0) {
    renderEmptyState("Empty folder", { keepBreadcrumbs: true });
    clearMetadataSelection();
    return;
  }

  if (gridNode.querySelector(".empty-state")) {
    gridNode.replaceChildren();
  }

  const existingNodes = new Map();
  for (const child of gridNode.children) {
    if (child.dataset?.itemKey) {
      existingNodes.set(child.dataset.itemKey, child);
    }
  }

  const desiredKeys = new Set();
  desired.forEach((item, index) => {
    desiredKeys.add(item.key);
    let node = existingNodes.get(item.key);
    if (node && node.dataset.summarySignature !== item.signature) {
      const replacement = item.render();
      node.replaceWith(replacement);
      node = replacement;
    } else if (!node) {
      node = item.render();
    }

    const currentNode = gridNode.children[index] ?? null;
    if (currentNode !== node) {
      gridNode.insertBefore(node, currentNode);
    }
  });

  for (const child of [...gridNode.children]) {
    if (!desiredKeys.has(child.dataset?.itemKey)) {
      child.remove();
    }
  }
  loadCurrentFolderMetadata().catch(showError);
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
  renderMetadataBar();
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
  renderMetadataBar();
  loadCurrentFolderMetadata().catch(showError);
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
  state.folderLoading = false;
  updateBusyIndicator();
  if (state.currentView.folders.length === 0 && state.currentView.images.length === 0) {
    renderEmptyState("Empty folder", { keepBreadcrumbs: true });
  }
  loadCurrentFolderMetadata().catch(showError);
  restorePendingScrollPosition();
  resumeDeferredThumbnails();
  cacheFolderView(state.currentView);
  setStatus(`${payload.folder_count} folders, ${payload.image_count} images in this folder`);
  scheduleVisibleFolderValidation(100);
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
    slideshow_speed_seconds: normalizeSlideshowSpeed(
      Number(settings?.slideshow_speed_seconds ?? 3),
    ),
    slideshow_loop: Boolean(settings?.slideshow_loop),
    slideshow_ignore_smaller_than: normalizeIgnoreSmallerValue(
      Number(settings?.slideshow_ignore_smaller_than ?? 0),
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
  slideshowLoopInput.checked = state.settings.slideshow_loop;
  syncSlideshowSpeedControls();
  slideshowIgnoreSmallerInput.value = String(state.settings.slideshow_ignore_smaller_than);
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
  state.settings.slideshow_speed_seconds = normalizeSlideshowSpeed(
    Number(slideshowSpeedInput.value),
  );
  syncSlideshowSpeedControls({ keepNumberFocus: false });
  if (state.slideshowActive) {
    scheduleSlideshow();
  }
}

function handleSlideshowSpeedNumberInput() {
  const value = Number(slideshowSpeedNumberInput.value);
  if (!Number.isFinite(value) || value <= 0) {
    return;
  }

  state.settings.slideshow_speed_seconds = roundSlideshowSpeed(value);
  syncSlideshowSpeedControls({ keepNumberFocus: true });
  if (state.slideshowActive) {
    scheduleSlideshow();
  }
}

function handleSettingsInput() {
  state.settings.upscale_fullscreen_images = upscaleFullscreenInput.checked;
  state.settings.slideshow_loop = slideshowLoopInput.checked;
  state.settings.slideshow_speed_seconds = speedFromControls();
  syncSlideshowSpeedControls();
  state.settings.slideshow_ignore_smaller_than = normalizeIgnoreSmallerValue(
    Number(slideshowIgnoreSmallerInput.value),
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
      slideshow_loop: state.settings.slideshow_loop,
      slideshow_ignore_smaller_than: state.settings.slideshow_ignore_smaller_than,
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

function normalizeSlideshowSpeed(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return 3;
  }
  return roundSlideshowSpeed(value);
}

function roundSlideshowSpeed(value) {
  return Math.round(value * 1000) / 1000;
}

function sliderSpeedValue(value) {
  if (!Number.isFinite(value)) {
    return 3;
  }
  return Math.min(10, Math.max(0.1, value));
}

function speedFromControls() {
  const numberValue = Number(slideshowSpeedNumberInput.value);
  if (Number.isFinite(numberValue) && numberValue > 0) {
    return roundSlideshowSpeed(numberValue);
  }
  return normalizeSlideshowSpeed(Number(slideshowSpeedInput.value));
}

function syncSlideshowSpeedControls(options = {}) {
  const value = normalizeSlideshowSpeed(state.settings.slideshow_speed_seconds);
  state.settings.slideshow_speed_seconds = value;
  slideshowSpeedInput.value = String(sliderSpeedValue(value));
  if (!options.keepNumberFocus || document.activeElement !== slideshowSpeedNumberInput) {
    slideshowSpeedNumberInput.value = value.toFixed(3);
  }
}

function normalizeIgnoreSmallerValue(value) {
  return [512, 800, 1024].includes(value) ? value : 0;
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

function thumbnailCacheKey(rootId, imageId, size) {
  return `${rootId}:${imageId}:${size}`;
}

function normalizeRelativePath(relativePath) {
  return String(relativePath || "")
    .replaceAll("\\", "/")
    .replace(/^\/+|\/+$/g, "");
}

function ancestorPaths(relativePath) {
  const normalized = normalizeRelativePath(relativePath);
  const paths = [normalized];
  let parent = parentPathFor(normalized);
  while (parent !== null) {
    paths.push(parent);
    parent = parentPathFor(parent);
  }
  return paths;
}

function pathContainsPath(ancestor, descendant) {
  const normalizedAncestor = normalizeRelativePath(ancestor);
  const normalizedDescendant = normalizeRelativePath(descendant);
  if (!normalizedAncestor) {
    return true;
  }
  return (
    normalizedDescendant === normalizedAncestor ||
    normalizedDescendant.startsWith(`${normalizedAncestor}/`)
  );
}

function folderItemKey(folder) {
  return `folder:${folder.relative_path}`;
}

function imageItemKey(image) {
  return `image:${image.id}`;
}

function folderSummarySignature(folder) {
  return JSON.stringify([
    folder.id,
    folder.relative_path,
    folder.name,
    folder.parent_relative_path,
    folder.thumbnail_image_id,
    folder.image_count,
    folder.child_folder_count,
    folder.direct_keywords ?? [],
    folder.inherited_keywords ?? [],
    folder.direct_people ?? [],
    folder.inherited_people ?? [],
  ]);
}

function imageSummarySignature(image) {
  return JSON.stringify([
    image.id,
    image.folder_id,
    image.file_name,
    image.relative_path,
    image.width,
    image.height,
    image.file_size,
    image.modified_unix_ms,
  ]);
}

function cloneFolderView(view) {
  return {
    ...view,
    folders: view.folders.map((folder) => ({
      ...folder,
      inherited_keywords: [...(folder.inherited_keywords ?? [])],
      direct_keywords: [...(folder.direct_keywords ?? [])],
      inherited_people: [...(folder.inherited_people ?? [])],
      direct_people: [...(folder.direct_people ?? [])],
    })),
    images: view.images.map((image) => ({ ...image })),
  };
}

function cachedFolderView(rootId, relativePath) {
  const key = viewKey(rootId, relativePath);
  const cached = state.folderViewCache.get(key);
  if (!cached) {
    return null;
  }

  state.folderViewCache.delete(key);
  state.folderViewCache.set(key, cached);
  return cloneFolderView(cached);
}

function cacheFolderView(view) {
  const key = viewKey(view.root_id, view.relative_path);
  state.folderViewCache.delete(key);
  state.folderViewCache.set(key, cloneFolderView(view));
  trimMapToSize(state.folderViewCache, MAX_FOLDER_VIEW_CACHE_ENTRIES);
}

function invalidateFolderViewCache(rootId, relativePath = undefined) {
  if (rootId === null || rootId === undefined) {
    state.folderViewCache.clear();
    return;
  }

  if (relativePath === undefined) {
    const prefix = `${rootId}:`;
    for (const key of [...state.folderViewCache.keys()]) {
      if (key.startsWith(prefix)) {
        state.folderViewCache.delete(key);
      }
    }
    return;
  }

  state.folderViewCache.delete(viewKey(rootId, relativePath));
}

function invalidateFolderCachesForChanges(rootId, relativePaths) {
  for (const relativePath of relativePaths ?? []) {
    for (const path of ancestorPaths(relativePath)) {
      invalidateFolderViewCache(rootId, path);
    }
  }
}

function trimMapToSize(map, maxEntries) {
  while (map.size > maxEntries) {
    const oldestKey = map.keys().next().value;
    map.delete(oldestKey);
  }
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
  card.dataset.rootId = folder.root_id;
  card.dataset.folderPath = folder.relative_path;
  card.dataset.itemKey = folderItemKey(folder);
  card.dataset.summarySignature = folderSummarySignature(folder);
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

function renderImageCard(image) {
  const card = document.createElement("article");
  card.className = "tile image-tile";
  card.tabIndex = 0;
  card.title = fullImagePath(image);
  card.dataset.imageId = String(image.id);
  card.dataset.itemKey = imageItemKey(image);
  card.dataset.summarySignature = imageSummarySignature(image);
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

  card.addEventListener("click", () => openViewerByImageId(image.id));
  card.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      openViewerByImageId(image.id);
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
  invalidateFolderViewCache(image.root_id, parentPathFor(state.currentPath));
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
    state.contextMenuFolder = null;
    showContextMenu(
      [{ action: "remove-root", label: "Remove root" }],
      event.clientX,
      event.clientY,
    );
    return;
  }

  const folderTile = event.target.closest(".folder-tile");
  if (folderTile) {
    const folder = folderByPath(folderTile.dataset.folderPath);
    if (!folder) {
      hideThumbContextMenu();
      return;
    }

    state.contextMenuFolder = folder;
    state.contextMenuImage = null;
    state.contextMenuRoot = null;
    showContextMenu(
      [
        { action: "play-folder-slideshow", label: "Play slideshow" },
        {
          action: "play-folder-slideshow-random",
          label: "Play slideshow randomized",
        },
      ],
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
  state.contextMenuFolder = null;
  state.contextMenuRoot = null;
  showContextMenu(imageContextMenuItems(), event.clientX, event.clientY);
}

function handleDocumentClick(event) {
  if (!thumbContextMenu.contains(event.target)) {
    hideThumbContextMenu();
  }
  if (state.personDropdownOpen && shouldClosePersonDropdownForClick(event.target)) {
    closePersonDropdown();
  }
}

function shouldClosePersonDropdownForClick(target) {
  if (target.closest(".person-dropdown")) {
    return false;
  }
  if (target.closest("button[data-action='toggle-person-dropdown']")) {
    return false;
  }
  return true;
}

async function handleThumbContextAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  const image = state.contextMenuImage;
  const folder = state.contextMenuFolder;
  const root = state.contextMenuRoot;
  const viewerId = button.dataset.viewerId;
  hideThumbContextMenu();

  try {
    if (action === "remove-root" && root) {
      await removeRoot(root.id);
    } else if (action === "play-folder-slideshow" && folder) {
      await playFolderSlideshow(folder, { randomized: false });
    } else if (action === "play-folder-slideshow-random" && folder) {
      await playFolderSlideshow(folder, { randomized: true });
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
  state.contextMenuFolder = null;
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
  state.imageDimensionCache.clear();
  invalidateThumbnailDataCache(image.root_id);
  invalidateFolderViewCache(image.root_id, state.currentPath);
  invalidateFolderViewCache(image.root_id, parentPathFor(state.currentPath));
  await refreshCurrentFolder({ keepStatus: true, forceReload: true });
  setStatus(`Rotated ${image.file_name}`);
}

async function moveImageToRecycleBin(image) {
  setStatus(`Moving ${image.file_name} to recycle bin...`);
  await invoke("move_image_to_recycle_bin", {
    rootId: image.root_id,
    imageId: image.id,
  });
  state.imageUrlCache.clear();
  state.imageDimensionCache.clear();
  invalidateThumbnailDataCache(image.root_id);
  invalidateFolderViewCache(image.root_id, state.currentPath);
  invalidateFolderViewCache(image.root_id, parentPathFor(state.currentPath));
  await refreshCurrentFolder({ keepStatus: true, forceReload: true });
  setStatus(`Moved ${image.file_name} to recycle bin`);
}

async function playFolderSlideshow(folder, options = {}) {
  if (!invoke) {
    return;
  }

  setStatus(`Loading slideshow for ${folder.name}...`);
  const images = await invoke("recursive_folder_images", {
    rootId: folder.root_id,
    relativePath: folder.relative_path,
  });
  if (!images || images.length === 0) {
    setStatus(`${folder.name} has no images`);
    return;
  }

  startPlaylistSlideshow(images, options);
}

function startPlaylistSlideshow(images, options = {}) {
  state.slideshowPlaylist = options.randomized ? shuffleImages(images) : [...images];
  state.viewerIndex = 0;
  state.slideshowActive = true;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
  viewer.classList.remove("hidden");
  viewer.focus({ preventScroll: true });
  showViewerCursorTemporarily();
  applyViewerUpscaleSetting();
  enterViewerFullscreen().catch(showError);
  renderViewerImage().catch(showError);
}

function shuffleImages(images) {
  const shuffled = [...images];
  for (let index = shuffled.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(Math.random() * (index + 1));
    [shuffled[index], shuffled[swapIndex]] = [shuffled[swapIndex], shuffled[index]];
  }
  return shuffled;
}

function imageById(imageId) {
  return state.currentView?.images.find((image) => image.id === imageId) ?? null;
}

function imageIndexById(imageId) {
  return state.currentView?.images.findIndex((image) => image.id === imageId) ?? -1;
}

function folderByPath(relativePath) {
  return (
    state.currentView?.folders.find((folder) => folder.relative_path === relativePath) ?? null
  );
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

function invalidateThumbnailDataCache(rootId = undefined) {
  if (rootId === undefined || rootId === null) {
    state.thumbnailDataCache.clear();
    return;
  }

  const prefix = `${rootId}:`;
  for (const key of [...state.thumbnailDataCache.keys()]) {
    if (key.startsWith(prefix)) {
      state.thumbnailDataCache.delete(key);
    }
  }
}

function applyThumbnailData(target, dataUrl) {
  delete target.dataset.thumbnailDeferred;
  target.replaceChildren();
  target.style.backgroundImage = `url("${dataUrl}")`;
  target.style.backgroundSize = "contain";
  target.style.backgroundPosition = "center";
  target.style.backgroundRepeat = "no-repeat";
  target.classList.add("loaded");
  target.classList.remove("failed");
}

function thumbnailsPausedFor(rootId) {
  return (
    state.activeScans.has(rootId) ||
    (state.folderLoading && rootId === state.currentRootId)
  );
}

function resumeDeferredThumbnails() {
  for (const target of gridNode.querySelectorAll(".thumb[data-thumbnail-deferred='true']")) {
    const rootId = target.dataset.rootId;
    if (thumbnailsPausedFor(rootId)) {
      continue;
    }

    delete target.dataset.thumbnailDeferred;
    requestThumbnailWhenVisible(
      rootId,
      Number(target.dataset.imageId),
      target,
      Number(target.dataset.thumbSize),
    );
  }
}

function requestThumbnailWhenVisible(rootId, imageId, target, size) {
  const generation = state.viewGeneration;
  target.dataset.rootId = rootId;
  target.dataset.imageId = String(imageId);
  target.dataset.thumbSize = String(size);
  target.dataset.generation = String(generation);

  const cached = state.thumbnailDataCache.get(thumbnailCacheKey(rootId, imageId, size));
  if (cached) {
    applyThumbnailData(target, cached);
    return;
  }

  if (thumbnailsPausedFor(rootId)) {
    target.dataset.thumbnailDeferred = "true";
    return;
  }

  delete target.dataset.thumbnailDeferred;
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
  const cached = state.thumbnailDataCache.get(thumbnailCacheKey(rootId, imageId, size));
  if (cached) {
    applyThumbnailData(target, cached);
    return;
  }

  if (thumbnailsPausedFor(rootId)) {
    target.dataset.thumbnailDeferred = "true";
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
    if (thumbnailsPausedFor(job.rootId)) {
      job.target.dataset.thumbnailDeferred = "true";
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
          !thumbnailsPausedFor(job.rootId) &&
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
  if (thumbnailsPausedFor(rootId)) {
    target.dataset.thumbnailDeferred = "true";
    return;
  }

  target.dataset.imageId = String(imageId);
  const cacheKey = thumbnailCacheKey(rootId, imageId, size);
  const thumbnail = await invoke("thumbnail", { rootId, imageId, size });
  state.thumbnailDataCache.delete(cacheKey);
  state.thumbnailDataCache.set(cacheKey, thumbnail.data_url);
  trimMapToSize(state.thumbnailDataCache, MAX_THUMBNAIL_DATA_CACHE_ENTRIES);
  if (
    generation !== state.viewGeneration ||
    target.dataset.imageId !== String(imageId)
  ) {
    return;
  }
  applyThumbnailData(target, thumbnail.data_url);
}

function openViewer(index) {
  state.slideshowPlaylist = null;
  state.slideshowActive = false;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
  state.viewerIndex = index;
  viewer.classList.remove("hidden");
  viewer.focus({ preventScroll: true });
  showViewerCursorTemporarily();
  applyViewerUpscaleSetting();
  enterViewerFullscreen().catch(showError);
  renderViewerImage().catch(showError);
}

function openViewerByImageId(imageId) {
  const index = imageIndexById(imageId);
  if (index >= 0) {
    openViewer(index);
  }
}

async function renderViewerImage() {
  const image = currentViewerImage();
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

  if (state.slideshowActive && state.settings.slideshow_ignore_smaller_than > 0) {
    const dimensions = await imageDimensionsFor(image, source);
    if (generation !== state.viewerGeneration) {
      return;
    }
    if (shouldIgnoreSlide(dimensions)) {
      advanceSlideshow({ fromFilter: true });
      return;
    }
  }

  state.slideshowSkipAttempts = 0;
  viewerImage.src = source;
  preloadNeighborImages(generation);
  if (state.slideshowActive) {
    scheduleSlideshow();
  }
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

function viewerImages() {
  return state.slideshowPlaylist ?? state.currentView?.images ?? [];
}

function currentViewerImage() {
  return viewerImages()[state.viewerIndex] ?? null;
}

async function imageDimensionsFor(image, source) {
  if (image.width && image.height) {
    return { width: image.width, height: image.height };
  }

  const cacheKey = `${image.root_id}:${image.id}:${image.modified_unix_ms}`;
  const cached = state.imageDimensionCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const dimensions = await loadImageDimensions(source);
  state.imageDimensionCache.set(cacheKey, dimensions);
  return dimensions;
}

function loadImageDimensions(source) {
  return new Promise((resolve) => {
    const probe = new Image();
    probe.onload = () => {
      resolve({
        width: probe.naturalWidth || 0,
        height: probe.naturalHeight || 0,
      });
    };
    probe.onerror = () => resolve({ width: 0, height: 0 });
    probe.src = source;
  });
}

function shouldIgnoreSlide(dimensions) {
  const threshold = state.settings.slideshow_ignore_smaller_than;
  if (!threshold || !dimensions) {
    return false;
  }

  return Math.max(dimensions.width, dimensions.height) < threshold;
}

function preloadNeighborImages(generation) {
  const images = viewerImages();
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
  if (!state.slideshowPlaylist) {
    state.slideshowPlaylist = [...(state.currentView?.images ?? [])];
  }

  if (state.slideshowPlaylist.length === 0) {
    return;
  }

  if (state.slideshowEnded || state.viewerIndex >= state.slideshowPlaylist.length) {
    state.viewerIndex = 0;
  }

  state.slideshowActive = true;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
  renderViewerImage().catch(showError);
}

function stopSlideshow(options = {}) {
  state.slideshowActive = false;
  state.slideshowEnded = Boolean(options.ended);
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
    advanceSlideshow();
  }, state.settings.slideshow_speed_seconds * 1000);
}

function advanceSlideshow(options = {}) {
  const images = viewerImages();
  if (images.length === 0) {
    stopSlideshow({ ended: true });
    return;
  }

  if (options.fromFilter) {
    state.slideshowSkipAttempts += 1;
    if (state.slideshowSkipAttempts >= images.length) {
      stopSlideshow({ ended: true });
      setStatus("No slideshow images match the size filter");
      return;
    }
  }

  const atLast = state.viewerIndex >= images.length - 1;
  if (atLast) {
    if (!state.settings.slideshow_loop) {
      stopSlideshow({ ended: true });
      return;
    }
    state.viewerIndex = 0;
  } else {
    state.viewerIndex += 1;
  }

  renderViewerImage().catch(showError);
}

function jumpToFirstViewerImage() {
  if (!state.slideshowPlaylist) {
    state.slideshowPlaylist = null;
  }
  const images = viewerImages();
  if (images.length === 0) {
    return;
  }

  state.viewerIndex = 0;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
  renderViewerImage().catch(showError);
}

function randomizeCurrentSlideshow() {
  if (!state.slideshowActive) {
    return;
  }

  const images = viewerImages();
  if (images.length === 0) {
    return;
  }

  state.slideshowPlaylist = shuffleImages(images);
  state.viewerIndex = 0;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
  renderViewerImage().catch(showError);
}

function withCacheBuster(source, modifiedUnixMs) {
  const separator = source.includes("?") ? "&" : "?";
  return `${source}${separator}v=${encodeURIComponent(modifiedUnixMs)}`;
}

function handleViewerWheel(event) {
  if (viewer.classList.contains("hidden")) {
    return;
  }

  showViewerCursorTemporarily();
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

function handleViewerMouseMove() {
  if (viewer.classList.contains("hidden")) {
    return;
  }

  showViewerCursorTemporarily();
}

function showViewerCursorTemporarily() {
  viewer.dataset.cursorHidden = "false";
  if (state.viewerCursorTimer) {
    window.clearTimeout(state.viewerCursorTimer);
  }

  state.viewerCursorTimer = window.setTimeout(() => {
    if (!viewer.classList.contains("hidden")) {
      viewer.dataset.cursorHidden = "true";
    }
  }, VIEWER_CURSOR_HIDE_DELAY_MS);
}

function resetViewerCursor() {
  if (state.viewerCursorTimer) {
    window.clearTimeout(state.viewerCursorTimer);
    state.viewerCursorTimer = null;
  }
  viewer.dataset.cursorHidden = "false";
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
  const images = viewerImages();
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
  resetViewerCursor();
  state.slideshowPlaylist = null;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
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

function updateBusyIndicator() {
  if (!busyIndicator || !busyText) {
    return;
  }

  const message = busyMessage();
  if (!message) {
    busyIndicator.classList.add("hidden");
    busyText.textContent = "Working";
    return;
  }

  busyText.textContent = message;
  busyIndicator.classList.remove("hidden");
}

function busyMessage() {
  if (state.folderLoading) {
    return "Loading folder";
  }

  if (state.visibleValidationActive) {
    return "Checking visible folders";
  }

  if (state.activeScans.size === 0) {
    return "";
  }

  if (state.currentRootId && state.scanProgressText.has(state.currentRootId)) {
    return state.scanProgressText.get(state.currentRootId);
  }

  const firstScanRoot = state.activeScans.values().next().value;
  if (state.activeScans.size === 1) {
    return state.scanProgressText.get(firstScanRoot) ?? "Scanning";
  }

  return `${state.activeScans.size} scans running`;
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
