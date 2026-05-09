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
const RAW_PLY_DIRECT_LOAD_LIMIT_BYTES = 2_000_000_000;
const SUPPORTED_IMAGE_EXTENSIONS = [
  ".jpg",
  ".jpeg",
  ".png",
  ".webp",
  ".gif",
  ".bmp",
  ".tif",
  ".tiff",
  ".avif",
];
const SUPPORTED_SPLAT_EXTENSIONS = [
  ".spz",
  ".sog",
  ".ply",
  ".compressed.ply",
  ".meta.json",
  ".lod-meta.json",
  ".splat",
  ".ksplat",
  ".rad",
];
const SUPPORTED_MODEL_EXTENSIONS = [".glb"];
const RATING_OPTIONS = [1, 2, 3, 4, 5];
const APP_MODES = {
  FOLDER: "folder",
  PERSONS: "persons",
  SEARCH: "search",
};

const gridNode = document.querySelector("#content-grid");
const statusNode = document.querySelector("#status");
const busyIndicator = document.querySelector("#busy-indicator");
const busyText = document.querySelector("#busy-text");
const titleNode = document.querySelector("#view-title");
const metadataBar = document.querySelector("#metadata-bar");
const breadcrumbsNode = document.querySelector("#breadcrumbs");
const addRootButton = document.querySelector("#add-root-button");
const aboutButton = document.querySelector("#about-button");
const settingsButton = document.querySelector("#settings-button");
const backButton = document.querySelector("#back-button");
const forwardButton = document.querySelector("#forward-button");
const thumbScaleInput = document.querySelector("#thumb-scale");
const viewer = document.querySelector("#viewer");
const viewerImage = document.querySelector("#viewer-image");
const splatViewerNode = document.querySelector("#splat-viewer");
const splatStatusNode = document.querySelector("#splat-status");
const viewerCloseHotspot = document.querySelector("#viewer-close-hotspot");
const toastNode = document.querySelector("#toast");
const viewerToastNode = document.querySelector("#viewer-toast");
const thumbContextMenu = document.querySelector("#thumb-context-menu");
const aboutDialog = document.querySelector("#about-dialog");
const aboutCloseButton = document.querySelector("#about-close-button");
const aboutHomepageLink = document.querySelector("#about-homepage-link");
const settingsDialog = document.querySelector("#settings-dialog");
const settingsCloseButton = document.querySelector("#settings-close-button");
const movieProgressDialog = document.querySelector("#movie-progress-dialog");
const movieProgressTitle = document.querySelector("#movie-progress-title");
const movieProgressMeta = document.querySelector("#movie-progress-meta");
const movieProgressOutput = document.querySelector("#movie-progress-output");
const movieCancelButton = document.querySelector("#movie-cancel-button");
const movieCloseButton = document.querySelector("#movie-close-button");
const warningDialog = document.querySelector("#warning-dialog");
const warningMessage = document.querySelector("#warning-message");
const warningDetail = document.querySelector("#warning-detail");
const warningOkButton = document.querySelector("#warning-ok-button");
const warningCancelButton = document.querySelector("#warning-cancel-button");
const metadataEditDialog = document.querySelector("#metadata-edit-dialog");
const metadataEditForm = document.querySelector("#metadata-edit-form");
const metadataEditTitle = document.querySelector("#metadata-edit-title");
const metadataEditLabel = document.querySelector("#metadata-edit-label");
const metadataEditInput = document.querySelector("#metadata-edit-input");
const metadataEditCancelButton = document.querySelector("#metadata-edit-cancel-button");
const upscaleFullscreenInput = document.querySelector("#upscale-fullscreen");
const slideshowLoopInput = document.querySelector("#slideshow-loop");
const slideshowSpeedInput = document.querySelector("#slideshow-speed");
const slideshowSpeedNumberInput = document.querySelector("#slideshow-speed-number");
const slideshowIgnoreSmallerInput = document.querySelector("#slideshow-ignore-smaller");
const jpgQualityInput = document.querySelector("#jpg-quality");
const jpgQualityValue = document.querySelector("#jpg-quality-value");
const movieCreateEnabledInput = document.querySelector("#movie-create-enabled");
const movieSettingsFields = document.querySelector("#movie-settings-fields");
const ffmpegPathInput = document.querySelector("#ffmpeg-path");
const pickFfmpegButton = document.querySelector("#pick-ffmpeg-button");
const movieCodecInput = document.querySelector("#movie-codec");
const movieQualityInput = document.querySelector("#movie-quality");
const movieOutputFolderInput = document.querySelector("#movie-output-folder");
const pickMovieOutputFolderButton = document.querySelector("#pick-movie-output-folder-button");
const clearMovieOutputFolderButton = document.querySelector("#clear-movie-output-folder-button");
const movieResolutionInput = document.querySelector("#movie-resolution");
const movieCustomResolutionRow = document.querySelector("#movie-custom-resolution-row");
const movieCustomResolutionInput = document.querySelector("#movie-custom-resolution");
const movieModeInput = document.querySelector("#movie-mode");
const movieFpsRow = document.querySelector("#movie-fps-row");
const movieFpsInput = document.querySelector("#movie-fps");
const movieSlideshowSecondsRow = document.querySelector("#movie-slideshow-seconds-row");
const movieSlideshowSecondsInput = document.querySelector("#movie-slideshow-seconds");
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
  metadataMode: APP_MODES.FOLDER,
  currentFolderMeta: null,
  metadataLoading: false,
  metadataRequestId: 0,
  searchActive: false,
  searchLoading: false,
  searchRequestId: 0,
  searchResults: [],
  searchDisplayedFolders: [],
  searchPersonResults: [],
  searchPeopleLoaded: false,
  searchPeopleFilterKey: "",
  searchPerson: null,
  searchIncludeTags: [],
  searchIncludeCombine: "and",
  searchExcludeTags: [],
  searchExcludeCombine: "or",
  searchMinimumRating: null,
  searchHierarchy: false,
  searchPersonDropdownOpen: false,
  searchPersonSearch: "",
  searchTagDropdownTarget: null,
  searchTagSearch: "",
  searchSlideshowMenuOpen: false,
  historyBack: [],
  historyForward: [],
  restoringHistory: false,
  peopleOptions: [],
  peopleOptionsLoaded: false,
  tagOptions: [],
  tagOptionsLoaded: false,
  personDropdownOpen: false,
  personSearch: "",
  tagDropdownOpen: false,
  tagSearch: "",
  metadataItemMenu: null,
  metadataEditDialogResolve: null,
  slideshowTimer: null,
  slideshowActive: false,
  slideshowEnded: false,
  slideshowPlaylist: null,
  slideshowSkipAttempts: 0,
  viewerCursorTimer: null,
  splatViewerModule: null,
  splatViewer: null,
  splatThumbnailSaving: false,
  imageDimensionCache: new Map(),
  settings: {
    upscale_fullscreen_images: false,
    slideshow_speed_seconds: 3,
    slideshow_loop: false,
    slideshow_ignore_smaller_than: 0,
    jpg_quality: 90,
    movie_create_enabled: false,
    ffmpeg_path: "",
    movie_codec: "h264",
    movie_quality: "balanced",
    movie_output_folder: "",
    movie_resolution: "1080p",
    movie_custom_resolution: "1920x1080",
    movie_mode: "movie",
    movie_fps: 30,
    movie_slideshow_seconds: 3,
    external_viewers: [],
  },
  movieJob: null,
  warningDialogResolve: null,
  toastTimer: null,
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
aboutButton.addEventListener("click", openAboutDialog);
settingsButton.addEventListener("click", openSettingsDialog);
backButton.addEventListener("click", () => goBackHistory().catch(showError));
forwardButton.addEventListener("click", () => goForwardHistory().catch(showError));
thumbScaleInput.addEventListener("input", handleThumbScaleInput);
aboutCloseButton.addEventListener("click", closeAboutDialog);
aboutHomepageLink.addEventListener("click", openAboutHomepage);
settingsCloseButton.addEventListener("click", closeSettingsDialog);
movieCancelButton.addEventListener("click", cancelActiveMovieCreation);
movieCloseButton.addEventListener("click", closeMovieProgressDialog);
movieProgressDialog.addEventListener("cancel", (event) => {
  if (state.movieJob?.running) {
    event.preventDefault();
  }
});
warningOkButton.addEventListener("click", () => resolveWarningDialog(true));
warningCancelButton.addEventListener("click", () => resolveWarningDialog(false));
warningDialog.addEventListener("cancel", (event) => {
  event.preventDefault();
  resolveWarningDialog(false);
});
metadataEditForm.addEventListener("submit", (event) => {
  event.preventDefault();
  resolveMetadataEditDialog(metadataEditInput.value);
});
metadataEditCancelButton.addEventListener("click", () => resolveMetadataEditDialog(null));
metadataEditDialog.addEventListener("cancel", (event) => {
  event.preventDefault();
  resolveMetadataEditDialog(null);
});
metadataBar.addEventListener("click", (event) => {
  handleMetadataBarClick(event).catch(showError);
});
metadataBar.addEventListener("input", handleMetadataBarInput);
metadataBar.addEventListener("keydown", (event) => {
  handleMetadataBarKeydown(event).catch(showError);
});
metadataBar.addEventListener(
  "scroll",
  (event) => {
    if (event.target.closest?.(".person-options, .tag-options")) {
      clearMetadataItemMenu();
    }
  },
  true,
);
upscaleFullscreenInput.addEventListener("change", handleSettingsInput);
slideshowLoopInput.addEventListener("change", handleSettingsInput);
slideshowSpeedInput.addEventListener("input", handleSlideshowSpeedInput);
slideshowSpeedInput.addEventListener("change", handleSettingsInput);
slideshowSpeedNumberInput.addEventListener("input", handleSlideshowSpeedNumberInput);
slideshowSpeedNumberInput.addEventListener("change", handleSettingsInput);
slideshowIgnoreSmallerInput.addEventListener("change", handleSettingsInput);
jpgQualityInput.addEventListener("input", handleJpgQualityInput);
jpgQualityInput.addEventListener("change", handleSettingsInput);
movieCreateEnabledInput.addEventListener("change", handleSettingsInput);
pickFfmpegButton.addEventListener("click", pickFfmpegPath);
movieCodecInput.addEventListener("change", handleSettingsInput);
movieQualityInput.addEventListener("change", handleSettingsInput);
pickMovieOutputFolderButton.addEventListener("click", pickMovieOutputFolder);
clearMovieOutputFolderButton.addEventListener("click", clearMovieOutputFolder);
movieResolutionInput.addEventListener("change", handleSettingsInput);
movieCustomResolutionInput.addEventListener("input", handleMovieCustomResolutionInput);
movieCustomResolutionInput.addEventListener("change", handleSettingsInput);
movieModeInput.addEventListener("change", handleSettingsInput);
movieFpsInput.addEventListener("change", handleSettingsInput);
movieSlideshowSecondsInput.addEventListener("input", handleMovieSlideshowSecondsInput);
movieSlideshowSecondsInput.addEventListener("change", handleSettingsInput);
addExternalViewerButton.addEventListener("click", addExternalViewer);
viewerCloseHotspot.addEventListener("click", closeViewer);
viewer.addEventListener("wheel", handleViewerWheel, { passive: false });
viewer.addEventListener("mousemove", handleViewerMouseMove);
document.addEventListener("fullscreenchange", handleBrowserFullscreenChange);
document.addEventListener("contextmenu", handleDocumentContextMenu);
document.addEventListener("click", handleDocumentClick);
window.addEventListener("blur", hideThumbContextMenu);
window.addEventListener("resize", () => {
  hideThumbContextMenu();
  clearMetadataItemMenu();
});
gridNode.addEventListener("scroll", () => scheduleVisibleFolderValidation(250), {
  passive: true,
});
thumbContextMenu.addEventListener("click", handleThumbContextAction);

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !thumbContextMenu.classList.contains("hidden")) {
    hideThumbContextMenu();
    return;
  }

  if (
    event.altKey &&
    event.key === "ArrowLeft" &&
    canGoBackHistory() &&
    viewer.classList.contains("hidden")
  ) {
    event.preventDefault();
    goBackHistory().catch(showError);
    return;
  }

  if (
    event.altKey &&
    event.key === "ArrowRight" &&
    canGoForwardHistory() &&
    viewer.classList.contains("hidden")
  ) {
    event.preventDefault();
    goForwardHistory().catch(showError);
    return;
  }

  if (
    event.key === "Escape" &&
    (state.personDropdownOpen ||
      state.tagDropdownOpen ||
      state.searchPersonDropdownOpen ||
      state.searchTagDropdownTarget ||
      state.searchSlideshowMenuOpen)
  ) {
    closeMetadataDropdowns();
    closeSearchDropdowns();
    return;
  }

  if (viewer.classList.contains("hidden")) {
    return;
  }

  if (event.key === "Escape") {
    closeViewer();
  } else if (currentViewerIsThreeD() && event.key.toLowerCase() === "t") {
    event.preventDefault();
    saveCurrentAssetThumbnail().catch(showError);
  } else if (currentViewerIsSplat() && event.key.toLowerCase() === "o") {
    event.preventDefault();
    cycleCurrentSplatOrientation();
  } else if (currentViewerIsThreeD() && event.key.toLowerCase() === "r") {
    event.preventDefault();
    resetCurrentSplatView();
  } else if (currentViewerIsThreeD() && event.key.toLowerCase() === "f") {
    event.preventDefault();
    frameCurrentSplatView();
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
    updateRescanButton();
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
      updateRescanButton();
      if (!state.atRootOverview) {
        await refreshCurrentFolder({ keepStatus: true, forceReload: true });
      }
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
      updateRescanButton();
      resumeDeferredThumbnails();
    }
    renderRootOverviewIfVisible({ keepStatus: true, keepScroll: true });
    if (payload.root_id === state.currentRootId) {
      setStatus(payload.message);
    } else if (state.atRootOverview) {
      setStatus(payload.message);
    }
  });

  await listen("movie-create-output", ({ payload }) => {
    appendMovieProgressOutput(payload);
  });

  await listen("movie-create-finished", ({ payload }) => {
    finishMovieProgress(payload);
  });
}

async function refreshOverview() {
  const overview = await invoke("library_overview");
  state.roots = overview.roots;
  state.peopleOptionsLoaded = false;
  state.tagOptionsLoaded = false;
  invalidateSearchCaches();
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
  updateRescanButton();
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

  const confirmed = await confirmWarning(
    `Remove ${root.display_name} from Picturious?`,
    "The root database and pictures are not deleted.",
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

  const historyEntry = beginHistoryNavigation();
  if (!state.restoringHistory) {
    rememberCurrentScrollPosition();
  }

  state.metadataMode = APP_MODES.FOLDER;
  closeSearchDropdowns();
  state.searchActive = false;
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
    commitHistoryNavigation(historyEntry);
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
  commitHistoryNavigation(historyEntry);
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
    state.searchActive ||
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
    state.searchActive ||
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

function openRootOverview(options = {}) {
  const historyEntry = beginHistoryNavigation();
  if (!state.restoringHistory) {
    rememberCurrentScrollPosition();
  }
  state.metadataMode = APP_MODES.FOLDER;
  closeSearchDropdowns();
  state.searchActive = false;
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
  updateNavigationButtons();
  resetStreamRenderQueue();
  resetThumbnailWork();
  clearMetadataSelection();
  prepareScrollRestore(null, "", options);
  renderRootOverview(options);
  commitHistoryNavigation(historyEntry);
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
  updateNavigationButtons();
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
    <div class="thumb root-thumb folder-thumb">
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
  state.tagDropdownOpen = false;
  state.tagSearch = "";
  clearMetadataItemMenu();
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
    mergeTagOptions([
      ...state.currentFolderMeta.tags,
      ...state.currentFolderMeta.inherited_tags,
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
    rating: normalizeRating(metadata?.rating),
    inherited_rating: normalizeRating(metadata?.inherited_rating),
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
    tags: Array.isArray(metadata?.tags)
      ? metadata.tags
          .filter((tag) => Number.isFinite(Number(tag?.id)) && tag?.name)
          .map((tag) => ({
            id: Number(tag.id),
            name: String(tag.name),
          }))
      : [],
    inherited_tags: Array.isArray(metadata?.inherited_tags)
      ? metadata.inherited_tags
          .filter((tag) => Number.isFinite(Number(tag?.id)) && tag?.name)
          .map((tag) => ({
            id: Number(tag.id),
            name: String(tag.name),
          }))
      : [],
  };
}

function renderMetadataBar(options = {}) {
  if (state.metadataMode === APP_MODES.SEARCH) {
    renderSearchMetadataBar(options);
    return;
  }

  if (state.metadataMode === APP_MODES.PERSONS) {
    renderPersonsMetadataBar(options);
    return;
  }

  renderEditMetadataBar(options);
}

function renderEditMetadataBar(options = {}) {
  const target = currentFolderMetadataTarget();
  const metadata = target ? state.currentFolderMeta : null;
  const editDisabled = !target || state.metadataMode !== APP_MODES.FOLDER;
  const disabledAttr = editDisabled ? " disabled" : "";
  const rating = normalizeRating(metadata?.rating);
  const inheritedRating = normalizeRating(metadata?.inherited_rating);
  const people = metadata?.people ?? [];
  const inheritedPeople = metadata?.inherited_people ?? [];
  const tags = metadata?.tags ?? [];
  const inheritedTags = metadata?.inherited_tags ?? [];
  const personDropdown = state.personDropdownOpen && !editDisabled
    ? renderPersonDropdownHtml()
    : "";
  const tagDropdown =
    state.tagDropdownOpen && !editDisabled ? renderTagDropdownHtml() : "";
  const rescanDisabled = rescanButtonDisabled() ? " disabled" : "";

  metadataBar.innerHTML = `
    ${renderMetadataModeTabs()}
    <div class="rating-toggle-group" role="group" aria-label="Rating">
      ${RATING_OPTIONS.map((option) => {
        const displayRating = rating ?? inheritedRating ?? 0;
        const active = rating === option;
        const inheritedActive = !rating && inheritedRating === option;
        const filled = displayRating >= option;
        const title = inheritedActive
          ? `${option} of 5 stars inherited from a parent folder`
          : active
            ? `Clear ${option} of 5 star rating`
            : `Set ${option} of 5 stars`;
        return `<button class="rating-toggle" type="button" data-rating="${option}" data-active="${active}" data-filled="${filled}" data-inherited-active="${inheritedActive}" aria-pressed="${active}" title="${title}" aria-label="${title}"${disabledAttr}><span class="star-icon" aria-hidden="true">${filled ? "★" : "☆"}</span></button>`;
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
    <div class="tags-editor">
      <span class="metadata-label">Tags:</span>
      <div class="tag-chips">
        ${tags.map(renderTagChipHtml).join("")}
        ${inheritedTags.map(renderInheritedTagChipHtml).join("")}
      </div>
      <button class="tag-add-button" type="button" data-action="toggle-tag-dropdown" title="Add tag" aria-label="Add tag"${disabledAttr}>+</button>
      ${tagDropdown}
    </div>
    <div class="folder-row-actions">
      <button class="folder-rescan-button" type="button" data-action="rescan-folder" title="Rescan folder" aria-label="Rescan folder"${rescanDisabled}>
        ${iconSvg("refresh")}<span>Rescan</span>
      </button>
    </div>
  `;

  updatePersonDropdownOptions();
  updateTagDropdownOptions();
  if (options.focusPersonInput && state.personDropdownOpen) {
    requestAnimationFrame(() => {
      const input = metadataBar.querySelector(".person-search-field");
      input?.focus({ preventScroll: true });
      input?.select();
    });
  }
  if (options.focusTagInput && state.tagDropdownOpen) {
    requestAnimationFrame(() => {
      const input = metadataBar.querySelector(".tag-search-field");
      input?.focus({ preventScroll: true });
      input?.select();
    });
  }
}

function renderSearchMetadataBar(options = {}) {
  renderSearchFilterMetadataBar({
    ...options,
    showPersonFilter: true,
    showGroupToggle: true,
    showSlideshowButton: true,
  });
}

function renderPersonsMetadataBar(options = {}) {
  renderSearchFilterMetadataBar({
    ...options,
    showPersonFilter: false,
    showGroupToggle: false,
    showSlideshowButton: false,
  });
}

function renderSearchFilterMetadataBar(options = {}) {
  const personDropdown = state.searchPersonDropdownOpen
    ? renderSearchPersonDropdownHtml()
    : "";
  const tagDropdown = state.searchTagDropdownTarget
    ? renderSearchTagDropdownHtml(state.searchTagDropdownTarget)
    : "";
  const minimumRating = normalizeRating(state.searchMinimumRating);
  const showPersonFilter = Boolean(options.showPersonFilter);
  const showGroupToggle = Boolean(options.showGroupToggle);
  const showSlideshowButton = Boolean(options.showSlideshowButton);
  const slideshowDisabled = !searchSlideshowAvailable();
  const slideshowMenu =
    showSlideshowButton && state.searchSlideshowMenuOpen && !slideshowDisabled
      ? renderSearchSlideshowMenuHtml()
      : "";

  metadataBar.innerHTML = `
    ${renderMetadataModeTabs()}
    ${showGroupToggle ? `<button class="search-group-toggle" type="button" data-action="toggle-search-hierarchy" data-active="${state.searchHierarchy}" aria-pressed="${state.searchHierarchy}" title="Group by folder structure" aria-label="Group by folder structure">
      <span class="mode-icon">${iconSvg("folderTree")}</span><span>Group</span>
    </button>` : ""}
    <button class="search-reset-button" type="button" data-action="reset-search-filters" title="Reset search filters" aria-label="Reset search filters">
      ${iconSvg("reset")}<span>Reset</span>
    </button>
    <div class="rating-toggle-group search-rating-group" role="group" aria-label="Minimum rating">
      ${RATING_OPTIONS.map((option) => {
        const active = minimumRating === option;
        const filled = Boolean(minimumRating && minimumRating >= option);
        const title = active
          ? `Clear minimum ${option} star rating`
          : `Minimum ${option} stars`;
        return `<button class="rating-toggle" type="button" data-rating="${option}" data-active="${active}" data-filled="${filled}" aria-pressed="${active}" title="${title}" aria-label="${title}"><span class="star-icon" aria-hidden="true">${filled ? "&#9733;" : "&#9734;"}</span></button>`;
      }).join("")}
    </div>
    ${showPersonFilter ? `<div class="people-editor search-person-editor">
      <span class="metadata-label">Person:</span>
      <div class="person-chips">
        ${state.searchPerson ? renderSearchPersonChipHtml(state.searchPerson) : ""}
      </div>
      <button class="person-add-button" type="button" data-action="toggle-search-person-dropdown" title="Choose person" aria-label="Choose person">+</button>
      ${personDropdown}
    </div>` : ""}
    <div class="tags-editor search-tags-editor" data-search-tag-list="include">
      <span class="metadata-label">Include:</span>
      ${renderSearchCombineHtml("include", state.searchIncludeCombine)}
      <div class="tag-chips">
        ${state.searchIncludeTags.map((tag) => renderSearchTagChipHtml(tag, "include")).join("")}
      </div>
      <button class="tag-add-button" type="button" data-action="toggle-search-tag-dropdown" data-tag-list="include" title="Add include tag" aria-label="Add include tag">+</button>
      ${state.searchTagDropdownTarget === "include" ? tagDropdown : ""}
    </div>
    <div class="tags-editor search-tags-editor" data-search-tag-list="exclude">
      <span class="metadata-label">Exclude:</span>
      ${renderSearchCombineHtml("exclude", state.searchExcludeCombine)}
      <div class="tag-chips">
        ${state.searchExcludeTags.map((tag) => renderSearchTagChipHtml(tag, "exclude")).join("")}
      </div>
      <button class="tag-add-button" type="button" data-action="toggle-search-tag-dropdown" data-tag-list="exclude" title="Add exclude tag" aria-label="Add exclude tag">+</button>
      ${state.searchTagDropdownTarget === "exclude" ? tagDropdown : ""}
    </div>
    ${showSlideshowButton ? `<div class="search-actions">
      <button class="icon-button search-play-button" type="button" data-action="toggle-search-slideshow-menu" title="Play search results" aria-label="Play search results"${slideshowDisabled ? " disabled" : ""}>
        ${iconSvg("play")}
      </button>
      ${slideshowMenu}
    </div>` : ""}
  `;

  if (showPersonFilter) {
    updateSearchPersonDropdownOptions();
  }
  updateSearchTagDropdownOptions();
  if (showPersonFilter && options.focusSearchPersonInput && state.searchPersonDropdownOpen) {
    requestAnimationFrame(() => {
      const input = metadataBar.querySelector(".search-person-search-field");
      input?.focus({ preventScroll: true });
      input?.select();
    });
  }
  if (options.focusSearchTagInput && state.searchTagDropdownTarget) {
    requestAnimationFrame(() => {
      const input = metadataBar.querySelector(".search-tag-search-field");
      input?.focus({ preventScroll: true });
      input?.select();
    });
  }
}

function renderSearchSlideshowMenuHtml() {
  return `
    <div class="search-slideshow-menu" role="menu">
      <button type="button" data-action="play-search-slideshow">Play slideshow from search results</button>
      <button type="button" data-action="play-search-slideshow-random">Play slideshow from search results randomized</button>
    </div>
  `;
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

function renderTagChipHtml(tag) {
  return `
    <span class="tag-chip">
      <span title="${escapeHtml(tag.name)}">${escapeHtml(tag.name)}</span>
      <button type="button" data-action="remove-tag" data-tag-id="${tag.id}" title="Remove tag" aria-label="Remove tag">x</button>
    </span>
  `;
}

function renderInheritedTagChipHtml(tag) {
  return `
    <span class="tag-chip" data-inherited="true" title="Inherited from a parent folder">
      <span>${escapeHtml(tag.name)}</span>
    </span>
  `;
}

function renderSearchPersonChipHtml(person) {
  return `
    <span class="person-chip">
      <span title="${escapeHtml(person.name)}">${escapeHtml(person.name)}</span>
      <button type="button" data-action="clear-search-person" title="Clear person" aria-label="Clear person">x</button>
    </span>
  `;
}

function renderSearchTagChipHtml(tag, listName) {
  return `
    <span class="tag-chip">
      <span title="${escapeHtml(tag.name)}">${escapeHtml(tag.name)}</span>
      <button type="button" data-action="remove-search-tag" data-tag-list="${listName}" data-tag-name="${escapeHtml(tag.name)}" title="Remove tag" aria-label="Remove tag">x</button>
    </span>
  `;
}

function renderSearchCombineHtml(listName, combineMode) {
  const mode = normalizeCombineMode(combineMode);
  return `
    <div class="search-combine-toggle" role="group" aria-label="${listName} tag combine mode">
      <button type="button" data-action="set-search-tag-combine" data-tag-list="${listName}" data-combine="and" data-active="${mode === "and"}">AND</button>
      <button type="button" data-action="set-search-tag-combine" data-tag-list="${listName}" data-combine="or" data-active="${mode === "or"}">OR</button>
    </div>
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

function renderTagDropdownHtml() {
  return `
    <div class="tag-dropdown">
      <input class="tag-search-field" type="text" value="${escapeHtml(state.tagSearch)}" placeholder="Tag" aria-label="Tag name" />
      <div class="tag-options" role="listbox"></div>
    </div>
  `;
}

function renderSearchPersonDropdownHtml() {
  return `
    <div class="person-dropdown search-person-dropdown">
      <input class="search-person-search-field" type="text" value="${escapeHtml(state.searchPersonSearch)}" placeholder="Name" aria-label="Person name" />
      <div class="search-person-options person-options" role="listbox"></div>
    </div>
  `;
}

function renderSearchTagDropdownHtml(target) {
  return `
    <div class="tag-dropdown search-tag-dropdown" data-search-tag-target="${target}">
      <input class="search-tag-search-field" type="text" value="${escapeHtml(state.searchTagSearch)}" placeholder="Tag" aria-label="Tag name" />
      <div class="search-tag-options tag-options" role="listbox"></div>
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
    ...(metadata?.people ?? []).map((person) => normalizedMetadataName(person.name)),
    ...(metadata?.inherited_people ?? []).map((person) => normalizedMetadataName(person.name)),
  ]);
  const query = state.personSearch.trim().toLowerCase();
  const options = state.peopleOptions
    .filter((person) => !query || person.name.toLowerCase().includes(query))
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
    );

  optionsNode.replaceChildren(
    ...options.map((person) =>
      renderMetadataOptionRow(
        "person",
        person.name,
        assignedNames.has(normalizedMetadataName(person.name)),
      ),
    ),
  );
}

function updateTagDropdownOptions() {
  const optionsNode = metadataBar.querySelector(".tag-options");
  if (!optionsNode) {
    return;
  }

  const metadata = state.currentFolderMeta;
  const assignedNames = new Set([
    ...(metadata?.tags ?? []).map((tag) => normalizedMetadataName(tag.name)),
    ...(metadata?.inherited_tags ?? []).map((tag) => normalizedMetadataName(tag.name)),
  ]);
  const query = state.tagSearch.trim().toLowerCase();
  const options = state.tagOptions
    .filter((tag) => !query || tag.name.toLowerCase().includes(query))
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
    );

  optionsNode.replaceChildren(
    ...options.map((tag) =>
      renderMetadataOptionRow(
        "tag",
        tag.name,
        assignedNames.has(normalizedMetadataName(tag.name)),
      ),
    ),
  );
}

function renderMetadataOptionRow(kind, name, assigned) {
  const row = document.createElement("div");
  row.className = "metadata-option-row";
  row.dataset.metadataKind = kind;
  row.dataset.metadataName = name;

  const mainButton = document.createElement("button");
  mainButton.type = "button";
  mainButton.className = `${kind === "person" ? "person-option" : "tag-option"} metadata-option-main`;
  mainButton.dataset[kind === "person" ? "personName" : "tagName"] = name;
  mainButton.textContent = name;
  mainButton.disabled = assigned;
  if (assigned) {
    mainButton.title = "Already assigned";
  }

  const menuButton = document.createElement("button");
  menuButton.type = "button";
  menuButton.className = "metadata-option-menu-button";
  menuButton.dataset.action = "toggle-metadata-item-menu";
  menuButton.dataset.metadataKind = kind;
  menuButton.dataset.metadataName = name;
  menuButton.title = `${metadataKindLabel(kind)} actions`;
  menuButton.setAttribute("aria-label", `${metadataKindLabel(kind)} actions`);
  menuButton.innerHTML = iconSvg("moreHorizontal");

  row.append(mainButton, menuButton);
  return row;
}

function renderMetadataItemMenu(kind, name) {
  const menu = document.createElement("div");
  menu.className = "metadata-item-menu";
  menu.setAttribute("role", "menu");

  const editButton = document.createElement("button");
  editButton.type = "button";
  editButton.dataset.action = "edit-metadata-item";
  editButton.dataset.metadataKind = kind;
  editButton.dataset.metadataName = name;
  editButton.textContent = "Edit";

  const deleteButton = document.createElement("button");
  deleteButton.type = "button";
  deleteButton.dataset.action = "delete-metadata-item";
  deleteButton.dataset.metadataKind = kind;
  deleteButton.dataset.metadataName = name;
  deleteButton.textContent = "Delete";

  menu.append(editButton, deleteButton);
  return menu;
}

function metadataItemMenuMatches(kind, name) {
  return (
    state.metadataItemMenu?.kind === kind &&
    normalizedMetadataName(state.metadataItemMenu.name) === normalizedMetadataName(name)
  );
}

function metadataKindLabel(kind) {
  return kind === "person" ? "Person" : "Tag";
}

function updateSearchPersonDropdownOptions() {
  const optionsNode = metadataBar.querySelector(".search-person-options");
  if (!optionsNode) {
    return;
  }

  const selectedName = normalizedMetadataName(state.searchPerson?.name);
  const query = state.searchPersonSearch.trim().toLowerCase();
  const options = state.peopleOptions
    .filter((person) => normalizedMetadataName(person.name) !== selectedName)
    .filter((person) => !query || person.name.toLowerCase().includes(query))
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
    );

  optionsNode.replaceChildren(
    ...options.map((person) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "search-person-option person-option";
      button.dataset.personName = person.name;
      button.textContent = person.name;
      return button;
    }),
  );
}

function updateSearchTagDropdownOptions() {
  const optionsNode = metadataBar.querySelector(".search-tag-options");
  if (!optionsNode || !state.searchTagDropdownTarget) {
    return;
  }

  const list = searchTagList(state.searchTagDropdownTarget);
  const assignedNames = new Set(list.map((tag) => normalizedMetadataName(tag.name)));
  const query = state.searchTagSearch.trim().toLowerCase();
  const options = state.tagOptions
    .filter((tag) => !assignedNames.has(normalizedMetadataName(tag.name)))
    .filter((tag) => !query || tag.name.toLowerCase().includes(query))
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
    );

  optionsNode.replaceChildren(
    ...options.map((tag) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "search-tag-option tag-option";
      button.dataset.tagName = tag.name;
      button.textContent = tag.name;
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
  state.tagDropdownOpen = false;
  state.tagSearch = "";
  clearMetadataItemMenu();
  if (!state.peopleOptionsLoaded) {
    state.peopleOptions = [];
  }
  renderMetadataBar({ focusPersonInput: true });
  await loadPeopleOptions();
}

async function openTagDropdown() {
  const target = currentFolderMetadataTarget();
  if (!target) {
    return;
  }

  state.tagDropdownOpen = true;
  state.tagSearch = "";
  state.personDropdownOpen = false;
  state.personSearch = "";
  clearMetadataItemMenu();
  if (!state.tagOptionsLoaded) {
    state.tagOptions = [];
  }
  renderMetadataBar({ focusTagInput: true });
  await loadTagOptions();
}

function closePersonDropdown() {
  if (!state.personDropdownOpen) {
    return;
  }
  state.personDropdownOpen = false;
  state.personSearch = "";
  clearMetadataItemMenu();
  renderMetadataBar();
}

function closeTagDropdown() {
  if (!state.tagDropdownOpen) {
    return;
  }
  state.tagDropdownOpen = false;
  state.tagSearch = "";
  clearMetadataItemMenu();
  renderMetadataBar();
}

function closeMetadataDropdowns() {
  const wasOpen = state.personDropdownOpen || state.tagDropdownOpen || state.metadataItemMenu;
  state.personDropdownOpen = false;
  state.personSearch = "";
  state.tagDropdownOpen = false;
  state.tagSearch = "";
  clearMetadataItemMenu();
  if (wasOpen) {
    renderMetadataBar();
  }
}

async function openSearchPersonDropdown() {
  if (state.metadataMode !== APP_MODES.SEARCH) {
    return;
  }
  state.searchPersonDropdownOpen = true;
  state.searchPersonSearch = "";
  state.searchTagDropdownTarget = null;
  state.searchTagSearch = "";
  state.searchSlideshowMenuOpen = false;
  if (!state.peopleOptionsLoaded) {
    state.peopleOptions = [];
  }
  renderMetadataBar({ focusSearchPersonInput: true });
  await loadPeopleOptions();
}

async function openSearchTagDropdown(target) {
  const listName = normalizeSearchTagListName(target);
  state.searchTagDropdownTarget = listName;
  state.searchTagSearch = "";
  state.searchPersonDropdownOpen = false;
  state.searchPersonSearch = "";
  state.searchSlideshowMenuOpen = false;
  if (!state.tagOptionsLoaded) {
    state.tagOptions = [];
  }
  renderMetadataBar({ focusSearchTagInput: true });
  await loadTagOptions();
}

function closeSearchPersonDropdown() {
  if (!state.searchPersonDropdownOpen) {
    return;
  }
  state.searchPersonDropdownOpen = false;
  state.searchPersonSearch = "";
  renderMetadataBar();
}

function closeSearchTagDropdown() {
  if (!state.searchTagDropdownTarget) {
    return;
  }
  state.searchTagDropdownTarget = null;
  state.searchTagSearch = "";
  renderMetadataBar();
}

function closeSearchDropdowns() {
  const wasOpen =
    state.searchPersonDropdownOpen ||
    state.searchTagDropdownTarget ||
    state.searchSlideshowMenuOpen;
  state.searchPersonDropdownOpen = false;
  state.searchPersonSearch = "";
  state.searchTagDropdownTarget = null;
  state.searchTagSearch = "";
  state.searchSlideshowMenuOpen = false;
  if (wasOpen) {
    renderMetadataBar();
  }
}

function closeSearchSlideshowMenu() {
  if (!state.searchSlideshowMenuOpen) {
    return;
  }
  state.searchSlideshowMenuOpen = false;
  renderMetadataBar();
}

async function loadPeopleOptions() {
  if (!invoke || state.peopleOptionsLoaded) {
    return;
  }

  const options = await invoke("metadata_people");
  state.peopleOptionsLoaded = true;
  state.peopleOptions = normalizePeopleOptions(options);
  renderMetadataBar(
    state.metadataMode === APP_MODES.SEARCH
      ? { focusSearchPersonInput: true }
      : { focusPersonInput: true },
  );
}

async function loadTagOptions() {
  if (!invoke || state.tagOptionsLoaded) {
    return;
  }

  const options = await invoke("metadata_tags");
  state.tagOptionsLoaded = true;
  state.tagOptions = normalizeMetadataOptions(options);
  renderMetadataBar(
    isFilterMode()
      ? { focusSearchTagInput: true }
      : { focusTagInput: true },
  );
}

function normalizePeopleOptions(options) {
  return normalizeMetadataOptions(options);
}

function normalizeMetadataOptions(options) {
  return Array.isArray(options)
    ? options
        .filter((item) => Number.isFinite(Number(item?.id)) && item?.name)
        .map((item) => ({
          id: Number(item.id),
          name: String(item.name),
        }))
    : [];
}

function mergePeopleOptions(people) {
  if (!Array.isArray(people) || people.length === 0) {
    return;
  }

  const existing = new Set(
    state.peopleOptions.map((person) => normalizedMetadataName(person.name)),
  );
  for (const person of people) {
    const key = normalizedMetadataName(person.name);
    if (!existing.has(key)) {
      state.peopleOptions.push({
        id: Number(person.id),
        name: String(person.name),
      });
      existing.add(key);
    }
  }
}

function mergeTagOptions(tags) {
  if (!Array.isArray(tags) || tags.length === 0) {
    return;
  }

  const existing = new Set(
    state.tagOptions.map((tag) => normalizedMetadataName(tag.name)),
  );
  for (const tag of tags) {
    const key = normalizedMetadataName(tag.name);
    if (!existing.has(key)) {
      state.tagOptions.push({
        id: Number(tag.id),
        name: String(tag.name),
      });
      existing.add(key);
    }
  }
}

function normalizedMetadataName(name) {
  return String(name || "").trim().toLowerCase();
}

function normalizeRating(value) {
  const rating = Number(value);
  return Number.isInteger(rating) && rating >= 1 && rating <= 5 ? rating : null;
}

function isFilterMode(mode = state.metadataMode) {
  return mode === APP_MODES.SEARCH || mode === APP_MODES.PERSONS;
}

function iconSvg(name) {
  const icons = {
    folder: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M3.5 6.5a2 2 0 0 1 2-2h4.2l2 2H18.5a2 2 0 0 1 2 2v8.8a2 2 0 0 1-2 2h-13a2 2 0 0 1-2-2Z"></path></svg>`,
    folderTree: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M6 5.5v12.8"></path><path d="M6 9.3h3.1"></path><path d="M6 16.2h3.1"></path><path d="M9.2 7.2h3l1 1h3.6a1.2 1.2 0 0 1 1.2 1.2v2.3a1.2 1.2 0 0 1-1.2 1.2H9.2Z"></path><path d="M9.2 14.1h3l1 1h3.6a1.2 1.2 0 0 1 1.2 1.2v2.3a1.2 1.2 0 0 1-1.2 1.2H9.2Z"></path></svg>`,
    persons: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M9.2 11.2a3.2 3.2 0 1 0 0-6.4 3.2 3.2 0 0 0 0 6.4Z"></path><path d="M15.9 11.1a2.7 2.7 0 1 0 0-5.4 2.7 2.7 0 0 0 0 5.4Z"></path><path d="M3.5 19.2c.5-3.4 2.4-5.1 5.7-5.1s5.2 1.7 5.7 5.1Z"></path><path d="M14.2 18.9c.5-2.6 2-4 4.4-4 1 0 1.8.2 2.5.7v3.3Z"></path></svg>`,
    search: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="10.7" cy="10.7" r="5.9"></circle><path d="m15.1 15.1 5.4 5.4"></path></svg>`,
    play: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M8 5.2v13.6L18.7 12Z"></path></svg>`,
    reset: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M5.5 8.5A7.3 7.3 0 0 1 18 6.9"></path><path d="M18 4.2v3.4h-3.4"></path><path d="M18.5 15.5A7.3 7.3 0 0 1 6 17.1"></path><path d="M6 19.8v-3.4h3.4"></path></svg>`,
    refresh: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M20 11a8 8 0 0 0-14.2-4.9"></path><path d="M5 3.8v4h4"></path><path d="M4 13a8 8 0 0 0 14.2 4.9"></path><path d="M19 20.2v-4h-4"></path></svg>`,
    moreHorizontal: `<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="6" cy="12" r="1.4"></circle><circle cx="12" cy="12" r="1.4"></circle><circle cx="18" cy="12" r="1.4"></circle></svg>`,
  };
  return icons[name] ?? "";
}

function renderMetadataModeTabs() {
  const mode = state.metadataMode;
  return `
    <div class="metadata-mode-tabs" role="tablist" aria-label="App state">
      <button type="button" data-metadata-mode="${APP_MODES.FOLDER}" data-active="${mode === APP_MODES.FOLDER}" title="Folder" aria-label="Folder">
        <span class="mode-icon">${iconSvg("folder")}</span><span>Folder</span>
      </button>
      <button type="button" data-metadata-mode="${APP_MODES.PERSONS}" data-active="${mode === APP_MODES.PERSONS}" title="Persons" aria-label="Persons">
        <span class="mode-icon">${iconSvg("persons")}</span><span>Persons</span>
      </button>
      <button type="button" data-metadata-mode="${APP_MODES.SEARCH}" data-active="${mode === APP_MODES.SEARCH}" title="Search" aria-label="Search">
        <span class="mode-icon">${iconSvg("search")}</span><span>Search</span>
      </button>
    </div>
  `;
}

async function handleMetadataBarClick(event) {
  const modeButton = event.target.closest("button[data-metadata-mode]");
  if (modeButton && !modeButton.disabled) {
    await switchMetadataMode(modeButton.dataset.metadataMode);
    return;
  }

  const ratingButton = event.target.closest("button[data-rating]");
  if (ratingButton && !ratingButton.disabled) {
    if (isFilterMode()) {
      await setSearchMinimumRating(Number(ratingButton.dataset.rating));
    } else {
      await setCurrentFolderRating(Number(ratingButton.dataset.rating));
    }
    return;
  }

  const actionButton = event.target.closest("button[data-action]");
  if (actionButton && !actionButton.disabled) {
    if (isFilterMode()) {
      await handleSearchMetadataAction(actionButton);
      return;
    }

    const action = actionButton.dataset.action;
    if (action === "rescan-folder") {
      await scanCurrentRoot();
      return;
    }
    if (action === "toggle-person-dropdown") {
      if (state.personDropdownOpen) {
        closePersonDropdown();
      } else {
        await openPersonDropdown();
      }
      return;
    }
    if (action === "toggle-tag-dropdown") {
      if (state.tagDropdownOpen) {
        closeTagDropdown();
      } else {
        await openTagDropdown();
      }
      return;
    }
    if (action === "toggle-metadata-item-menu") {
      toggleMetadataItemMenu(
        actionButton.dataset.metadataKind,
        actionButton.dataset.metadataName,
        actionButton,
      );
      return;
    }
    if (action === "edit-metadata-item") {
      await editMetadataItem(
        actionButton.dataset.metadataKind,
        actionButton.dataset.metadataName,
      );
      return;
    }
    if (action === "delete-metadata-item") {
      await deleteMetadataItem(
        actionButton.dataset.metadataKind,
        actionButton.dataset.metadataName,
      );
      return;
    }
    if (action === "remove-person") {
      await removeCurrentFolderPerson(Number(actionButton.dataset.personId));
      return;
    }
    if (action === "remove-tag") {
      await removeCurrentFolderTag(Number(actionButton.dataset.tagId));
      return;
    }
  }

  if (isFilterMode()) {
    const searchPersonOption = event.target.closest(".search-person-option");
    if (searchPersonOption && state.metadataMode === APP_MODES.SEARCH) {
      await setSearchPerson(searchPersonOption.dataset.personName);
      return;
    }

    const searchTagOption = event.target.closest(".search-tag-option");
    if (searchTagOption) {
      await addSearchTag(state.searchTagDropdownTarget, searchTagOption.dataset.tagName);
      return;
    }
  }

  const personOption = event.target.closest(".person-option");
  if (personOption) {
    await addCurrentFolderPerson(personOption.dataset.personName);
  }

  const tagOption = event.target.closest(".tag-option");
  if (tagOption) {
    await addCurrentFolderTag(tagOption.dataset.tagName);
  }
}

function handleMetadataBarInput(event) {
  if (event.target.classList.contains("search-person-search-field")) {
    state.searchPersonSearch = event.target.value;
    updateSearchPersonDropdownOptions();
  } else if (event.target.classList.contains("search-tag-search-field")) {
    state.searchTagSearch = event.target.value;
    updateSearchTagDropdownOptions();
  } else if (event.target.classList.contains("person-search-field")) {
    state.personSearch = event.target.value;
    updatePersonDropdownOptions();
  } else if (event.target.classList.contains("tag-search-field")) {
    state.tagSearch = event.target.value;
    updateTagDropdownOptions();
  }
}

async function handleMetadataBarKeydown(event) {
  const isSearchPersonSearch = event.target.classList.contains(
    "search-person-search-field",
  );
  const isSearchTagSearch = event.target.classList.contains("search-tag-search-field");
  if (isSearchPersonSearch || isSearchTagSearch) {
    if (event.key === "Escape") {
      event.preventDefault();
      if (isSearchPersonSearch) {
        closeSearchPersonDropdown();
      } else {
        closeSearchTagDropdown();
      }
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      if (isSearchPersonSearch) {
        await setSearchPerson(event.target.value);
      } else {
        await addSearchTag(state.searchTagDropdownTarget, event.target.value);
      }
    }
    return;
  }

  const isPersonSearch = event.target.classList.contains("person-search-field");
  const isTagSearch = event.target.classList.contains("tag-search-field");
  if (!isPersonSearch && !isTagSearch) {
    return;
  }

  if (event.key === "Escape") {
    event.preventDefault();
    if (isPersonSearch) {
      closePersonDropdown();
    } else {
      closeTagDropdown();
    }
    return;
  }

  if (event.key === "Enter") {
    event.preventDefault();
    if (isPersonSearch) {
      await addCurrentFolderPerson(event.target.value);
    } else {
      await addCurrentFolderTag(event.target.value);
    }
  }
}

async function switchMetadataMode(mode) {
  if (mode === state.metadataMode) {
    return;
  }

  const historyEntry = beginHistoryNavigation();
  if (mode === APP_MODES.SEARCH) {
    state.metadataMode = APP_MODES.SEARCH;
    closeMetadataDropdowns();
    state.searchSlideshowMenuOpen = false;
    renderMetadataBar();
    await refreshSearchSurface();
    commitHistoryNavigation(historyEntry);
    return;
  }

  if (mode === APP_MODES.PERSONS) {
    state.metadataMode = APP_MODES.PERSONS;
    state.searchPerson = null;
    state.searchPersonDropdownOpen = false;
    state.searchPersonSearch = "";
    state.searchSlideshowMenuOpen = false;
    closeMetadataDropdowns();
    renderMetadataBar();
    await refreshSearchSurface();
    commitHistoryNavigation(historyEntry);
    return;
  }

  state.metadataMode = APP_MODES.FOLDER;
  closeSearchDropdowns();
  state.searchActive = false;
  state.searchLoading = false;
  state.searchRequestId += 1;
  renderCurrentLibrarySurface({ keepStatus: true });
  restoreCurrentLibraryScrollPosition();
  commitHistoryNavigation(historyEntry);
}

async function handleSearchMetadataAction(button) {
  const action = button.dataset.action;
  if (action === "toggle-search-person-dropdown") {
    if (state.metadataMode !== APP_MODES.SEARCH) {
      return;
    }
    if (state.searchPersonDropdownOpen) {
      closeSearchPersonDropdown();
    } else {
      await openSearchPersonDropdown();
    }
    return;
  }

  if (action === "toggle-search-slideshow-menu") {
    state.searchSlideshowMenuOpen = !state.searchSlideshowMenuOpen;
    state.searchPersonDropdownOpen = false;
    state.searchPersonSearch = "";
    state.searchTagDropdownTarget = null;
    state.searchTagSearch = "";
    renderMetadataBar();
    return;
  }

  if (action === "toggle-search-hierarchy") {
    const historyEntry = beginHistoryNavigation();
    state.searchHierarchy = !state.searchHierarchy;
    renderMetadataBar();
    if (state.searchActive && state.metadataMode === APP_MODES.SEARCH) {
      renderSearchResults();
    }
    commitHistoryNavigation(historyEntry);
    return;
  }

  if (action === "clear-search-person") {
    await setSearchPerson(null);
    return;
  }

  if (action === "reset-search-filters") {
    await resetSearchFilters();
    return;
  }

  if (action === "play-search-slideshow") {
    closeSearchSlideshowMenu();
    await playSearchResultsSlideshow({ randomized: false });
    return;
  }

  if (action === "play-search-slideshow-random") {
    closeSearchSlideshowMenu();
    await playSearchResultsSlideshow({ randomized: true });
    return;
  }

  if (action === "toggle-search-tag-dropdown") {
    const target = normalizeSearchTagListName(button.dataset.tagList);
    if (state.searchTagDropdownTarget === target) {
      closeSearchTagDropdown();
    } else {
      await openSearchTagDropdown(target);
    }
    return;
  }

  if (action === "remove-search-tag") {
    await removeSearchTag(button.dataset.tagList, button.dataset.tagName);
    return;
  }

  if (action === "set-search-tag-combine") {
    await setSearchTagCombine(button.dataset.tagList, button.dataset.combine);
  }
}

async function setSearchPerson(name, options = {}) {
  const historyEntry = beginHistoryNavigation();
  const cleanName = String(name || "").trim();
  state.searchPerson = cleanName ? { name: cleanName } : null;
  if (options.switchToSearch) {
    state.metadataMode = APP_MODES.SEARCH;
  }
  state.searchPersonSearch = "";
  state.searchPersonDropdownOpen = false;
  state.searchSlideshowMenuOpen = false;
  renderMetadataBar();
  await refreshSearchSurface();
  commitHistoryNavigation(historyEntry);
}

async function resetSearchFilters() {
  const historyEntry = beginHistoryNavigation();
  state.searchPerson = null;
  state.searchIncludeTags = [];
  state.searchIncludeCombine = "and";
  state.searchExcludeTags = [];
  state.searchExcludeCombine = "or";
  state.searchMinimumRating = null;
  state.searchHierarchy = false;
  state.searchPersonDropdownOpen = false;
  state.searchPersonSearch = "";
  state.searchTagDropdownTarget = null;
  state.searchTagSearch = "";
  state.searchSlideshowMenuOpen = false;
  state.searchResults = [];
  state.searchDisplayedFolders = [];
  state.searchPersonResults = [];
  state.searchPeopleLoaded = false;
  state.searchPeopleFilterKey = "";
  state.searchLoading = false;
  state.searchRequestId += 1;
  renderMetadataBar();
  if (state.metadataMode === APP_MODES.PERSONS) {
    await refreshSearchSurface();
    commitHistoryNavigation(historyEntry);
    return;
  }

  prepareSearchSurface();
  updateBusyIndicator();
  setStatus("Choose search filters");
  renderEmptyState("Choose search filters", { keepBreadcrumbs: true });
  commitHistoryNavigation(historyEntry);
}

async function addSearchTag(listName, name) {
  const cleanName = String(name || "").trim();
  if (!cleanName) {
    return;
  }

  const historyEntry = beginHistoryNavigation();
  const list = searchTagList(listName);
  const key = normalizedMetadataName(cleanName);
  if (!list.some((tag) => normalizedMetadataName(tag.name) === key)) {
    list.push({ id: Date.now(), name: cleanName });
  }

  state.searchTagSearch = "";
  state.searchPeopleLoaded = false;
  closeSearchTagDropdown();
  renderMetadataBar();
  await refreshSearchSurface();
  commitHistoryNavigation(historyEntry);
}

async function removeSearchTag(listName, name) {
  const historyEntry = beginHistoryNavigation();
  const normalizedName = normalizedMetadataName(name);
  const target = normalizeSearchTagListName(listName);
  if (target === "include") {
    state.searchIncludeTags = state.searchIncludeTags.filter(
      (tag) => normalizedMetadataName(tag.name) !== normalizedName,
    );
  } else {
    state.searchExcludeTags = state.searchExcludeTags.filter(
      (tag) => normalizedMetadataName(tag.name) !== normalizedName,
    );
  }
  state.searchPeopleLoaded = false;
  renderMetadataBar();
  await refreshSearchSurface();
  commitHistoryNavigation(historyEntry);
}

async function setSearchTagCombine(listName, combine) {
  const historyEntry = beginHistoryNavigation();
  const target = normalizeSearchTagListName(listName);
  const mode = normalizeCombineMode(combine);
  if (target === "include") {
    state.searchIncludeCombine = mode;
  } else {
    state.searchExcludeCombine = mode;
  }
  state.searchPeopleLoaded = false;
  renderMetadataBar();
  await refreshSearchSurface();
  commitHistoryNavigation(historyEntry);
}

async function setSearchMinimumRating(rating) {
  const normalized = normalizeRating(rating);
  if (!normalized) {
    return;
  }

  const historyEntry = beginHistoryNavigation();
  state.searchMinimumRating =
    normalizeRating(state.searchMinimumRating) === normalized ? null : normalized;
  state.searchPeopleLoaded = false;
  renderMetadataBar();
  await refreshSearchSurface();
  commitHistoryNavigation(historyEntry);
}

function searchTagList(listName) {
  return normalizeSearchTagListName(listName) === "include"
    ? state.searchIncludeTags
    : state.searchExcludeTags;
}

function normalizeSearchTagListName(listName) {
  return listName === "exclude" ? "exclude" : "include";
}

function normalizeCombineMode(value) {
  return value === "or" ? "or" : "and";
}

async function setCurrentFolderRating(rating) {
  const target = currentFolderMetadataTarget();
  rating = normalizeRating(rating);
  if (!invoke || !target || !rating) {
    return;
  }

  const currentRating = normalizeRating(state.currentFolderMeta?.rating);
  const nextRating = currentRating === rating ? null : rating;
  const metadata = await invoke("set_folder_rating", {
    rootId: target.rootId,
    folderId: target.folderId,
    rating: nextRating,
  });
  applyCurrentFolderMetadata(metadata, target);
  invalidateFolderViewCache(target.rootId);
  invalidateSearchCaches();
  await patchCurrentFolderFromDb({ keepStatus: true });
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
  invalidateSearchCaches();
  await patchCurrentFolderFromDb({ keepStatus: true });
}

async function addCurrentFolderTag(name) {
  const target = currentFolderMetadataTarget();
  const cleanName = String(name || "").trim();
  if (!invoke || !target || !cleanName) {
    return;
  }

  const metadata = await invoke("add_folder_tag", {
    rootId: target.rootId,
    folderId: target.folderId,
    name: cleanName,
  });
  applyCurrentFolderMetadata(metadata, target);
  state.tagSearch = "";
  mergeTagOptions(state.currentFolderMeta?.tags ?? []);
  closeTagDropdown();
  invalidateFolderViewCache(target.rootId);
  invalidateSearchCaches();
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
  invalidateSearchCaches();
  await patchCurrentFolderFromDb({ keepStatus: true });
}

async function removeCurrentFolderTag(tagId) {
  const target = currentFolderMetadataTarget();
  if (!invoke || !target || !Number.isFinite(tagId)) {
    return;
  }

  const metadata = await invoke("remove_folder_tag", {
    rootId: target.rootId,
    folderId: target.folderId,
    tagId,
  });
  applyCurrentFolderMetadata(metadata, target);
  invalidateFolderViewCache(target.rootId);
  invalidateSearchCaches();
  await patchCurrentFolderFromDb({ keepStatus: true });
}

function toggleMetadataItemMenu(kind, name, anchor) {
  kind = normalizeMetadataKind(kind);
  const cleanName = String(name || "").trim();
  if (!kind || !cleanName) {
    return;
  }

  if (metadataItemMenuMatches(kind, cleanName)) {
    clearMetadataItemMenu();
    return;
  }

  state.metadataItemMenu = { kind, name: cleanName };
  showMetadataItemMenuOverlay(kind, cleanName, anchor);
}

function showMetadataItemMenuOverlay(kind, name, anchor) {
  removeMetadataItemMenuOverlay();
  const menu = renderMetadataItemMenu(kind, name);
  metadataBar.append(menu);

  const anchorRect = anchor.getBoundingClientRect();
  const menuRect = menu.getBoundingClientRect();
  const gap = 4;
  const margin = 8;
  const left = Math.min(
    window.innerWidth - menuRect.width - margin,
    Math.max(margin, anchorRect.right - menuRect.width),
  );
  let top = anchorRect.bottom + gap;
  if (top + menuRect.height + margin > window.innerHeight) {
    top = anchorRect.top - menuRect.height - gap;
  }

  menu.style.left = `${Math.max(margin, left)}px`;
  menu.style.top = `${Math.max(margin, top)}px`;
}

function clearMetadataItemMenu() {
  state.metadataItemMenu = null;
  removeMetadataItemMenuOverlay();
}

function removeMetadataItemMenuOverlay() {
  metadataBar.querySelector(".metadata-item-menu")?.remove();
}

async function editMetadataItem(kind, name) {
  kind = normalizeMetadataKind(kind);
  const oldName = String(name || "").trim();
  if (!invoke || !kind || !oldName) {
    return;
  }

  const newName = await promptMetadataEdit(kind, oldName);
  const cleanName = String(newName || "").trim();
  if (!cleanName || cleanName === oldName) {
    clearMetadataItemMenu();
    return;
  }

  const label = metadataKindLabel(kind).toLowerCase();
  setStatus(`Updating ${label}...`);
  await invoke(kind === "person" ? "rename_metadata_person" : "rename_metadata_tag", {
    oldName,
    newName: cleanName,
  });
  renameMetadataReferencesInState(kind, oldName, cleanName);
  await refreshAfterMetadataCatalogChange(kind);
  setStatus(`${metadataKindLabel(kind)} updated`);
}

async function deleteMetadataItem(kind, name) {
  kind = normalizeMetadataKind(kind);
  const cleanName = String(name || "").trim();
  if (!invoke || !kind || !cleanName) {
    return;
  }

  const label = metadataKindLabel(kind).toLowerCase();
  const confirmed = await confirmWarning(
    `Delete ${label} "${cleanName}"?`,
    `This removes the ${label} and all references from every connected root database.`,
  );
  if (!confirmed) {
    clearMetadataItemMenu();
    return;
  }

  setStatus(`Deleting ${label}...`);
  await invoke(kind === "person" ? "delete_metadata_person" : "delete_metadata_tag", {
    name: cleanName,
  });
  deleteMetadataReferencesInState(kind, cleanName);
  await refreshAfterMetadataCatalogChange(kind);
  setStatus(`${metadataKindLabel(kind)} deleted`);
}

async function refreshAfterMetadataCatalogChange(kind) {
  clearMetadataItemMenu();
  state.personDropdownOpen = false;
  state.personSearch = "";
  state.tagDropdownOpen = false;
  state.tagSearch = "";
  if (kind === "person") {
    state.peopleOptions = [];
    state.peopleOptionsLoaded = false;
  } else {
    state.tagOptions = [];
    state.tagOptionsLoaded = false;
  }
  invalidateFolderViewCache();
  invalidateSearchCaches();
  if (state.currentRootId && !state.atRootOverview) {
    await patchCurrentFolderFromDb({ keepStatus: true });
  } else {
    renderMetadataBar();
  }
}

function renameMetadataReferencesInState(kind, oldName, newName) {
  if (kind === "person") {
    if (metadataNamesMatch(state.searchPerson?.name, oldName)) {
      state.searchPerson = { name: newName };
    }
    return;
  }

  state.searchIncludeTags = renameSearchTags(state.searchIncludeTags, oldName, newName);
  state.searchExcludeTags = renameSearchTags(state.searchExcludeTags, oldName, newName);
}

function deleteMetadataReferencesInState(kind, name) {
  if (kind === "person") {
    if (metadataNamesMatch(state.searchPerson?.name, name)) {
      state.searchPerson = null;
    }
    return;
  }

  state.searchIncludeTags = state.searchIncludeTags.filter(
    (tag) => !metadataNamesMatch(tag.name, name),
  );
  state.searchExcludeTags = state.searchExcludeTags.filter(
    (tag) => !metadataNamesMatch(tag.name, name),
  );
}

function renameSearchTags(tags, oldName, newName) {
  const seen = new Set();
  const renamed = [];
  for (const tag of tags) {
    const name = metadataNamesMatch(tag.name, oldName) ? newName : tag.name;
    const key = normalizedMetadataName(name);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    renamed.push({ ...tag, name });
  }
  return renamed;
}

function metadataNamesMatch(left, right) {
  return normalizedMetadataName(left) === normalizedMetadataName(right);
}

function normalizeMetadataKind(kind) {
  if (kind === "person" || kind === "tag") {
    return kind;
  }
  return null;
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
  mergeTagOptions(state.currentFolderMeta.tags);
  renderMetadataBar();
}

function invalidateSearchCaches() {
  state.searchResults = [];
  state.searchDisplayedFolders = [];
  state.searchPersonResults = [];
  state.searchPeopleLoaded = false;
  state.searchPeopleFilterKey = "";
  updateNavigationButtons();
}

function switchSearchNavigationToEditMode() {
  if (!isFilterMode()) {
    return;
  }

  state.metadataMode = APP_MODES.FOLDER;
  closeSearchDropdowns();
}

function cloneSearchTags(tags) {
  return tags.map((tag) => ({
    id: Number(tag.id ?? Date.now()),
    name: String(tag.name),
  }));
}

function cloneSearchHistoryState() {
  return {
    person: state.searchPerson ? { name: state.searchPerson.name } : null,
    includeTags: cloneSearchTags(state.searchIncludeTags),
    includeCombine: normalizeCombineMode(state.searchIncludeCombine),
    excludeTags: cloneSearchTags(state.searchExcludeTags),
    excludeCombine: normalizeCombineMode(state.searchExcludeCombine),
    minimumRating: normalizeRating(state.searchMinimumRating),
    hierarchy: Boolean(state.searchHierarchy),
  };
}

function applySearchHistoryState(search) {
  state.searchPerson = search?.person?.name ? { name: String(search.person.name) } : null;
  state.searchIncludeTags = cloneSearchTags(search?.includeTags ?? []);
  state.searchIncludeCombine = normalizeCombineMode(search?.includeCombine);
  state.searchExcludeTags = cloneSearchTags(search?.excludeTags ?? []);
  state.searchExcludeCombine = normalizeCombineMode(search?.excludeCombine);
  state.searchMinimumRating = normalizeRating(search?.minimumRating);
  state.searchHierarchy = Boolean(search?.hierarchy);
  state.searchPersonDropdownOpen = false;
  state.searchPersonSearch = "";
  state.searchTagDropdownTarget = null;
  state.searchTagSearch = "";
  state.searchSlideshowMenuOpen = false;
}

function currentHistoryEntry() {
  return {
    mode: state.metadataMode,
    atRootOverview: Boolean(state.atRootOverview),
    currentRootId: state.currentRootId,
    currentPath: state.currentPath ?? "",
    search: cloneSearchHistoryState(),
    scroll: {
      left: gridNode.scrollLeft,
      top: gridNode.scrollTop,
    },
  };
}

function historyEntrySignature(entry) {
  if (!entry) {
    return "";
  }

  if (entry.mode === APP_MODES.FOLDER) {
    return JSON.stringify({
      mode: entry.mode,
      atRootOverview: Boolean(entry.atRootOverview),
      currentRootId: entry.currentRootId ?? null,
      currentPath: entry.currentPath ?? "",
    });
  }

  const search = entry.search ?? {};
  return JSON.stringify({
    mode: entry.mode,
    person: search.person?.name ?? null,
    includeTags: (search.includeTags ?? []).map((tag) => tag.name),
    includeCombine: normalizeCombineMode(search.includeCombine),
    excludeTags: (search.excludeTags ?? []).map((tag) => tag.name),
    excludeCombine: normalizeCombineMode(search.excludeCombine),
    minimumRating: normalizeRating(search.minimumRating),
    hierarchy: entry.mode === APP_MODES.SEARCH ? Boolean(search.hierarchy) : false,
  });
}

function beginHistoryNavigation() {
  return state.restoringHistory ? null : currentHistoryEntry();
}

function commitHistoryNavigation(previousEntry) {
  if (state.restoringHistory || !previousEntry) {
    updateNavigationButtons();
    return;
  }

  const currentEntry = currentHistoryEntry();
  if (historyEntrySignature(previousEntry) === historyEntrySignature(currentEntry)) {
    updateNavigationButtons();
    return;
  }

  const lastEntry = state.historyBack[state.historyBack.length - 1];
  if (historyEntrySignature(lastEntry) !== historyEntrySignature(previousEntry)) {
    state.historyBack.push(previousEntry);
    if (state.historyBack.length > 100) {
      state.historyBack.shift();
    }
  }
  state.historyForward = [];
  updateNavigationButtons();
}

function canGoBackHistory() {
  return state.historyBack.length > 0;
}

function canGoForwardHistory() {
  return state.historyForward.length > 0;
}

function updateNavigationButtons() {
  if (backButton) {
    backButton.disabled = !canGoBackHistory();
  }
  if (forwardButton) {
    forwardButton.disabled = !canGoForwardHistory();
  }
}

function rescanButtonDisabled() {
  return (
    state.metadataMode !== APP_MODES.FOLDER ||
    !state.currentRootId ||
    state.activeScans.has(state.currentRootId)
  );
}

function updateRescanButton() {
  const button = metadataBar.querySelector("button[data-action='rescan-folder']");
  if (button) {
    button.disabled = rescanButtonDisabled();
  }
}

async function goBackHistory() {
  if (!canGoBackHistory()) {
    return;
  }

  const currentEntry = currentHistoryEntry();
  const targetEntry = state.historyBack.pop();
  state.historyForward.push(currentEntry);
  await restoreHistoryEntry(targetEntry);
}

async function goForwardHistory() {
  if (!canGoForwardHistory()) {
    return;
  }

  const currentEntry = currentHistoryEntry();
  const targetEntry = state.historyForward.pop();
  state.historyBack.push(currentEntry);
  await restoreHistoryEntry(targetEntry);
}

async function restoreHistoryEntry(entry) {
  if (!entry) {
    updateNavigationButtons();
    return;
  }

  state.restoringHistory = true;
  try {
    closeMetadataDropdowns();
    closeSearchDropdowns();
    applySearchHistoryState(entry.search);

    if (entry.mode === APP_MODES.SEARCH || entry.mode === APP_MODES.PERSONS) {
      state.metadataMode = entry.mode;
      state.searchActive = false;
      state.searchLoading = false;
      state.searchRequestId += 1;
      renderMetadataBar();
      await refreshSearchSurface();
      restoreSearchScroll(entry.scroll);
      return;
    }

    state.metadataMode = APP_MODES.FOLDER;
    state.searchActive = false;
    state.searchLoading = false;
    state.searchRequestId += 1;
    if (entry.atRootOverview || !entry.currentRootId) {
      state.viewScrollPositions.set(viewKey(null, ""), entry.scroll ?? { left: 0, top: 0 });
      openRootOverview({ keepStatus: true });
    } else {
      state.viewScrollPositions.set(
        viewKey(entry.currentRootId, entry.currentPath ?? ""),
        entry.scroll ?? { left: 0, top: 0 },
      );
      await openFolder(entry.currentRootId, entry.currentPath ?? "", { keepStatus: true });
    }
  } finally {
    state.restoringHistory = false;
    updateNavigationButtons();
  }
}

function renderCurrentLibrarySurface(options = {}) {
  resetThumbnailWork();
  updateBusyIndicator();
  if (state.atRootOverview) {
    renderRootOverview(options);
    return;
  }

  if (!state.currentView) {
    openRootOverview(options);
    return;
  }

  if (state.currentView.folder_id === null) {
    refreshCurrentFolder({ keepStatus: true, forceReload: true }).catch(showError);
    return;
  }

  if (state.folderLoading) {
    renderPendingFolderView(state.currentView, options);
  } else {
    renderFolderView(state.currentView, options);
  }
}

async function refreshSearchSurface() {
  if (!isFilterMode()) {
    return;
  }

  prepareSearchSurface();
  if (state.metadataMode === APP_MODES.PERSONS) {
    state.searchPerson = null;
    await loadSearchPeopleSurface();
    return;
  }

  if (!searchHasFilters()) {
    state.searchRequestId += 1;
    state.searchLoading = false;
    state.searchResults = [];
    state.searchDisplayedFolders = [];
    updateBusyIndicator();
    setStatus("Choose search filters");
    renderEmptyState("Choose search filters", { keepBreadcrumbs: true });
    return;
  }

  await runMetadataSearch();
}

function prepareSearchSurface() {
  if (!state.searchActive) {
    rememberCurrentScrollPosition();
  }

  state.searchActive = true;
  state.activeFolderRequestId = null;
  state.folderLoading = false;
  state.visibleValidationActive = false;
  state.validatedVisibleKeys.clear();
  clearValidationPatchTimer();
  clearVisibleValidationTimer();
  resetStreamRenderQueue();
  resetThumbnailWork();
  updateBusyIndicator();
  titleNode.textContent =
    state.metadataMode === APP_MODES.PERSONS ? "Persons" : "Search";
  updateNavigationButtons();
  breadcrumbsNode.replaceChildren();
}

function searchHasFilters() {
  return Boolean(
    state.searchPerson ||
      state.searchIncludeTags.length > 0 ||
      state.searchExcludeTags.length > 0 ||
      normalizeRating(state.searchMinimumRating),
  );
}

async function runMetadataSearch() {
  if (!invoke) {
    state.searchResults = [];
    renderSearchResults();
    return;
  }

  const requestId = ++state.searchRequestId;
  state.searchLoading = true;
  updateBusyIndicator();
  setStatus("Searching metadata...");
  renderEmptyState("Searching...", { keepBreadcrumbs: true });

  try {
    const results = await invoke("metadata_search", { query: searchQueryPayload() });
    if (requestId !== state.searchRequestId || state.metadataMode !== APP_MODES.SEARCH) {
      return;
    }
    state.searchResults = normalizeSearchFolders(results);
    renderSearchResults();
  } finally {
    if (requestId === state.searchRequestId) {
      state.searchLoading = false;
      updateBusyIndicator();
    }
  }
}

function searchQueryPayload(options = {}) {
  const personName =
    Object.prototype.hasOwnProperty.call(options, "person")
      ? options.person
      : state.searchPerson?.name ?? null;
  return {
    person: personName,
    include_tags: {
      names: state.searchIncludeTags.map((tag) => tag.name),
      combine: normalizeCombineMode(state.searchIncludeCombine),
    },
    exclude_tags: {
      names: state.searchExcludeTags.map((tag) => tag.name),
      combine: normalizeCombineMode(state.searchExcludeCombine),
    },
    minimum_rating: normalizeRating(state.searchMinimumRating),
  };
}

function personsQueryPayload() {
  return searchQueryPayload({ person: null });
}

function personsFilterKey() {
  return JSON.stringify(personsQueryPayload());
}

function normalizeSearchFolders(folders) {
  return Array.isArray(folders)
    ? folders
        .filter((folder) => folder?.root_id && Number.isFinite(Number(folder?.id)))
        .map((folder) => ({
          root_id: String(folder.root_id),
          id: Number(folder.id),
          relative_path: String(folder.relative_path ?? ""),
          name: String(folder.name || folder.relative_path || rootDisplayName(folder.root_id)),
          parent_relative_path:
            folder.parent_relative_path === null ||
            folder.parent_relative_path === undefined
              ? null
              : String(folder.parent_relative_path),
          thumbnail_image_id: Number.isFinite(Number(folder.thumbnail_image_id))
            ? Number(folder.thumbnail_image_id)
            : null,
          direct_keywords: Array.isArray(folder.direct_keywords)
            ? folder.direct_keywords.map(String)
            : [],
          inherited_keywords: Array.isArray(folder.inherited_keywords)
            ? folder.inherited_keywords.map(String)
            : [],
          direct_people: Array.isArray(folder.direct_people)
            ? folder.direct_people.map(String)
            : [],
          inherited_people: Array.isArray(folder.inherited_people)
            ? folder.inherited_people.map(String)
            : [],
          direct_rating: normalizeRating(folder.direct_rating),
          inherited_rating: normalizeRating(folder.inherited_rating),
          image_count: Number(folder.image_count ?? 0),
          child_folder_count: Number(folder.child_folder_count ?? 0),
        }))
    : [];
}

function renderSearchResults(options = {}) {
  prepareSearchSurface();
  const folders = searchDisplayFolders();
  state.searchDisplayedFolders = folders;
  const total = state.searchResults.length;
  renderMetadataBar();

  if (folders.length === 0) {
    setStatus(total === 0 ? "No matching folders" : "No final folders in results");
    renderEmptyState("No matching folders", { keepBreadcrumbs: true });
    restoreSearchScroll(options.restoreScroll);
    return;
  }

  const label = state.searchHierarchy ? "hierarchy results" : "final folders";
  setStatus(`${folders.length} ${label} from ${total} matching folders`);
  gridNode.replaceChildren(...folders.map(renderSearchFolderCard));
  restoreSearchScroll(options.restoreScroll);
}

function restoreSearchScroll(scroll) {
  restoreGridScroll(scroll);
}

function restoreCurrentLibraryScrollPosition() {
  const key = state.atRootOverview
    ? viewKey(null, "")
    : state.currentRootId
      ? viewKey(state.currentRootId, state.currentPath)
      : null;
  if (!key) {
    return;
  }

  restoreGridScroll(state.viewScrollPositions.get(key));
}

function restoreGridScroll(scroll) {
  if (!scroll) {
    return;
  }

  const left = Number(scroll.left) || 0;
  const top = Number(scroll.top) || 0;
  gridNode.scrollLeft = left;
  gridNode.scrollTop = top;
  requestAnimationFrame(() => {
    gridNode.scrollLeft = left;
    gridNode.scrollTop = top;
  });
}

function searchDisplayFolders() {
  const results = sortedSearchFolders(state.searchResults);
  if (state.searchHierarchy) {
    return topmostSearchFolders(results);
  }

  const finalFolders = results.filter((folder) => Number(folder.image_count) > 0);
  return finalFolders.length > 0 ? finalFolders : results;
}

function sortedSearchFolders(folders) {
  return [...folders].sort((left, right) =>
    rootDisplayName(left.root_id)
      .toLowerCase()
      .localeCompare(rootDisplayName(right.root_id).toLowerCase())
      || left.relative_path
        .toLowerCase()
        .localeCompare(right.relative_path.toLowerCase())
      || left.id - right.id,
  );
}

function topmostSearchFolders(folders) {
  const displayed = [];
  for (const folder of folders) {
    const covered = displayed.some(
      (candidate) =>
        candidate.root_id === folder.root_id &&
        pathContainsPath(candidate.relative_path, folder.relative_path),
    );
    if (!covered) {
      displayed.push(folder);
    }
  }
  return displayed;
}

function renderSearchFolderCard(folder) {
  const card = document.createElement("article");
  card.className = "tile folder-tile search-result-tile";
  card.tabIndex = 0;
  card.title = fullFolderPath(folder);
  card.dataset.rootId = folder.root_id;
  card.dataset.folderPath = folder.relative_path;
  card.dataset.itemKey = `search:${folder.root_id}:${folder.relative_path}`;
  card.dataset.summarySignature = folderSummarySignature(folder);
  card.innerHTML = `
    <div class="thumb folder-thumb">
      <span>${escapeHtml(initials(folder.name))}</span>
      ${renderFolderRatingBadge(folder)}
    </div>
    <div class="tile-body">
      <h3>${escapeHtml(folder.name || rootDisplayName(folder.root_id))}</h3>
      <p>${escapeHtml(searchFolderSubtitle(folder))}</p>
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

  card.addEventListener("click", () => openFolderFromSearchResult(folder));
  card.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      openFolderFromSearchResult(folder);
    }
  });
  return card;
}

function openFolderFromSearchResult(folder) {
  openFolder(folder.root_id, folder.relative_path).catch(showError);
}

function searchFolderSubtitle(folder) {
  const path = folder.relative_path ? ` / ${folder.relative_path}` : "";
  return `${rootDisplayName(folder.root_id)}${path} - ${folder.image_count} images - ${folder.child_folder_count} folders`;
}

async function loadSearchPeopleSurface() {
  prepareSearchSurface();
  if (!invoke) {
    state.searchPersonResults = [];
    renderSearchPeople();
    return;
  }

  const filterKey = personsFilterKey();
  if (state.searchPeopleLoaded && state.searchPeopleFilterKey === filterKey) {
    renderSearchPeople();
    return;
  }

  const requestId = ++state.searchRequestId;
  state.searchLoading = true;
  updateBusyIndicator();
  setStatus("Loading persons...");
  renderEmptyState("Loading persons...", { keepBreadcrumbs: true });

  try {
    const people = await invoke("metadata_filtered_person_thumbnails", {
      query: personsQueryPayload(),
    });
    if (requestId !== state.searchRequestId || state.metadataMode !== APP_MODES.PERSONS) {
      return;
    }
    state.searchPersonResults = normalizeSearchPeople(people);
    state.searchPeopleLoaded = true;
    state.searchPeopleFilterKey = filterKey;
    renderSearchPeople();
  } finally {
    if (requestId === state.searchRequestId) {
      state.searchLoading = false;
      updateBusyIndicator();
    }
  }
}

function normalizeSearchPeople(people) {
  return Array.isArray(people)
    ? people
        .filter((person) => person?.name)
        .map((person) => ({
          id: Number(person.id ?? 0),
          name: String(person.name),
          root_id: person.root_id ? String(person.root_id) : null,
          thumbnail_image_id: Number.isFinite(Number(person.thumbnail_image_id))
            ? Number(person.thumbnail_image_id)
            : null,
          folder_count: Number(person.folder_count ?? 0),
        }))
        .sort((left, right) =>
          left.name.localeCompare(right.name, undefined, { sensitivity: "base" }),
        )
    : [];
}

function renderSearchPeople(options = {}) {
  prepareSearchSurface();
  const people = state.searchPersonResults;
  if (people.length === 0) {
    const message = personFiltersActive() ? "No matching persons" : "No persons in metadata";
    setStatus(message);
    renderEmptyState(message, { keepBreadcrumbs: true });
    restoreSearchScroll(options.restoreScroll);
    return;
  }

  setStatus(`${people.length} ${personFiltersActive() ? "matching " : ""}persons`);
  gridNode.replaceChildren(...people.map(renderSearchPersonCard));
  restoreSearchScroll(options.restoreScroll);
}

function renderSearchPersonCard(person) {
  const card = document.createElement("article");
  card.className = "tile person-result-tile";
  card.tabIndex = 0;
  card.title = person.name;
  card.innerHTML = `
    <div class="thumb person-result-thumb">
      <span>${escapeHtml(initials(person.name))}</span>
    </div>
    <div class="tile-body">
      <h3>${escapeHtml(person.name)}</h3>
      <p>${person.folder_count} folders</p>
    </div>
  `;

  const thumb = card.querySelector(".thumb");
  sizeTile(card);
  if (person.root_id && person.thumbnail_image_id) {
    requestThumbnailWhenVisible(
      person.root_id,
      person.thumbnail_image_id,
      thumb,
      THUMBNAIL_PIXEL_SIZE,
    );
  }

  card.addEventListener("click", () => {
    setSearchPerson(person.name, { switchToSearch: true }).catch(showError);
  });
  card.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      setSearchPerson(person.name, { switchToSearch: true }).catch(showError);
    }
  });
  return card;
}

function personFiltersActive() {
  return Boolean(
    state.searchIncludeTags.length > 0 ||
      state.searchExcludeTags.length > 0 ||
      normalizeRating(state.searchMinimumRating),
  );
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
  updateNavigationButtons();
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
  updateNavigationButtons();
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
  updateNavigationButtons();
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
  updateNavigationButtons();
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
    jpg_quality: normalizeJpgQuality(Number(settings?.jpg_quality ?? 90)),
    movie_create_enabled: Boolean(settings?.movie_create_enabled),
    ffmpeg_path: String(settings?.ffmpeg_path ?? ""),
    movie_codec: normalizeMovieCodec(settings?.movie_codec),
    movie_quality: normalizeMovieQuality(settings?.movie_quality),
    movie_output_folder: String(settings?.movie_output_folder ?? ""),
    movie_resolution: normalizeMovieResolution(settings?.movie_resolution),
    movie_custom_resolution: normalizeMovieCustomResolution(
      settings?.movie_custom_resolution,
    ),
    movie_mode: normalizeMovieMode(settings?.movie_mode),
    movie_fps: normalizeMovieFps(Number(settings?.movie_fps ?? 30)),
    movie_slideshow_seconds: normalizeMovieSlideshowSeconds(
      Number(settings?.movie_slideshow_seconds ?? 3),
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

function openAboutDialog() {
  if (!aboutDialog.open) {
    aboutDialog.showModal();
  }
}

function closeAboutDialog() {
  aboutDialog.close();
}

function openAboutHomepage(event) {
  event.preventDefault();
  if (invoke) {
    invoke("open_homepage").catch(showError);
    return;
  }

  window.open(aboutHomepageLink.href, "_blank", "noopener");
}

function closeSettingsDialog() {
  settingsDialog.close();
}

function renderSettingsDialog() {
  upscaleFullscreenInput.checked = state.settings.upscale_fullscreen_images;
  slideshowLoopInput.checked = state.settings.slideshow_loop;
  syncSlideshowSpeedControls();
  slideshowIgnoreSmallerInput.value = String(state.settings.slideshow_ignore_smaller_than);
  syncJpgQualityControl();
  movieCreateEnabledInput.checked = state.settings.movie_create_enabled;
  ffmpegPathInput.value = state.settings.ffmpeg_path;
  movieCodecInput.value = state.settings.movie_codec;
  movieQualityInput.value = state.settings.movie_quality;
  movieOutputFolderInput.value = state.settings.movie_output_folder;
  movieResolutionInput.value = state.settings.movie_resolution;
  movieCustomResolutionInput.value = state.settings.movie_custom_resolution;
  movieModeInput.value = state.settings.movie_mode;
  movieFpsInput.value = String(state.settings.movie_fps);
  movieSlideshowSecondsInput.value = state.settings.movie_slideshow_seconds.toFixed(3);
  updateMovieSettingsVisibility();
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

function handleJpgQualityInput() {
  state.settings.jpg_quality = normalizeJpgQuality(Number(jpgQualityInput.value));
  syncJpgQualityControl();
}

function handleMovieSlideshowSecondsInput() {
  const value = Number(movieSlideshowSecondsInput.value);
  if (!Number.isFinite(value) || value <= 0) {
    return;
  }

  state.settings.movie_slideshow_seconds = normalizeMovieSlideshowSeconds(value);
}

function handleMovieCustomResolutionInput() {
  state.settings.movie_custom_resolution = movieCustomResolutionInput.value.trim();
}

function handleSettingsInput() {
  state.settings.upscale_fullscreen_images = upscaleFullscreenInput.checked;
  state.settings.slideshow_loop = slideshowLoopInput.checked;
  state.settings.slideshow_speed_seconds = speedFromControls();
  syncSlideshowSpeedControls();
  state.settings.slideshow_ignore_smaller_than = normalizeIgnoreSmallerValue(
    Number(slideshowIgnoreSmallerInput.value),
  );
  state.settings.jpg_quality = normalizeJpgQuality(Number(jpgQualityInput.value));
  state.settings.movie_create_enabled = movieCreateEnabledInput.checked;
  state.settings.movie_codec = normalizeMovieCodec(movieCodecInput.value);
  state.settings.movie_quality = normalizeMovieQuality(movieQualityInput.value);
  state.settings.movie_resolution = normalizeMovieResolution(movieResolutionInput.value);
  state.settings.movie_custom_resolution = normalizeMovieCustomResolution(
    movieCustomResolutionInput.value,
  );
  state.settings.movie_mode = normalizeMovieMode(movieModeInput.value);
  state.settings.movie_fps = normalizeMovieFps(Number(movieFpsInput.value));
  state.settings.movie_slideshow_seconds = normalizeMovieSlideshowSeconds(
    Number(movieSlideshowSecondsInput.value),
  );
  syncJpgQualityControl();
  updateMovieSettingsVisibility();
  applyViewerUpscaleSetting();
  saveSettingsPreferences().catch(showError);
  if (state.slideshowActive) {
    scheduleSlideshow();
  }
}

async function pickFfmpegPath() {
  if (!invoke) {
    return;
  }

  const path = await invoke("pick_ffmpeg_executable");
  if (!path) {
    return;
  }

  state.settings.ffmpeg_path = path;
  await saveSettingsPreferences();
  renderSettingsDialog();
}

async function pickMovieOutputFolder() {
  if (!invoke) {
    return;
  }

  const path = await invoke("pick_movie_output_folder");
  if (!path) {
    return;
  }

  state.settings.movie_output_folder = path;
  await saveSettingsPreferences();
  renderSettingsDialog();
}

function clearMovieOutputFolder() {
  state.settings.movie_output_folder = "";
  saveSettingsPreferences().catch(showError);
  renderSettingsDialog();
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
      jpg_quality: state.settings.jpg_quality,
      movie_create_enabled: state.settings.movie_create_enabled,
      ffmpeg_path: state.settings.ffmpeg_path,
      movie_codec: state.settings.movie_codec,
      movie_quality: state.settings.movie_quality,
      movie_output_folder: state.settings.movie_output_folder,
      movie_resolution: state.settings.movie_resolution,
      movie_custom_resolution: state.settings.movie_custom_resolution,
      movie_mode: state.settings.movie_mode,
      movie_fps: state.settings.movie_fps,
      movie_slideshow_seconds: state.settings.movie_slideshow_seconds,
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

function normalizeJpgQuality(value) {
  if (!Number.isFinite(value)) {
    return 90;
  }
  return Math.min(100, Math.max(1, Math.round(value)));
}

function syncJpgQualityControl() {
  const value = normalizeJpgQuality(state.settings.jpg_quality);
  state.settings.jpg_quality = value;
  jpgQualityInput.value = String(value);
  jpgQualityValue.textContent = String(value);
}

function normalizeMovieCodec(value) {
  return ["h264", "h265"].includes(value) ? value : "h264";
}

function normalizeMovieQuality(value) {
  return ["high", "balanced", "small"].includes(value) ? value : "balanced";
}

function normalizeMovieResolution(value) {
  return ["720p", "1080p", "4k", "custom"].includes(value) ? value : "1080p";
}

function normalizeMovieCustomResolution(value) {
  const match = String(value ?? "")
    .trim()
    .match(/^(\d{2,5})\s*[xX,* ]\s*(\d{2,5})$/);
  if (!match) {
    return "1920x1080";
  }

  const width = normalizeMovieDimension(Number(match[1]));
  const height = normalizeMovieDimension(Number(match[2]));
  return `${width}x${height}`;
}

function normalizeMovieDimension(value) {
  if (!Number.isFinite(value)) {
    return 1920;
  }
  const clamped = Math.min(8192, Math.max(16, Math.round(value)));
  return clamped % 2 === 0 ? clamped : clamped - 1;
}

function normalizeMovieMode(value) {
  return ["movie", "slideshow"].includes(value) ? value : "movie";
}

function normalizeMovieFps(value) {
  return [24, 25, 30, 50, 60].includes(value) ? value : 30;
}

function normalizeMovieSlideshowSeconds(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return 3;
  }
  return Math.round(value * 1000) / 1000;
}

function updateMovieSettingsVisibility() {
  const enabled = state.settings.movie_create_enabled;
  movieSettingsFields.hidden = !enabled;
  movieCustomResolutionRow.hidden =
    !enabled || state.settings.movie_resolution !== "custom";
  const mode = normalizeMovieMode(state.settings.movie_mode);
  movieFpsRow.hidden = !enabled || mode !== "movie";
  movieSlideshowSecondsRow.hidden = !enabled || mode !== "slideshow";
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
    normalizeRating(folder.direct_rating),
    normalizeRating(folder.inherited_rating),
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
      direct_rating: normalizeRating(folder.direct_rating),
      inherited_rating: normalizeRating(folder.inherited_rating),
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
  if (state.searchActive) {
    return;
  }

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
        switchSearchNavigationToEditMode();
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
    <div class="thumb folder-thumb">
      <span>${escapeHtml(initials(folder.name))}</span>
      ${renderFolderRatingBadge(folder)}
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
  const splat = isSplatItem(image);
  const model = isModelItem(image);
  const mediaClass = splat ? " splat-tile" : model ? " model-tile" : "";
  const thumbClass = splat ? " splat-thumb" : model ? " model-thumb" : "";
  const card = document.createElement("article");
  card.className = `tile image-tile${mediaClass}`;
  card.tabIndex = 0;
  card.title = fullImagePath(image);
  card.dataset.imageId = String(image.id);
  card.dataset.itemKey = imageItemKey(image);
  card.dataset.summarySignature = imageSummarySignature(image);
  card.innerHTML = `
    <div class="thumb image-thumb${thumbClass}" data-image-id="${image.id}">
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

function renderFolderRatingBadge(folder) {
  const directRating = normalizeRating(folder.direct_rating);
  const inheritedRating = normalizeRating(folder.inherited_rating);
  const rating = directRating ?? inheritedRating;
  if (!rating) {
    return "";
  }

  const inherited = !directRating && Boolean(inheritedRating);
  const title = inherited
    ? `${rating} of 5 stars inherited from a parent folder`
    : `${rating} of 5 stars`;
  return `
    <span class="folder-rating-badge" data-inherited="${inherited}" title="${title}" role="img" aria-label="${title}">
      <svg class="rating-star-badge" viewBox="0 0 32 32" aria-hidden="true" focusable="false">
        <path class="rating-star-glyph" d="M16 2.8l3.72 8.6 9.28.84-7 6.2 2.04 9.16L16 22.86 7.96 27.6 10 18.44l-7-6.2 9.28-.84L16 2.8z"></path>
        <text class="rating-star-number" x="16" y="17.1">${rating}</text>
      </svg>
    </span>
  `;
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
  await refreshAfterFolderCoverChange(image.root_id, state.currentPath);
  setStatus(`Cover set to ${image.file_name}`);
}

async function setParentFolderCoverFromFolder(folder) {
  const imageId = Number(folder?.thumbnail_image_id);
  const parentRelativePath = folderParentRelativePath(folder);
  if (!Number.isFinite(imageId) || parentRelativePath === null) {
    return;
  }

  await invoke("set_folder_thumbnail_by_path", {
    rootId: folder.root_id,
    relativePath: parentRelativePath,
    imageId,
  });
  await refreshAfterFolderCoverChange(folder.root_id, parentRelativePath);
  const parentName = parentRelativePath || rootDisplayName(folder.root_id);
  setStatus(`${folder.name} set as cover for ${parentName}`);
}

async function refreshAfterFolderCoverChange(rootId, relativePath) {
  const searchScroll = state.searchActive
    ? { left: gridNode.scrollLeft, top: gridNode.scrollTop }
    : null;
  const containingPath = parentPathFor(relativePath);

  if (containingPath === null) {
    await refreshOverview();
  } else {
    invalidateFolderViewCache(rootId, containingPath);
    if (
      !state.searchActive &&
      state.currentRootId === rootId &&
      state.currentPath === containingPath
    ) {
      await refreshCurrentFolder({ keepStatus: true, forceReload: true, quiet: true });
    }
  }

  if (state.searchActive) {
    invalidateSearchCaches();
    await refreshSearchSurface();
    restoreSearchScroll(searchScroll);
  }
}

function folderParentRelativePath(folder) {
  if (!folder) {
    return null;
  }

  if (folder.parent_relative_path !== null && folder.parent_relative_path !== undefined) {
    return String(folder.parent_relative_path);
  }

  return parentPathFor(folder.relative_path);
}

function canSetParentCoverFromFolder(folder) {
  return (
    folderParentRelativePath(folder) !== null &&
    Number.isFinite(Number(folder?.thumbnail_image_id))
  );
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
    const folder = folderFromTile(folderTile);
    if (!folder) {
      hideThumbContextMenu();
      return;
    }

    state.contextMenuFolder = folder;
    state.contextMenuImage = null;
    state.contextMenuRoot = null;
    const items = [
      { action: "play-folder-slideshow", label: "Play slideshow" },
      {
        action: "play-folder-slideshow-random",
        label: "Play slideshow randomized",
      },
      { action: "convert-png-to-jpg", label: "Convert PNG to JPG" },
      { action: "show-explorer", label: "Show in Explorer" },
    ];
    if (state.settings.movie_create_enabled) {
      items.splice(3, 0, {
        action: "create-movie",
        label: "Create movie from pictures",
      });
    }
    if (canSetParentCoverFromFolder(folder)) {
      items.unshift({ action: "set-parent-cover", label: "Set as parent cover" });
    }
    if (folder.relative_path) {
      items.push({ action: "recycle", label: "Move to recycle bin" });
    }
    showContextMenu(items, event.clientX, event.clientY);
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
  if (state.tagDropdownOpen && shouldCloseTagDropdownForClick(event.target)) {
    closeTagDropdown();
  }
  if (
    state.searchPersonDropdownOpen &&
    shouldCloseSearchPersonDropdownForClick(event.target)
  ) {
    closeSearchPersonDropdown();
  }
  if (
    state.searchTagDropdownTarget &&
    shouldCloseSearchTagDropdownForClick(event.target)
  ) {
    closeSearchTagDropdown();
  }
  if (
    state.searchSlideshowMenuOpen &&
    shouldCloseSearchSlideshowMenuForClick(event.target)
  ) {
    closeSearchSlideshowMenu();
  }
}

function shouldClosePersonDropdownForClick(target) {
  if (target.closest(".metadata-item-menu")) {
    return false;
  }
  if (target.closest(".person-dropdown")) {
    return false;
  }
  if (target.closest("button[data-action='toggle-person-dropdown']")) {
    return false;
  }
  return true;
}

function shouldCloseTagDropdownForClick(target) {
  if (target.closest(".metadata-item-menu")) {
    return false;
  }
  if (target.closest(".tag-dropdown")) {
    return false;
  }
  if (target.closest("button[data-action='toggle-tag-dropdown']")) {
    return false;
  }
  return true;
}

function shouldCloseSearchPersonDropdownForClick(target) {
  if (target.closest(".search-person-dropdown")) {
    return false;
  }
  if (target.closest("button[data-action='toggle-search-person-dropdown']")) {
    return false;
  }
  return true;
}

function shouldCloseSearchTagDropdownForClick(target) {
  if (target.closest(".search-tag-dropdown")) {
    return false;
  }
  if (target.closest("button[data-action='toggle-search-tag-dropdown']")) {
    return false;
  }
  return true;
}

function shouldCloseSearchSlideshowMenuForClick(target) {
  if (target.closest(".search-slideshow-menu")) {
    return false;
  }
  if (target.closest("button[data-action='toggle-search-slideshow-menu']")) {
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
    } else if (action === "set-parent-cover" && folder) {
      await setParentFolderCoverFromFolder(folder);
    } else if (action === "set-cover" && image) {
      await setCurrentFolderCover(image);
    } else if (action === "rotate-right" && image) {
      await rotateImage(image, "right");
    } else if (action === "rotate-left" && image) {
      await rotateImage(image, "left");
    } else if (action === "convert-png-to-jpg") {
      if (image) {
        await convertImagePngToJpg(image);
      } else if (folder) {
        await convertFolderPngsToJpg(folder);
      }
    } else if (action === "create-movie" && folder) {
      await createMovieFromFolder(folder);
    } else if (action === "show-explorer") {
      if (image) {
        await invoke("show_image_in_explorer", {
          rootId: image.root_id,
          imageId: image.id,
        });
      } else if (folder) {
        await invoke("show_folder_in_explorer", {
          rootId: folder.root_id,
          relativePath: folder.relative_path,
        });
      }
    } else if (action === "recycle") {
      if (image) {
        await moveImageToRecycleBin(image);
      } else if (folder) {
        await moveFolderToRecycleBin(folder);
      }
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
  const items = [{ action: "set-cover", label: "Set as cover" }];
  if (!isThreeDItem(state.contextMenuImage)) {
    items.push(
      { action: "rotate-right", label: "Rotate right" },
      { action: "rotate-left", label: "Rotate left" },
    );
  }
  items.push(
    { action: "show-explorer", label: "Show in Explorer" },
    { action: "recycle", label: "Move to recycle bin" },
  );

  if (!isSplatItem(state.contextMenuImage) && isPngFileName(state.contextMenuImage?.file_name)) {
    items.splice(3, 0, { action: "convert-png-to-jpg", label: "Convert PNG to JPG" });
  }

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

async function convertImagePngToJpg(image) {
  setStatus(`Converting ${image.file_name} to JPG...`);
  await invoke("convert_image_png_to_jpg", {
    rootId: image.root_id,
    imageId: image.id,
    folderRelativePath: state.currentPath,
  });
  state.imageUrlCache.clear();
  state.imageDimensionCache.clear();
  invalidateThumbnailDataCache(image.root_id);
  invalidateFolderViewCache(image.root_id, state.currentPath);
  invalidateFolderViewCache(image.root_id, parentPathFor(state.currentPath));
  await refreshCurrentFolder({ keepStatus: true, forceReload: true });
  setStatus(`Converted ${image.file_name} to JPG`);
}

async function convertFolderPngsToJpg(folder) {
  const folderName = folder.name || folder.relative_path || "folder";
  setStatus(`Converting PNG images in ${folderName}...`);
  const report = await invoke("convert_folder_pngs_to_jpg", {
    rootId: folder.root_id,
    relativePath: folder.relative_path,
  });
  state.imageUrlCache.clear();
  state.imageDimensionCache.clear();
  invalidateThumbnailDataCache(folder.root_id);
  invalidateFolderViewCache(folder.root_id);

  if (!state.searchActive) {
    await refreshCurrentFolder({ keepStatus: true, forceReload: true });
  }

  const converted = Number(report?.converted ?? 0);
  if (converted === 0) {
    setStatus(`No PNG images found in ${folderName}`);
  } else {
    setStatus(`Converted ${converted} PNG ${converted === 1 ? "file" : "files"} to JPG in ${folderName}`);
  }
}

async function createMovieFromFolder(folder) {
  if (state.movieJob?.running) {
    setStatus("A movie creation is already running");
    return;
  }

  const folderName = folder.name || folder.relative_path || "folder";
  setStatus(`Preparing movie from ${folderName}...`);
  const preview = await invoke("movie_output_preview", {
    rootId: folder.root_id,
    relativePath: folder.relative_path,
  });
  if (preview?.exists) {
    const confirmed = await confirmWarning(
      "Overwrite existing movie?",
      preview.output_path,
    );
    if (!confirmed) {
      setStatus("Movie creation canceled");
      return;
    }
  }

  const jobId = newMovieJobId();
  openMovieProgressDialog({
    jobId,
    folderName,
    outputPath: preview?.output_path ?? "",
    imageCount: Number(preview?.image_count ?? 0),
  });
  setStatus(`Creating movie from ${folderName}...`);
  try {
    await invoke("start_movie_creation", {
      rootId: folder.root_id,
      relativePath: folder.relative_path,
      overwrite: Boolean(preview?.exists),
      jobId,
    });
  } catch (error) {
    failMovieProgress(jobId, String(error));
    throw error;
  }
}

function newMovieJobId() {
  const random = Math.random().toString(36).slice(2, 10);
  return `movie-${Date.now()}-${random}`;
}

function openMovieProgressDialog({ jobId, folderName, outputPath, imageCount }) {
  state.movieJob = {
    id: jobId,
    folderName,
    outputPath,
    imageCount,
    running: true,
  };
  movieProgressTitle.textContent = `Creating ${folderName}`;
  movieProgressMeta.textContent = `${imageCount} images -> ${outputPath}`;
  movieProgressOutput.textContent = "";
  appendMovieProgressText(`Output: ${outputPath}\nImages: ${imageCount}\n\n`);
  movieCancelButton.disabled = false;
  movieCancelButton.textContent = "Cancel";
  movieCancelButton.hidden = false;
  movieCloseButton.hidden = true;
  movieCloseButton.disabled = true;
  if (!movieProgressDialog.open) {
    movieProgressDialog.showModal();
  }
}

function closeMovieProgressDialog() {
  if (state.movieJob?.running) {
    return;
  }
  if (movieProgressDialog.open) {
    movieProgressDialog.close();
  }
}

async function cancelActiveMovieCreation() {
  const job = state.movieJob;
  if (!job?.running) {
    return;
  }

  movieCancelButton.disabled = true;
  movieCancelButton.textContent = "Canceling...";
  appendMovieProgressText("\nCancel requested...\n");
  setStatus("Canceling movie creation...");
  try {
    await invoke("cancel_movie_creation", { jobId: job.id });
  } catch (error) {
    appendMovieProgressText(`\nCancel failed: ${String(error)}\n`);
    movieCancelButton.disabled = false;
    movieCancelButton.textContent = "Cancel";
    showError(error);
  }
}

function appendMovieProgressOutput(payload) {
  if (!payload || payload.job_id !== state.movieJob?.id) {
    return;
  }
  appendMovieProgressText(String(payload.text ?? ""));
}

function appendMovieProgressText(text) {
  movieProgressOutput.textContent += text.replaceAll("\r", "\n");
  if (movieProgressOutput.textContent.length > 240000) {
    movieProgressOutput.textContent = movieProgressOutput.textContent.slice(-180000);
  }
  movieProgressOutput.scrollTop = movieProgressOutput.scrollHeight;
}

function finishMovieProgress(payload) {
  if (!payload || payload.job_id !== state.movieJob?.id) {
    return;
  }

  state.movieJob.running = false;
  movieCancelButton.hidden = true;
  movieCancelButton.disabled = true;
  movieCloseButton.hidden = false;
  movieCloseButton.disabled = false;
  const message = String(payload.message || "");
  if (payload.success) {
    appendMovieProgressText(`\n\nDone: ${payload.output_path}\n`);
    setStatus(
      `Created movie from ${Number(payload.image_count ?? 0)} images: ${payload.output_path}`,
    );
  } else if (payload.canceled) {
    appendMovieProgressText("\n\nCanceled.\n");
    setStatus("Movie creation canceled");
  } else {
    appendMovieProgressText(`\n\nFailed: ${message}\n`);
    setStatus(message || "Movie creation failed");
  }
}

function failMovieProgress(jobId, message) {
  if (jobId !== state.movieJob?.id) {
    return;
  }

  state.movieJob.running = false;
  appendMovieProgressText(`\nFailed: ${message}\n`);
  movieCancelButton.hidden = true;
  movieCancelButton.disabled = true;
  movieCloseButton.hidden = false;
  movieCloseButton.disabled = false;
  setStatus(message);
}

function confirmWarning(message, detail = "") {
  if (state.warningDialogResolve) {
    return Promise.resolve(false);
  }

  warningMessage.textContent = message;
  warningDetail.textContent = detail;
  warningDetail.hidden = !detail;
  if (!warningDialog.open) {
    warningDialog.showModal();
  }
  warningOkButton.focus({ preventScroll: true });

  return new Promise((resolve) => {
    state.warningDialogResolve = resolve;
  });
}

function resolveWarningDialog(confirmed) {
  const resolve = state.warningDialogResolve;
  if (!resolve) {
    return;
  }

  state.warningDialogResolve = null;
  if (warningDialog.open) {
    warningDialog.close();
  }
  resolve(Boolean(confirmed));
}

function promptMetadataEdit(kind, name) {
  if (state.metadataEditDialogResolve) {
    return Promise.resolve(null);
  }

  const label = metadataKindLabel(kind);
  metadataEditTitle.textContent = `Edit ${label}`;
  metadataEditLabel.textContent = `${label} name`;
  metadataEditInput.value = name;
  if (!metadataEditDialog.open) {
    metadataEditDialog.showModal();
  }
  requestAnimationFrame(() => {
    metadataEditInput.focus({ preventScroll: true });
    metadataEditInput.select();
  });

  return new Promise((resolve) => {
    state.metadataEditDialogResolve = resolve;
  });
}

function resolveMetadataEditDialog(value) {
  const resolve = state.metadataEditDialogResolve;
  if (!resolve) {
    return;
  }

  state.metadataEditDialogResolve = null;
  if (metadataEditDialog.open) {
    metadataEditDialog.close();
  }
  resolve(value);
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

async function moveFolderToRecycleBin(folder) {
  const folderName = folder.name || folder.relative_path || "folder";
  const wasSearchActive = state.searchActive;
  const searchScroll = wasSearchActive
    ? { left: gridNode.scrollLeft, top: gridNode.scrollTop }
    : null;
  setStatus(`Moving ${folderName} to recycle bin...`);
  await invoke("move_folder_to_recycle_bin", {
    rootId: folder.root_id,
    relativePath: folder.relative_path,
  });
  state.imageUrlCache.clear();
  state.imageDimensionCache.clear();
  invalidateThumbnailDataCache(folder.root_id);
  invalidateFolderViewCache(folder.root_id);
  removeFolderFromCachedSearch(folder);

  if (wasSearchActive) {
    renderSearchResults({ restoreScroll: searchScroll });
    setStatus(`Moved ${folderName} to recycle bin`);
    return;
  }

  if (
    state.currentRootId === folder.root_id &&
    pathContainsPath(folder.relative_path, state.currentPath)
  ) {
    await openFolder(folder.root_id, parentPathFor(folder.relative_path), {
      keepStatus: true,
      forceReload: true,
    });
  } else {
    await refreshCurrentFolder({ keepStatus: true, forceReload: true });
  }

  setStatus(`Moved ${folderName} to recycle bin`);
}

function removeFolderFromCachedSearch(folder) {
  state.searchResults = state.searchResults.filter(
    (candidate) =>
      candidate.root_id !== folder.root_id ||
      !pathContainsPath(folder.relative_path, candidate.relative_path),
  );
  state.searchDisplayedFolders = state.searchDisplayedFolders.filter(
    (candidate) =>
      candidate.root_id !== folder.root_id ||
      !pathContainsPath(folder.relative_path, candidate.relative_path),
  );
  state.searchPeopleLoaded = false;
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
  const slideshowImages = imageSlideshowItems(images);
  if (slideshowImages.length === 0) {
    setStatus(`${folder.name} has no images`);
    return;
  }

  startPlaylistSlideshow(slideshowImages, options);
}

function searchSlideshowAvailable() {
  return state.metadataMode === APP_MODES.SEARCH && searchDisplayFolders().length > 0;
}

async function playSearchResultsSlideshow(options = {}) {
  if (!invoke) {
    return;
  }

  const folders = searchDisplayFolders();
  if (folders.length === 0) {
    setStatus("No search results to play");
    return;
  }

  const requestId = state.searchRequestId;
  const images = [];
  const seenImages = new Set();
  setStatus(`Loading slideshow from ${folders.length} search results...`);

  for (const folder of folders) {
    const folderImages = await invoke("recursive_folder_images", {
      rootId: folder.root_id,
      relativePath: folder.relative_path,
    });
    if (requestId !== state.searchRequestId || state.metadataMode !== APP_MODES.SEARCH) {
      return;
    }

    for (const image of folderImages ?? []) {
      if (!isImageItem(image)) {
        continue;
      }
      const key = `${image.root_id}:${image.id}`;
      if (seenImages.has(key)) {
        continue;
      }
      seenImages.add(key);
      images.push(image);
    }
  }

  if (images.length === 0) {
    setStatus("Search results have no images");
    return;
  }

  startPlaylistSlideshow(images, options);
}

function startPlaylistSlideshow(images, options = {}) {
  const playlist = imageSlideshowItems(images);
  if (playlist.length === 0) {
    setStatus("No images available for slideshow");
    return;
  }

  state.slideshowPlaylist = options.randomized ? shuffleImages(playlist) : [...playlist];
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

function folderFromTile(tile) {
  const rootId = tile.dataset.rootId;
  const relativePath = tile.dataset.folderPath ?? "";
  if (state.searchActive) {
    return searchFolderByPath(rootId, relativePath);
  }
  return folderByPath(rootId, relativePath);
}

function folderByPath(rootId, relativePath) {
  return (
    state.currentView?.folders.find(
      (folder) => folder.root_id === rootId && folder.relative_path === relativePath,
    ) ?? null
  );
}

function searchFolderByPath(rootId, relativePath) {
  return (
    state.searchDisplayedFolders.find(
      (folder) => folder.root_id === rootId && folder.relative_path === relativePath,
    ) ??
    state.searchResults.find(
      (folder) => folder.root_id === rootId && folder.relative_path === relativePath,
    ) ??
    null
  );
}

function rootById(rootId) {
  return state.roots.find((root) => root.id === rootId) ?? null;
}

function rootDisplayName(rootId) {
  return rootById(rootId)?.display_name ?? "Root";
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

function isPngFileName(fileName) {
  return String(fileName || "").toLowerCase().endsWith(".png");
}

function isImageItem(image) {
  return isImageFileName(image?.file_name) || isImageFileName(image?.relative_path);
}

function isImageFileName(fileName) {
  const lower = String(fileName || "").toLowerCase();
  return SUPPORTED_IMAGE_EXTENSIONS.some((extension) => lower.endsWith(extension));
}

function imageSlideshowItems(images) {
  return (images ?? []).filter(isImageItem);
}

function sameImage(left, right) {
  return left?.id === right?.id && left?.root_id === right?.root_id;
}

function isRawPlyFileName(fileName) {
  const lower = String(fileName || "").toLowerCase();
  return lower.endsWith(".ply") && !lower.endsWith(".compressed.ply");
}

function isSplatItem(image) {
  return isSplatFileName(image?.file_name) || isSplatFileName(image?.relative_path);
}

function isSplatFileName(fileName) {
  const lower = String(fileName || "").toLowerCase();
  return SUPPORTED_SPLAT_EXTENSIONS.some((extension) => lower.endsWith(extension));
}

function isModelItem(image) {
  return isModelFileName(image?.file_name) || isModelFileName(image?.relative_path);
}

function isModelFileName(fileName) {
  const lower = String(fileName || "").toLowerCase();
  return SUPPORTED_MODEL_EXTENSIONS.some((extension) => lower.endsWith(extension));
}

function isThreeDItem(image) {
  return isSplatItem(image) || isModelItem(image);
}

function threeDLabelFor(image) {
  return isModelItem(image) ? "GLB" : "3DGS";
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
  if (isThreeDItem(image)) {
    await renderViewerThreeDAsset(image, generation);
    return;
  }

  stopSplatViewer();
  splatViewerNode.classList.add("hidden");
  viewerImage.classList.remove("hidden");
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

async function renderViewerThreeDAsset(image, generation) {
  stopSlideshow();
  viewerImage.removeAttribute("src");
  viewerImage.classList.add("hidden");
  splatViewerNode.classList.remove("hidden");
  if (isOversizedRawPly(image)) {
    splatStatusNode.textContent = `Raw PLY is too large for the embedded viewer (${formatBytes(image.file_size)}). Convert it to SPZ, SOG, or compressed PLY to view it here.`;
    splatStatusNode.hidden = false;
    return;
  }
  const assetLabel = threeDLabelFor(image);
  splatStatusNode.textContent = `Reading ${assetLabel} file...`;
  splatStatusNode.hidden = false;

  try {
    const source = await threeDSourceFor(image);
    if (generation !== state.viewerGeneration) {
      return;
    }
    splatStatusNode.textContent = `Loading PlayCanvas runtime (${formatBytes(source.byteLength)})...`;
    const splatViewer = await ensureSplatViewer((message) => {
      if (generation === state.viewerGeneration) {
        splatStatusNode.textContent = message;
      }
    });
    if (generation !== state.viewerGeneration) {
      return;
    }
    const cameraState = await assetCameraStateFor(image);
    if (generation !== state.viewerGeneration) {
      return;
    }
    splatStatusNode.textContent = `Starting PlayCanvas loader (${formatBytes(source.byteLength)})...`;
    await splatViewer.open({
      fileBytes: source.fileBytes,
      url: source.url,
      fileName: image.file_name,
      byteLength: source.byteLength,
      cameraState,
      kind: isModelItem(image) ? "model" : "splat",
    });
  } catch (error) {
    if (generation === state.viewerGeneration) {
      splatStatusNode.textContent = `Could not load ${assetLabel}: ${errorMessage(error)}`;
      splatStatusNode.hidden = false;
      showError(error);
    }
  }
}

async function ensureSplatViewer(onStage) {
  const slowTimer = window.setTimeout(() => {
    onStage?.("Still loading PlayCanvas runtime...");
  }, 8000);
  if (!state.splatViewerModule) {
    state.splatViewerModule = import("./playcanvas-viewer.js");
  }
  try {
    const module = await state.splatViewerModule;
    if (!state.splatViewer) {
      state.splatViewer = new module.PlayCanvasSplatViewer(splatViewerNode, splatStatusNode);
    }
    return state.splatViewer;
  } finally {
    window.clearTimeout(slowTimer);
  }
}

function stopSplatViewer() {
  state.splatViewer?.stop();
}

function currentViewerIsSplat() {
  return isSplatItem(currentViewerImage());
}

function currentViewerIsThreeD() {
  return isThreeDItem(currentViewerImage());
}

async function saveCurrentAssetThumbnail() {
  if (state.splatThumbnailSaving) {
    return;
  }
  const image = currentViewerImage();
  if (!image || !currentViewerIsThreeD() || !state.splatViewer) {
    return;
  }

  const dataUrl = state.splatViewer.captureThumbnail();
  if (!dataUrl) {
    return;
  }
  const cameraState = state.splatViewer.cameraState();

  state.splatThumbnailSaving = true;
  try {
    await invoke("save_asset_thumbnail", {
      rootId: image.root_id,
      imageId: image.id,
      dataUrl,
      cameraState,
    });
    invalidateAssetThumbnail(image, dataUrl);
    showToast("Thumbnail captured");
    setStatus(`Captured thumbnail for ${image.file_name}`);
  } finally {
    state.splatThumbnailSaving = false;
  }
}

function cycleCurrentSplatOrientation() {
  if (!currentViewerIsSplat() || !state.splatViewer) {
    return;
  }
  const label = state.splatViewer.cycleOrientation();
  if (label) {
    setStatus(`3DGS orientation: ${label}`);
  }
}

function resetCurrentSplatView() {
  if (!currentViewerIsThreeD() || !state.splatViewer) {
    return;
  }
  state.splatViewer.resetView();
  setStatus(`${threeDLabelFor(currentViewerImage())} view reset`);
}

function frameCurrentSplatView() {
  if (!currentViewerIsThreeD() || !state.splatViewer) {
    return;
  }
  state.splatViewer.frameView();
  setStatus(`${threeDLabelFor(currentViewerImage())} view framed`);
}

function invalidateAssetThumbnail(image, dataUrl) {
  for (const key of [...state.thumbnailDataCache.keys()]) {
    if (key.startsWith(`${image.root_id}:${image.id}:`)) {
      state.thumbnailDataCache.delete(key);
    }
  }

  const card = gridNode.querySelector(`[data-image-id="${image.id}"]`);
  const thumb = card?.querySelector(".thumb");
  if (thumb) {
    applyThumbnailData(thumb, dataUrl);
  }
}

async function assetCameraStateFor(image) {
  if (!invoke || !isThreeDItem(image)) {
    return null;
  }

  try {
    return await invoke("asset_camera_state", {
      rootId: image.root_id,
      imageId: image.id,
    });
  } catch (error) {
    console.warn("Could not restore 3D camera state", error);
    return null;
  }
}

async function threeDSourceFor(image) {
  if (!invoke) {
    throw new Error("Run with Tauri to load 3D files.");
  }

  if (convertFileSrc) {
    const path = await invoke("image_file_path", {
      rootId: image.root_id,
      imageId: image.id,
    });
    return {
      url: withCacheBuster(convertFileSrc(path), image.modified_unix_ms),
      fileName: image.file_name,
      byteLength: image.file_size || 0,
    };
  }

  const fileBytes = await assetFileBytesFor(image);
  return {
    fileBytes,
    fileName: image.file_name,
    byteLength: fileBytes.byteLength,
  };
}

function isOversizedRawPly(image) {
  return isRawPlyFileName(image?.file_name || image?.relative_path) && (image?.file_size || 0) >= RAW_PLY_DIRECT_LOAD_LIMIT_BYTES;
}

async function assetFileBytesFor(image) {
  if (!invoke) {
    throw new Error("Run with Tauri to load 3D files.");
  }
  const response = await invoke("asset_file_bytes", {
    rootId: image.root_id,
    imageId: image.id,
  });
  return normalizeByteResponse(response);
}

function normalizeByteResponse(response) {
  if (response instanceof Uint8Array) {
    return response;
  }
  if (response instanceof ArrayBuffer) {
    return new Uint8Array(response);
  }
  if (ArrayBuffer.isView(response)) {
    return new Uint8Array(response.buffer, response.byteOffset, response.byteLength);
  }
  if (Array.isArray(response)) {
    return new Uint8Array(response);
  }
  throw new Error(`Unexpected 3D byte response: ${typeof response}`);
}

function formatBytes(bytes) {
  const value = Number(bytes) || 0;
  if (value >= 1024 * 1024) {
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }
  if (value >= 1024) {
    return `${Math.round(value / 1024)} KB`;
  }
  return `${value} B`;
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
    if (!isImageItem(image)) {
      continue;
    }
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
  const currentImage = currentViewerImage();
  if (!state.slideshowPlaylist) {
    state.slideshowPlaylist = imageSlideshowItems(state.currentView?.images ?? []);
    const currentIndex = currentImage
      ? state.slideshowPlaylist.findIndex((image) => sameImage(image, currentImage))
      : -1;
    state.viewerIndex = currentIndex >= 0 ? currentIndex : 0;
  } else {
    state.slideshowPlaylist = imageSlideshowItems(state.slideshowPlaylist);
  }

  if (state.slideshowPlaylist.length === 0) {
    setStatus("No images available for slideshow");
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

  const playlist = imageSlideshowItems(images);
  if (playlist.length === 0) {
    stopSlideshow({ ended: true });
    setStatus("No images available for slideshow");
    return;
  }

  state.slideshowPlaylist = shuffleImages(playlist);
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
  if (currentViewerIsThreeD()) {
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
  stopSplatViewer();
  resetViewerCursor();
  state.slideshowPlaylist = null;
  state.slideshowEnded = false;
  state.slideshowSkipAttempts = 0;
  state.viewerGeneration += 1;
  viewer.classList.add("hidden");
  viewerImage.removeAttribute("src");
  viewerImage.classList.remove("hidden");
  splatViewerNode.classList.add("hidden");
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

function showToast(message, durationMs = 1800) {
  const target = viewer && !viewer.classList.contains("hidden") && viewerToastNode
    ? viewerToastNode
    : toastNode;
  if (!target) {
    return;
  }

  if (state.toastTimer) {
    window.clearTimeout(state.toastTimer);
  }

  if (target !== toastNode) {
    toastNode?.classList.remove("visible");
  }
  if (target !== viewerToastNode) {
    viewerToastNode?.classList.remove("visible");
  }

  target.textContent = message;
  target.classList.add("visible");
  state.toastTimer = window.setTimeout(() => {
    target.classList.remove("visible");
    state.toastTimer = null;
  }, durationMs);
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

  if (state.searchLoading) {
    return state.metadataMode === APP_MODES.PERSONS ? "Loading persons" : "Searching";
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
  const message = errorMessage(error);
  setStatus(message);
  console.error(error);
}

function errorMessage(error) {
  return error?.message || String(error);
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
