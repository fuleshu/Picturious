import * as pc from "./vendor/playcanvas/playcanvas.min.mjs";
import { CameraControls } from "./vendor/playcanvas/camera-controls.mjs";
import { createGSplatEntityFromSpzUrlAsync } from "./vendor/playcanvas/spz-loader-playcanvas.js";

const DEFAULT_FOV = 60;
const FIT_PADDING = 1.45;
const DEFAULT_EYE_HEIGHT = 0;
const MAX_DEVICE_PIXEL_RATIO = 2;
const RAW_PLY_MEMORY_SAFE_MODE = true;
const BACKGROUND = new pc.Color(0.03, 0.04, 0.045, 1);
const MODEL_BACKGROUND = new pc.Color(0.66, 0.71, 0.75, 1);
const MODEL_AMBIENT = new pc.Color(0.012, 0.014, 0.016);
const MODEL_KEY_LIGHT_INTENSITY = 4.0;
const STUDIO_ENV_RGBM_RANGE = 8;
const DEFAULT_FORWARD_FOCUS = new pc.Vec3(0, DEFAULT_EYE_HEIGHT, -1);
const REQUIRED_PLY_PROPERTIES = new Set([
  "x",
  "y",
  "z",
  "f_dc_0",
  "f_dc_1",
  "f_dc_2",
  "opacity",
  "scale_0",
  "scale_1",
  "scale_2",
  "rot_0",
  "rot_1",
  "rot_2",
  "rot_3",
]);

let nextCanvasId = 1;

export class PlayCanvasSplatViewer {
  constructor(container, statusNode) {
    this.container = container;
    this.statusNode = statusNode;
    this.canvas = null;
    this.app = null;
    this.cameraEntity = null;
    this.controls = null;
    this.splatEntity = null;
    this.splatAsset = null;
    this.modelEntity = null;
    this.modelAsset = null;
    this.keyLightEntity = null;
    this.studioEnvSource = null;
    this.studioEnvCubemap = null;
    this.studioEnvAtlas = null;
    this.objectUrl = null;
    this.resizeObserver = null;
    this.orientationIndex = 0;
    this.contentKind = "splat";
    this.currentFileName = "";
    this.loadToken = 0;
    this.disposed = false;

    this.handleResize = this.handleResize.bind(this);
  }

  async open({ url, fileBytes, fileName, byteLength = 0, cameraState = null, kind = "splat" }) {
    this.ensureApp();
    this.clearSplat();
    this.contentKind = kind === "model" ? "model" : "splat";
    this.currentFileName = fileName || "";
    this.orientationIndex = defaultOrientationIndexFor(fileName);
    this.setStatus(this.contentKind === "model" ? "Loading GLB model..." : "Loading 3DGS...");

    const token = ++this.loadToken;
    const sourceUrl = url || this.objectUrlFor(fileBytes, fileName);

    try {
      if (this.contentKind === "model") {
        await this.openModelAsset(sourceUrl, fileName, token);
      } else if (isSpzFileName(fileName)) {
        await this.openSpz(sourceUrl, token);
      } else {
        await this.openGsplatAsset(sourceUrl, fileName, token, byteLength);
      }
    } catch (error) {
      if (token === this.loadToken) {
        this.clearSplat();
        const label = this.contentKind === "model" ? "GLB" : "3DGS";
        this.setStatus(`Could not load ${label}: ${errorMessage(error)}`);
      }
      throw error;
    }

    if (token !== this.loadToken) {
      return;
    }

    if (this.contentKind === "splat") {
      this.applySavedOrientation(cameraState);
      this.applyOrientation();
    }
    if (!this.applyCameraState(cameraState)) {
      this.defaultView();
    }
    this.setStatus("");
    this.start();
  }

  start() {
    if (this.app && !this.app.frameRequestId) {
      this.app.requestAnimationFrame();
    }
  }

  stop() {
    if (this.app) {
      pc.AppBase.cancelTick(this.app);
    }
  }

  resetView() {
    if (this.contentKind === "model") {
      this.frameView();
    } else {
      this.originView();
    }
  }

  defaultView() {
    this.resetView();
  }

  originView() {
    if (!this.cameraEntity) {
      return;
    }

    const box = this.worldBoundsForContent();
    const position = new pc.Vec3(0, DEFAULT_EYE_HEIGHT, 0);
    const focus = box?.center?.clone() ?? DEFAULT_FORWARD_FOCUS.clone();
    const radius = box ? Math.max(box.halfExtents.length(), 0.1) : 1;

    if (position.distance(focus) < 0.01) {
      focus.copy(DEFAULT_FORWARD_FOCUS);
    }

    this.placeCamera(position, focus, radius);
  }

  frameView() {
    if (!this.cameraEntity) {
      return;
    }

    const box = this.worldBoundsForContent();
    const focus = box?.center?.clone() ?? new pc.Vec3(0, 0, 0);
    const radius = box ? Math.max(box.halfExtents.length(), 0.1) : 1;
    const fov = this.cameraEntity.camera?.fov ?? DEFAULT_FOV;
    const distance = Math.max(0.25, (radius / Math.sin(0.5 * fov * pc.math.DEG_TO_RAD)) * FIT_PADDING);
    const position = new pc.Vec3(focus.x, focus.y, focus.z + distance);
    this.placeCamera(position, focus, radius);
  }

  cycleOrientation() {
    if (this.contentKind !== "splat" || !this.splatEntity) {
      return;
    }
    this.orientationIndex = (this.orientationIndex + 1) % orientationPresets.length;
    this.applyOrientation();
    this.resetView();
    return orientationPresets[this.orientationIndex].label;
  }

  captureThumbnail(maxSize = 900) {
    if (!this.app || !this.canvas || !this.currentContentEntity()) {
      return null;
    }
    this.app.render();
    const sourceWidth = this.canvas.width;
    const sourceHeight = this.canvas.height;
    const scale = Math.min(1, maxSize / Math.max(sourceWidth, sourceHeight));
    if (scale >= 1) {
      return this.canvas.toDataURL("image/jpeg", 0.9);
    }

    const canvas = document.createElement("canvas");
    canvas.width = Math.max(1, Math.round(sourceWidth * scale));
    canvas.height = Math.max(1, Math.round(sourceHeight * scale));
    const context = canvas.getContext("2d", { alpha: false });
    context.drawImage(this.canvas, 0, 0, canvas.width, canvas.height);
    return canvas.toDataURL("image/jpeg", 0.88);
  }

  cameraState() {
    if (!this.cameraEntity) {
      return null;
    }

    const position = this.cameraEntity.getPosition();
    const focus = this.controls?.focusPoint ?? this.cameraEntity.getPosition().clone().add(this.cameraEntity.forward);
    return {
      version: 1,
      kind: this.contentKind,
      orientation:
        this.contentKind === "splat"
          ? orientationPresets[this.orientationIndex]?.label ?? "identity"
          : "identity",
      position: vec3ToArray(position),
      focus: vec3ToArray(focus),
      fov: this.cameraEntity.camera?.fov ?? DEFAULT_FOV,
    };
  }

  dispose() {
    this.loadToken += 1;
    this.stop();
    this.clearSplat();
    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
      this.resizeObserver = null;
    }
    this.studioEnvSource?.destroy();
    this.studioEnvCubemap?.destroy();
    this.studioEnvAtlas?.destroy();
    this.studioEnvSource = null;
    this.studioEnvCubemap = null;
    this.studioEnvAtlas = null;
    this.app?.destroy();
    this.app = null;
    this.cameraEntity = null;
    this.controls = null;
    this.keyLightEntity = null;
    this.canvas?.remove();
    this.canvas = null;
    this.disposed = true;
  }

  ensureApp() {
    if (this.app) {
      this.handleResize();
      this.start();
      return;
    }

    this.canvas = document.createElement("canvas");
    this.canvas.id = `picturious-playcanvas-${nextCanvasId++}`;
    this.canvas.className = "splat-canvas";
    this.canvas.tabIndex = 0;
    this.container.appendChild(this.canvas);

    this.app = new pc.Application(this.canvas, {
      graphicsDeviceOptions: {
        alpha: false,
        antialias: false,
        preserveDrawingBuffer: true,
        powerPreference: "high-performance",
      },
    });
    this.app.graphicsDevice.maxPixelRatio = Math.min(window.devicePixelRatio || 1, MAX_DEVICE_PIXEL_RATIO);
    this.app.setCanvasFillMode(pc.FILLMODE_NONE);
    this.app.setCanvasResolution(pc.RESOLUTION_AUTO);

    this.cameraEntity = new pc.Entity("3DGS Camera");
    this.cameraEntity.addComponent("camera", {
      clearColor: BACKGROUND,
      fov: DEFAULT_FOV,
      nearClip: 0.001,
      farClip: 10000,
    });
    this.cameraEntity.addComponent("script");
    this.app.root.addChild(this.cameraEntity);

    this.controls = this.cameraEntity.script.create(CameraControls);
    if (this.controls) {
      this.controls.moveSpeed = 4;
      this.controls.moveFastSpeed = 16;
      this.controls.moveSlowSpeed = 1;
      this.controls.rotateSpeed = 0.16;
      this.controls.zoomSpeed = 0.00065;
      this.controls.moveDamping = 0.9;
      this.controls.rotateDamping = 0.92;
      this.controls.zoomDamping = 0.9;
      this.controls.focusDamping = 0.9;
      this.controls.enablePan = true;
    }

    this.keyLightEntity = new pc.Entity("Model Key Light");
    this.keyLightEntity.addComponent("light", {
      type: "directional",
      color: new pc.Color(1, 0.97, 0.9),
      intensity: MODEL_KEY_LIGHT_INTENSITY,
    });
    this.keyLightEntity.enabled = false;
    this.app.root.addChild(this.keyLightEntity);
    this.app.on("prerender", this.updateModelKeyLight, this);

    this.resizeObserver = new ResizeObserver(this.handleResize);
    this.resizeObserver.observe(this.container);
    this.handleResize();
    this.app.start();
  }

  async openSpz(sourceUrl, token) {
    this.setStatus("Decoding SPZ...");
    this.setModelLighting(false);
    const entity = await createGSplatEntityFromSpzUrlAsync(sourceUrl, {
      appId: this.canvas.id,
    });
    if (token !== this.loadToken) {
      entity.destroy?.();
      return;
    }
    entity.name = this.currentFileName || "SPZ";
    this.splatEntity = entity;
    this.app.root.addChild(entity);
  }

  async openGsplatAsset(sourceUrl, fileName, token, byteLength) {
    const rawPlySafeMode = shouldUseRawPlySafeMode(fileName);
    this.setStatus(rawPlySafeMode ? "Loading raw PLY in memory-safe mode..." : "Loading PlayCanvas GSplat...");
    this.setModelLighting(false);
    const asset = new pc.Asset(
      fileName || "3DGS",
      "gsplat",
      {
        url: sourceUrl,
        filename: fileName || sourceUrl,
      },
      gsplatAssetDataFor(fileName, byteLength),
      {
        crossOrigin: null,
        minimalMemory: true,
      },
    );

    this.splatAsset = asset;
    await new Promise((resolve, reject) => {
      const cleanup = () => {
        asset.off("load", onLoad);
        asset.off("error", onError);
        asset.off("progress", onProgress);
      };
      const onLoad = () => {
        cleanup();
        resolve();
      };
      const onError = (error) => {
        cleanup();
        reject(error);
      };
      const onProgress = (receivedBytes, totalBytes) => {
        if (token !== this.loadToken || !totalBytes) {
          return;
        }
        const percent = Math.max(0, Math.min(100, Math.round((receivedBytes / totalBytes) * 100)));
        this.setStatus(`Loading 3DGS ${percent}%`);
      };
      asset.once("load", onLoad);
      asset.once("error", onError);
      asset.on("progress", onProgress);
      this.app.assets.add(asset);
      this.app.assets.load(asset);
    });

    if (token !== this.loadToken) {
      this.unloadAsset(asset);
      return;
    }

    const entity = new pc.Entity(fileName || "3DGS");
    entity.addComponent("gsplat", {
      asset,
    });
    this.splatEntity = entity;
    this.app.root.addChild(entity);
  }

  async openModelAsset(sourceUrl, fileName, token) {
    this.setStatus("Loading GLB model...");
    this.setModelLighting(true);
    const asset = new pc.Asset(
      fileName || "GLB Model",
      "container",
      {
        url: sourceUrl,
        filename: fileName || sourceUrl,
      },
      {},
      {
        crossOrigin: null,
      },
    );

    this.modelAsset = asset;
    await new Promise((resolve, reject) => {
      const cleanup = () => {
        asset.off("load", onLoad);
        asset.off("error", onError);
        asset.off("progress", onProgress);
      };
      const onLoad = () => {
        cleanup();
        resolve();
      };
      const onError = (error) => {
        cleanup();
        reject(error);
      };
      const onProgress = (receivedBytes, totalBytes) => {
        if (token !== this.loadToken || !totalBytes) {
          return;
        }
        const percent = Math.max(0, Math.min(100, Math.round((receivedBytes / totalBytes) * 100)));
        this.setStatus(`Loading GLB ${percent}%`);
      };
      asset.once("load", onLoad);
      asset.once("error", onError);
      asset.on("progress", onProgress);
      this.app.assets.add(asset);
      this.app.assets.load(asset);
    });

    if (token !== this.loadToken) {
      this.unloadAsset(asset);
      return;
    }

    const entity = asset.resource?.instantiateRenderEntity?.();
    if (!entity) {
      throw new Error("GLB did not produce a renderable scene.");
    }
    entity.name = fileName || "GLB Model";
    this.disableImportedLights(entity);
    this.modelEntity = entity;
    this.app.root.addChild(entity);
    entity.syncHierarchy();
  }

  objectUrlFor(fileBytes, fileName) {
    if (!fileBytes) {
      throw new Error("3D source URL is missing.");
    }
    if (this.objectUrl) {
      URL.revokeObjectURL(this.objectUrl);
    }
    this.objectUrl = URL.createObjectURL(new Blob([fileBytes], { type: mimeTypeFor(fileName) }));
    return this.objectUrl;
  }

  clearSplat() {
    if (this.splatEntity) {
      this.splatEntity.destroy();
      this.splatEntity = null;
    }
    if (this.modelEntity) {
      this.modelEntity.destroy();
      this.modelEntity = null;
    }
    if (this.splatAsset) {
      this.unloadAsset(this.splatAsset);
      this.splatAsset = null;
    }
    if (this.modelAsset) {
      this.unloadAsset(this.modelAsset);
      this.modelAsset = null;
    }
    if (this.objectUrl) {
      URL.revokeObjectURL(this.objectUrl);
      this.objectUrl = null;
    }
    this.setModelLighting(false);
  }

  unloadAsset(asset) {
    asset.off();
    if (this.app?.assets?.get(asset.id)) {
      this.app.assets.remove(asset);
    }
    asset.unload();
  }

  applyOrientation() {
    if (!this.splatEntity) {
      return;
    }
    orientationPresets[this.orientationIndex].apply(this.splatEntity);
    this.splatEntity.syncHierarchy();
  }

  applySavedOrientation(cameraState) {
    const label = typeof cameraState?.orientation === "string" ? cameraState.orientation : "";
    if (!label) {
      return;
    }
    this.orientationIndex = orientationIndexForLabel(label);
  }

  applyCameraState(cameraState) {
    if (!this.cameraEntity || !cameraState) {
      return false;
    }

    const position = vec3FromArray(cameraState.position);
    const focus = vec3FromArray(cameraState.focus);
    if (!position || !focus || position.distance(focus) < 0.01) {
      return false;
    }

    const fov = Number(cameraState.fov);
    if (Number.isFinite(fov)) {
      this.cameraEntity.camera.fov = Math.max(20, Math.min(100, fov));
    }

    const box = this.worldBoundsForContent();
    const radius = box ? Math.max(box.halfExtents.length(), 0.1) : 1;
    this.placeCamera(position, focus, radius);
    return true;
  }

  worldBoundsForContent() {
    if (this.contentKind === "model") {
      return this.worldBoundsForModel();
    }
    return this.worldBoundsForSplat();
  }

  worldBoundsForSplat() {
    const localBounds = this.splatEntity?.gsplat?.resource?.aabb ?? this.splatAsset?.resource?.aabb;
    if (!localBounds || !this.splatEntity) {
      return null;
    }
    const worldBounds = new pc.BoundingBox();
    worldBounds.setFromTransformedAabb(localBounds, this.splatEntity.getWorldTransform());
    return isFiniteBounds(worldBounds) ? worldBounds : null;
  }

  worldBoundsForModel() {
    if (!this.modelEntity) {
      return null;
    }

    this.modelEntity.syncHierarchy();
    const renderComponents = this.modelEntity.findComponents?.("render") ?? [];
    const worldBounds = new pc.BoundingBox();
    let hasBounds = false;
    for (const component of renderComponents) {
      for (const meshInstance of component.meshInstances ?? []) {
        const meshBounds = meshInstance.aabb;
        if (!meshBounds || !isFiniteBounds(meshBounds)) {
          continue;
        }
        if (hasBounds) {
          worldBounds.add(meshBounds);
        } else {
          worldBounds.copy(meshBounds);
          hasBounds = true;
        }
      }
    }
    return hasBounds && isFiniteBounds(worldBounds) ? worldBounds : null;
  }

  currentContentEntity() {
    return this.contentKind === "model" ? this.modelEntity : this.splatEntity;
  }

  disableImportedLights(entity) {
    for (const light of entity.findComponents?.("light") ?? []) {
      light.enabled = false;
    }
  }

  ensureStudioEnvironment() {
    if (
      this.studioEnvAtlas ||
      !this.app?.graphicsDevice ||
      typeof document === "undefined" ||
      !document.createElement
    ) {
      return this.studioEnvAtlas;
    }

    try {
      const canvas = createStudioEnvironmentCanvas(384, 192);
      this.studioEnvSource = new pc.Texture(this.app.graphicsDevice, {
        name: "Picturious Studio Environment Source",
        width: canvas.width,
        height: canvas.height,
        format: pc.PIXELFORMAT_RGBA8,
        type: pc.TEXTURETYPE_RGBM,
        projection: pc.TEXTUREPROJECTION_EQUIRECT,
        mipmaps: false,
        addressU: pc.ADDRESS_REPEAT,
        addressV: pc.ADDRESS_CLAMP_TO_EDGE,
        minFilter: pc.FILTER_LINEAR,
        magFilter: pc.FILTER_LINEAR,
      });
      this.studioEnvSource.setSource(canvas);
      this.studioEnvCubemap = pc.EnvLighting.generateLightingSource(this.studioEnvSource, {
        size: 128,
      });
      this.studioEnvAtlas = pc.EnvLighting.generateAtlas(this.studioEnvCubemap, {
        size: 256,
        numAmbientSamples: 256,
        numReflectionSamples: 256,
      });
    } catch (error) {
      console.warn("Could not create GLB studio environment", error);
      this.studioEnvSource?.destroy();
      this.studioEnvCubemap?.destroy();
      this.studioEnvAtlas?.destroy();
      this.studioEnvSource = null;
      this.studioEnvCubemap = null;
      this.studioEnvAtlas = null;
    }
    return this.studioEnvAtlas;
  }

  setModelLighting(enabled) {
    if (this.keyLightEntity) {
      this.keyLightEntity.enabled = Boolean(enabled);
      this.updateModelKeyLight();
    }
    if (this.app?.scene) {
      this.app.scene.envAtlas = enabled ? this.ensureStudioEnvironment() : null;
      this.app.scene.skyboxIntensity = enabled ? 1.0 : 1.0;
      this.app.scene.ambientLight = enabled ? MODEL_AMBIENT.clone() : new pc.Color(0, 0, 0);
    }
    if (this.cameraEntity?.camera) {
      this.cameraEntity.camera.clearColor = enabled ? MODEL_BACKGROUND : BACKGROUND;
    }
  }

  updateModelKeyLight() {
    if (this.contentKind !== "model" || !this.keyLightEntity?.enabled || !this.cameraEntity) {
      return;
    }

    const focus =
      this.controls?.focusPoint?.clone?.() ??
      this.worldBoundsForModel()?.center?.clone() ??
      new pc.Vec3(0, 0, 0);
    const cameraPosition = this.cameraEntity.getPosition();
    const toCamera = cameraPosition.clone().sub(focus);
    const distance = Math.max(toCamera.length(), 1);
    if (toCamera.lengthSq() < 1e-6) {
      toCamera.copy(this.cameraEntity.forward).mulScalar(-1);
    } else {
      toCamera.normalize();
    }

    const lightPosition = focus
      .clone()
      .add(toCamera.mulScalar(distance * 1.15))
      .add(this.cameraEntity.right.clone().mulScalar(distance * 0.65))
      .add(this.cameraEntity.up.clone().mulScalar(distance * 0.75));

    this.keyLightEntity.setPosition(lightPosition);
    this.keyLightEntity.lookAt(focus);
    // PlayCanvas directional lights emit along the entity's negative Y axis.
    this.keyLightEntity.rotateLocal(90, 0, 0);
  }

  placeCamera(position, focus, radius) {
    const farClip = Math.max(1000, position.distance(focus) + radius * 20);
    this.cameraEntity.camera.nearClip = 0.001;
    this.cameraEntity.camera.farClip = farClip;
    this.cameraEntity.setPosition(position);
    this.cameraEntity.lookAt(focus);
    this.controls?.reset(focus, position);
    this.updateModelKeyLight();
  }

  handleResize() {
    if (!this.app || !this.container) {
      return;
    }
    const rect = this.container.getBoundingClientRect();
    const width = Math.max(1, Math.floor(rect.width));
    const height = Math.max(1, Math.floor(rect.height));
    this.app.resizeCanvas(width, height);
  }

  setStatus(message) {
    if (this.statusNode) {
      this.statusNode.textContent = message || "";
      this.statusNode.hidden = !message;
    }
  }
}

const orientationPresets = [
  {
    label: "identity",
    apply: (entity) => entity.setEulerAngles(0, 0, 0),
  },
  {
    label: "x-180",
    apply: (entity) => entity.setEulerAngles(180, 0, 0),
  },
  {
    label: "x-90",
    apply: (entity) => entity.setEulerAngles(90, 0, 0),
  },
  {
    label: "x+90",
    apply: (entity) => entity.setEulerAngles(-90, 0, 0),
  },
  {
    label: "y-90",
    apply: (entity) => entity.setEulerAngles(0, 90, 0),
  },
  {
    label: "y+90",
    apply: (entity) => entity.setEulerAngles(0, -90, 0),
  },
  {
    label: "z-180",
    apply: (entity) => entity.setEulerAngles(0, 0, 180),
  },
  {
    label: "z-90",
    apply: (entity) => entity.setEulerAngles(0, 0, 90),
  },
  {
    label: "z+90",
    apply: (entity) => entity.setEulerAngles(0, 0, -90),
  },
];

function defaultOrientationIndexFor(fileName) {
  if (isPlyFileName(fileName)) {
    return orientationIndexForLabel("x-180");
  }
  return 0;
}

function orientationIndexForLabel(label) {
  const index = orientationPresets.findIndex((preset) => preset.label === label);
  return index >= 0 ? index : 0;
}

function isSpzFileName(fileName) {
  return String(fileName || "").toLowerCase().endsWith(".spz");
}

function isGlbFileName(fileName) {
  return String(fileName || "").toLowerCase().endsWith(".glb");
}

function isPlyFileName(fileName) {
  const lowerName = String(fileName || "").toLowerCase();
  return lowerName.endsWith(".ply") || lowerName.endsWith(".compressed.ply");
}

function shouldUseRawPlySafeMode(fileName) {
  const lowerName = String(fileName || "").toLowerCase();
  return RAW_PLY_MEMORY_SAFE_MODE && lowerName.endsWith(".ply") && !lowerName.endsWith(".compressed.ply");
}

function gsplatAssetDataFor(fileName) {
  if (!shouldUseRawPlySafeMode(fileName)) {
    return {};
  }

  return {
    elementFilter: (propertyName) => REQUIRED_PLY_PROPERTIES.has(propertyName),
    reorder: false,
  };
}

function createStudioEnvironmentCanvas(width, height) {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const context = canvas.getContext("2d", { alpha: true });
  const imageData = context.createImageData(width, height);
  const pixels = imageData.data;

  for (let y = 0; y < height; y += 1) {
    const v = y / Math.max(1, height - 1);
    for (let x = 0; x < width; x += 1) {
      const u = x / Math.max(1, width - 1);
      const sky = Math.max(0, 1 - v * 1.35);
      const floor = smoothStep(0.58, 1, v);
      const color = [
        0.018 + sky * 0.09 + floor * 0.01,
        0.02 + sky * 0.105 + floor * 0.01,
        0.022 + sky * 0.12 + floor * 0.009,
      ];

      addHdrPanel(color, u, v, 0.66, 0.2, 0.12, 0.095, [6.8, 5.9, 4.8]);
      addHdrPanel(color, u, v, 0.24, 0.34, 0.18, 0.16, [0.44, 0.6, 0.72]);
      addHdrPanel(color, u, v, 0.82, 0.52, 0.15, 0.18, [0.28, 0.38, 0.42]);
      addHdrPanel(color, u, v, 0.5, 0.88, 0.42, 0.16, [0.08, 0.075, 0.065]);

      encodeRgbm(pixels, (y * width + x) * 4, color);
    }
  }

  context.putImageData(imageData, 0, 0);

  return canvas;
}

function addHdrPanel(color, u, v, centerU, centerV, radiusU, radiusV, panelColor) {
  const wrappedU = Math.min(Math.abs(u - centerU), 1 - Math.abs(u - centerU));
  const du = wrappedU / radiusU;
  const dv = (v - centerV) / radiusV;
  const weight = Math.exp(-(du * du + dv * dv) * 2.6);
  color[0] += panelColor[0] * weight;
  color[1] += panelColor[1] * weight;
  color[2] += panelColor[2] * weight;
}

function encodeRgbm(pixels, offset, color) {
  const maxChannel = Math.max(color[0], color[1], color[2], 1e-6);
  const multiplier = Math.min(1, Math.max(1 / 255, Math.ceil((maxChannel / STUDIO_ENV_RGBM_RANGE) * 255) / 255));
  const scale = multiplier * STUDIO_ENV_RGBM_RANGE;
  pixels[offset] = Math.round(clamp01(color[0] / scale) * 255);
  pixels[offset + 1] = Math.round(clamp01(color[1] / scale) * 255);
  pixels[offset + 2] = Math.round(clamp01(color[2] / scale) * 255);
  pixels[offset + 3] = Math.round(multiplier * 255);
}

function smoothStep(edge0, edge1, value) {
  const t = clamp01((value - edge0) / (edge1 - edge0));
  return t * t * (3 - 2 * t);
}

function clamp01(value) {
  return Math.max(0, Math.min(1, value));
}

function vec3ToArray(vec) {
  return [vec.x, vec.y, vec.z].map((value) => Number(value));
}

function vec3FromArray(values) {
  if (!Array.isArray(values) || values.length !== 3) {
    return null;
  }
  const [x, y, z] = values.map(Number);
  if (![x, y, z].every(Number.isFinite)) {
    return null;
  }
  return new pc.Vec3(x, y, z);
}

function mimeTypeFor(fileName) {
  if (isGlbFileName(fileName)) {
    return "model/gltf-binary";
  }
  return isSpzFileName(fileName) ? "model/vnd.niantic.spz" : "application/octet-stream";
}

function isFiniteBounds(bounds) {
  const min = bounds.getMin();
  const max = bounds.getMax();
  return [min.x, min.y, min.z, max.x, max.y, max.z].every(Number.isFinite);
}

function errorMessage(error) {
  return error?.message || String(error);
}
