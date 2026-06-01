#![windows_subsystem = "windows"]

use anyhow::{Context, Result, bail};
use std::ffi::{OsStr, c_void};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, BufRead, Write};
use std::mem::size_of;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::null;
use std::sync::{Mutex, OnceLock, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use windows::Win32::Devices::Display::{
    DISPLAYCONFIG_DEVICE_INFO_GET_SDR_WHITE_LEVEL, DISPLAYCONFIG_DEVICE_INFO_GET_SOURCE_NAME,
    DISPLAYCONFIG_DEVICE_INFO_HEADER, DISPLAYCONFIG_MODE_INFO, DISPLAYCONFIG_PATH_INFO,
    DISPLAYCONFIG_SDR_WHITE_LEVEL, DISPLAYCONFIG_SOURCE_DEVICE_NAME, DisplayConfigGetDeviceInfo,
    GetDisplayConfigBufferSizes, QDC_ONLY_ACTIVE_PATHS, QueryDisplayConfig,
};
use windows::Win32::Foundation::{
    ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS, GENERIC_READ, HINSTANCE, HMODULE, HWND, LPARAM,
    LRESULT, RECT, S_FALSE, S_OK, WPARAM,
};
use windows::Win32::Graphics::Direct3D::Fxc::D3DCompile;
use windows::Win32::Graphics::Direct3D::{
    D3D_DRIVER_TYPE_HARDWARE, D3D_FEATURE_LEVEL, D3D_FEATURE_LEVEL_11_0, D3D_FEATURE_LEVEL_11_1,
    D3D_PRIMITIVE_TOPOLOGY_TRIANGLESTRIP, ID3DBlob,
};
use windows::Win32::Graphics::Direct3D11::{
    D3D11_BIND_SHADER_RESOURCE, D3D11_BIND_VERTEX_BUFFER, D3D11_BUFFER_DESC,
    D3D11_CREATE_DEVICE_BGRA_SUPPORT, D3D11_CREATE_DEVICE_FLAG, D3D11_FILTER_MIN_MAG_MIP_LINEAR,
    D3D11_INPUT_ELEMENT_DESC, D3D11_INPUT_PER_VERTEX_DATA, D3D11_SAMPLER_DESC, D3D11_SDK_VERSION,
    D3D11_SUBRESOURCE_DATA, D3D11_TEXTURE_ADDRESS_CLAMP, D3D11_TEXTURE2D_DESC, D3D11_USAGE_DEFAULT,
    D3D11_VIEWPORT, D3D11CreateDevice, ID3D11Buffer, ID3D11Device, ID3D11DeviceContext,
    ID3D11InputLayout, ID3D11PixelShader, ID3D11RenderTargetView, ID3D11SamplerState,
    ID3D11ShaderResourceView, ID3D11Texture2D, ID3D11VertexShader,
};
use windows::Win32::Graphics::Dwm::DwmFlush;
use windows::Win32::Graphics::Dxgi::Common::{
    DXGI_ALPHA_MODE_IGNORE, DXGI_COLOR_SPACE_RGB_FULL_G10_NONE_P709,
    DXGI_COLOR_SPACE_RGB_FULL_G22_NONE_P709, DXGI_COLOR_SPACE_RGB_FULL_G2084_NONE_P2020,
    DXGI_COLOR_SPACE_TYPE, DXGI_FORMAT, DXGI_FORMAT_R8_UNORM, DXGI_FORMAT_R8G8B8A8_UNORM,
    DXGI_FORMAT_R8G8B8A8_UNORM_SRGB, DXGI_FORMAT_R10G10B10A2_UNORM, DXGI_FORMAT_R16G16B16A16_FLOAT,
    DXGI_FORMAT_R16G16B16A16_UNORM, DXGI_FORMAT_R32G32_FLOAT, DXGI_SAMPLE_DESC,
};
use windows::Win32::Graphics::Dxgi::{
    DXGI_SCALING_STRETCH, DXGI_SWAP_CHAIN_COLOR_SPACE_SUPPORT_FLAG_PRESENT, DXGI_SWAP_CHAIN_DESC1,
    DXGI_SWAP_CHAIN_FLAG, DXGI_SWAP_EFFECT_FLIP_DISCARD, DXGI_USAGE_RENDER_TARGET_OUTPUT,
    IDXGIAdapter, IDXGIDevice, IDXGIFactory2, IDXGIOutput6, IDXGISwapChain1, IDXGISwapChain3,
};
use windows::Win32::Graphics::Gdi::{
    GetMonitorInfoW, HMONITOR, MONITOR_DEFAULTTONEAREST, MONITORINFO, MonitorFromWindow,
    ValidateRect,
};
use windows::Win32::Graphics::Imaging::{
    CLSID_WICImagingFactory, GUID_WICPixelFormat32bppR10G10B10A2, GUID_WICPixelFormat32bppRGBA,
    GUID_WICPixelFormat64bppRGBA, GUID_WICPixelFormat64bppRGBAHalf, IWICBitmapDecoder,
    IWICBitmapFrameDecode, IWICBitmapSource, IWICBitmapSourceTransform, IWICColorContext,
    IWICImagingFactory, IWICPalette, WICBitmapDitherTypeNone, WICBitmapPaletteTypeCustom,
    WICBitmapTransformRotate0, WICColorContextProfile, WICDecodeMetadataCacheOnLoad,
};
use windows::Win32::System::Com::{
    CLSCTX_INPROC_SERVER, COINIT_MULTITHREADED, CoCreateInstance, CoInitializeEx, CoUninitialize,
};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::UI::HiDpi::{
    DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2, SetProcessDpiAwarenessContext,
};
use windows::Win32::UI::WindowsAndMessaging::{
    CS_HREDRAW, CS_VREDRAW, CreateWindowExW, DefWindowProcW, DestroyWindow, DispatchMessageW,
    HWND_TOPMOST, IDC_ARROW, IsWindowVisible, LoadCursorW, MSG, MessageBoxW, PM_REMOVE,
    PeekMessageW, PostQuitMessage, RegisterClassW, SW_SHOW, SetCursor, SetWindowPos, ShowWindow,
    TranslateMessage, WINDOW_EX_STYLE, WINDOW_STYLE, WM_CLOSE, WM_DESTROY, WM_KEYDOWN,
    WM_LBUTTONDOWN, WM_LBUTTONUP, WM_MOUSEMOVE, WM_MOUSEWHEEL, WM_PAINT, WM_SETCURSOR, WNDCLASSW,
    WS_POPUP,
};
use windows::core::{GUID, Interface, PCSTR, PCWSTR, s};

static HELPER_EVENT_SENDER: OnceLock<Mutex<Option<mpsc::Sender<HelperEvent>>>> = OnceLock::new();
static HELPER_CURSOR_HIDDEN: OnceLock<Mutex<bool>> = OnceLock::new();

const WINDOW_CLASS: &str = "PicturiousHdrViewerWindow";
const WINDOW_TITLE: &str = "Picturious HDR Viewer";
const SC_RGB_NOMINAL_WHITE_NITS: f32 = 80.0;
const HDR_VIEWER_LOG_ENV: &str = "PICTURIOUS_HDR_VIEWER_LOG";
const GUID_WIC_PIXEL_FORMAT_8BPP_GAIN: GUID =
    GUID::from_u128(0xa884022a_af13_4c16_b746_619bf618b878);

#[repr(C)]
#[derive(Clone, Copy)]
struct Vertex {
    position: [f32; 2],
    texcoord: [f32; 2],
}

struct DecodedImage {
    width: u32,
    height: u32,
    stride: u32,
    format: DXGI_FORMAT,
    transfer: TextureTransfer,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
struct WicDecodeFormat {
    wic_format: GUID,
    dxgi_format: DXGI_FORMAT,
    bytes_per_pixel: u32,
    transfer: TextureTransfer,
}

struct ViewerImage {
    base: DecodedImage,
    gain_map: Option<DecodedImage>,
    hdr_gain_metadata: Option<HdrGainMetadata>,
    tmap_metadata: Option<TmapMetadata>,
    color_space: ImageColorSpace,
}

#[derive(Clone, Copy, Debug)]
struct HdrGainMetadata {
    headroom: f32,
    version: Option<u32>,
    source: HdrGainMetadataSource,
}

#[derive(Clone, Debug)]
struct ImageMetadata {
    hdr_gain_metadata: Option<HdrGainMetadata>,
    tmap_metadata: Option<TmapMetadata>,
    color_space: ImageColorSpace,
}

#[derive(Clone, Copy, Debug)]
enum HdrGainMetadataSource {
    AppleXmpHeadroom,
}

#[derive(Clone, Debug)]
struct TmapMetadata {
    gain_map_min_log2: f32,
    gain_map_max_log2: f32,
    gamma: f32,
    offset_sdr: f32,
    offset_hdr: f32,
    hdr_capacity_min_log2: f32,
    hdr_capacity_max_log2: f32,
    adaptive_curve: Option<AdaptiveGainCurve>,
    source: TmapMetadataSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TmapMetadataSource {
    AppleHeifTmapIso21496,
    AppleHeifHdGainMapInfo,
}

#[derive(Clone, Debug)]
struct AdaptiveGainCurve {
    nodes: Vec<AdaptiveGainCurveNode>,
}

#[derive(Clone, Copy, Debug)]
struct AdaptiveGainCurveNode {
    x: f32,
    y: f32,
    slope: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImageColorSpace {
    Srgb,
    DisplayP3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TextureTransfer {
    SrgbEncoded,
    HardwareSrgbDecoded,
    Linear,
}

#[derive(Clone, Copy)]
struct HdrShaderConfig {
    gain_mapping: Option<GainMapShaderConfig>,
    color_space: ImageColorSpace,
    source_transfer: TextureTransfer,
    sdr_white_scale: f32,
    hdr_output: bool,
}

#[derive(Clone, Copy)]
struct GainMapShaderConfig {
    mode: GainMapShaderMode,
}

#[derive(Clone, Copy)]
enum GainMapShaderMode {
    AppleLinear {
        headroom: f32,
    },
    TmapLog {
        gain_map_min_log2: f32,
        gain_map_max_log2: f32,
        gamma: f32,
        offset_sdr: f32,
        offset_hdr: f32,
        weight_factor: f32,
    },
}

#[derive(Clone, Copy, Debug)]
struct HdrDisplayInfo {
    active: bool,
    color_space: DXGI_COLOR_SPACE_TYPE,
    bits_per_color: u32,
    red_primary: [f32; 2],
    green_primary: [f32; 2],
    blue_primary: [f32; 2],
    white_point: [f32; 2],
    min_luminance_nits: f32,
    max_luminance_nits: f32,
    max_full_frame_luminance_nits: f32,
    sdr_white_level_nits: Option<f32>,
}

impl Default for HdrDisplayInfo {
    fn default() -> Self {
        Self {
            active: false,
            color_space: DXGI_COLOR_SPACE_TYPE::default(),
            bits_per_color: 0,
            red_primary: [0.0, 0.0],
            green_primary: [0.0, 0.0],
            blue_primary: [0.0, 0.0],
            white_point: [0.0, 0.0],
            min_luminance_nits: 0.0,
            max_luminance_nits: 0.0,
            max_full_frame_luminance_nits: 0.0,
            sdr_white_level_nits: None,
        }
    }
}

struct Renderer {
    hwnd: HWND,
    device: ID3D11Device,
    context: ID3D11DeviceContext,
    swap_chain: IDXGISwapChain1,
    swap_chain3: Option<IDXGISwapChain3>,
    swap_chain_format: DXGI_FORMAT,
    render_target: Option<ID3D11RenderTargetView>,
    vertex_shader: ID3D11VertexShader,
    pixel_shader: ID3D11PixelShader,
    input_layout: ID3D11InputLayout,
    sampler: ID3D11SamplerState,
    vertex_buffer: Option<ID3D11Buffer>,
    texture_view: ID3D11ShaderResourceView,
    gain_texture_view: Option<ID3D11ShaderResourceView>,
    image_width: u32,
    image_height: u32,
    target_width: u32,
    target_height: u32,
}

struct ComApartment {
    should_uninitialize: bool,
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
struct WindowBounds {
    x: i32,
    y: i32,
    width: i32,
    height: i32,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum HelperCommand {
    Show {
        path: String,
        generation: u64,
        bounds: Option<WindowBounds>,
        #[serde(default)]
        cursor_hidden: bool,
    },
    SetCursorHidden {
        hidden: bool,
    },
    Hide,
    Close,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum HelperEvent {
    Ready,
    Loaded {
        generation: u64,
        base_width: u32,
        base_height: u32,
        gain_width: Option<u32>,
        gain_height: Option<u32>,
        hdr_active: bool,
    },
    Key {
        key: String,
    },
    Wheel {
        delta: i32,
    },
    MouseMove,
    Close,
    Error {
        generation: Option<u64>,
        message: String,
    },
}

impl ComApartment {
    fn initialize() -> Result<Self> {
        let hr = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        hr.ok().context("could not initialize COM for HDR viewer")?;
        Ok(Self {
            should_uninitialize: hr == S_OK || hr == S_FALSE,
        })
    }
}

impl Drop for ComApartment {
    fn drop(&mut self) {
        if self.should_uninitialize {
            unsafe { CoUninitialize() };
        }
    }
}

fn main() {
    if let Err(error) = run() {
        let message = format!("{error:#}");
        show_error_message(&message);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    enable_per_monitor_dpi_awareness();
    if let Some(path) = image_path_from_args()? {
        return run_one_shot(path);
    }

    run_protocol()
}

fn enable_per_monitor_dpi_awareness() {
    let _ = unsafe { SetProcessDpiAwarenessContext(DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2) };
}

fn run_one_shot(path: PathBuf) -> Result<()> {
    let _com = ComApartment::initialize()?;
    let (device, context) = create_d3d_device()?;
    let preferred_monitor = active_hdr_monitor(&device).unwrap_or(None);
    let hwnd = create_fullscreen_window(preferred_monitor)?;
    let hdr_display = hdr_display_info(hwnd, &device).unwrap_or_default();
    let wic_report = wic_frame_report(&path).unwrap_or_else(|error| format!("{error:#}"));
    let image = decode_image(&path, hdr_display.active).with_context(|| {
        format!(
            "could not decode image for DirectX viewer: {}",
            path.display()
        )
    })?;
    write_diagnostic_log(&path, hdr_display, &image, &wic_report);
    let mut renderer = Renderer::new(hwnd, device, context, image, hdr_display)?;

    unsafe {
        let _ = ShowWindow(hwnd, SW_SHOW);
    }

    run_message_loop(&mut renderer)
}

fn run_protocol() -> Result<()> {
    let _com = ComApartment::initialize()?;
    let (device, context) = create_d3d_device()?;
    let hwnd = create_fullscreen_window(None)?;
    trace_transition(
        "helper_protocol_window_created",
        None,
        &format!("visible={}", unsafe { IsWindowVisible(hwnd).as_bool() }),
    );
    let (command_sender, command_receiver) = mpsc::channel::<HelperCommand>();
    let (event_sender, event_receiver) = mpsc::channel::<HelperEvent>();
    let event_sender_for_static = event_sender.clone();
    HELPER_EVENT_SENDER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .map_err(|_| anyhow::anyhow!("HDR viewer event sender is locked"))?
        .replace(event_sender_for_static);

    start_command_reader(command_sender);
    start_event_writer(event_receiver);
    let _ = event_sender.send(HelperEvent::Ready);
    trace_transition("helper_ready_sent", None, "");

    run_protocol_loop(hwnd, device, context, command_receiver, event_sender)
}

fn start_command_reader(command_sender: mpsc::Sender<HelperCommand>) {
    std::thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else {
                break;
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<HelperCommand>(trimmed) {
                Ok(command) => {
                    let is_close = matches!(command, HelperCommand::Close);
                    if command_sender.send(command).is_err() || is_close {
                        break;
                    }
                }
                Err(error) => {
                    eprintln!("could not parse HDR viewer command: {error}");
                }
            }
        }
    });
}

fn start_event_writer(event_receiver: mpsc::Receiver<HelperEvent>) {
    std::thread::spawn(move || {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        for event in event_receiver {
            if serde_json::to_writer(&mut stdout, &event).is_err() {
                break;
            }
            if writeln!(stdout).is_err() || stdout.flush().is_err() {
                break;
            }
        }
    });
}

fn run_protocol_loop(
    hwnd: HWND,
    device: ID3D11Device,
    context: ID3D11DeviceContext,
    command_receiver: mpsc::Receiver<HelperCommand>,
    event_sender: mpsc::Sender<HelperEvent>,
) -> Result<()> {
    let mut renderer = None;
    loop {
        let mut message = MSG::default();
        while unsafe { PeekMessageW(&mut message, None, 0, 0, PM_REMOVE) }.as_bool() {
            if message.message == windows::Win32::UI::WindowsAndMessaging::WM_QUIT {
                return Ok(());
            }
            unsafe {
                let _ = TranslateMessage(&message);
                DispatchMessageW(&message);
            }
        }

        while let Ok(command) = command_receiver.try_recv() {
            match command {
                HelperCommand::Show {
                    path,
                    generation,
                    bounds,
                    cursor_hidden,
                } => {
                    set_helper_cursor_hidden(cursor_hidden);
                    trace_transition(
                        "helper_show_received",
                        Some(generation),
                        &format!(
                            "visible_before={} path={}",
                            unsafe { IsWindowVisible(hwnd).as_bool() },
                            path
                        ),
                    );
                    if let Some(bounds) = bounds {
                        if let Err(error) = position_window_to_bounds(hwnd, bounds) {
                            let _ = event_sender.send(HelperEvent::Error {
                                generation: Some(generation),
                                message: format!("{error:#}"),
                            });
                            continue;
                        }
                        trace_transition(
                            "helper_window_positioned",
                            Some(generation),
                            &format!("visible={}", unsafe { IsWindowVisible(hwnd).as_bool() }),
                        );
                    }

                    match load_viewer_image(hwnd, &device, &PathBuf::from(path), generation) {
                        Ok((image, hdr_display, loaded_event)) => {
                            trace_transition(
                                "helper_decode_image_ready",
                                Some(generation),
                                &format!(
                                    "had_previous_renderer={} visible={}",
                                    renderer.is_some(),
                                    unsafe { IsWindowVisible(hwnd).as_bool() }
                                ),
                            );
                            if renderer.is_some() {
                                trace_transition(
                                    "helper_drop_previous_renderer",
                                    Some(generation),
                                    "",
                                );
                            }
                            renderer = None;
                            let mut next_renderer = match Renderer::new(
                                hwnd,
                                device.clone(),
                                context.clone(),
                                image,
                                hdr_display,
                            ) {
                                Ok(renderer) => renderer,
                                Err(error) => {
                                    unsafe {
                                        let _ = ShowWindow(
                                            hwnd,
                                            windows::Win32::UI::WindowsAndMessaging::SW_HIDE,
                                        );
                                    }
                                    let _ = event_sender.send(HelperEvent::Error {
                                        generation: Some(generation),
                                        message: format!("{error:#}"),
                                    });
                                    continue;
                                }
                            };
                            trace_transition("helper_renderer_ready", Some(generation), "");
                            if let Err(error) = next_renderer.render() {
                                unsafe {
                                    let _ = ShowWindow(
                                        hwnd,
                                        windows::Win32::UI::WindowsAndMessaging::SW_HIDE,
                                    );
                                }
                                let _ = event_sender.send(HelperEvent::Error {
                                    generation: Some(generation),
                                    message: format!("{error:#}"),
                                });
                                continue;
                            }
                            trace_transition(
                                "helper_hidden_present_done",
                                Some(generation),
                                &format!("visible={}", unsafe { IsWindowVisible(hwnd).as_bool() }),
                            );
                            renderer = Some(next_renderer);
                            unsafe {
                                let _ = ShowWindow(hwnd, SW_SHOW);
                            }
                            trace_transition(
                                "helper_show_window_done",
                                Some(generation),
                                &format!("visible={}", unsafe { IsWindowVisible(hwnd).as_bool() }),
                            );
                            if let Some(active_renderer) = renderer.as_mut() {
                                if let Err(error) = active_renderer.render() {
                                    unsafe {
                                        let _ = ShowWindow(
                                            hwnd,
                                            windows::Win32::UI::WindowsAndMessaging::SW_HIDE,
                                        );
                                    }
                                    let _ = event_sender.send(HelperEvent::Error {
                                        generation: Some(generation),
                                        message: format!("{error:#}"),
                                    });
                                    renderer = None;
                                    continue;
                                }
                            }
                            trace_transition("helper_visible_present_done", Some(generation), "");
                            let _ = unsafe { DwmFlush() };
                            trace_transition("helper_dwm_flush_done", Some(generation), "");
                            let _ = event_sender.send(loaded_event);
                            trace_transition("helper_loaded_sent", Some(generation), "");
                        }
                        Err(error) => {
                            unsafe {
                                let _ = ShowWindow(
                                    hwnd,
                                    windows::Win32::UI::WindowsAndMessaging::SW_HIDE,
                                );
                            }
                            let _ = event_sender.send(HelperEvent::Error {
                                generation: Some(generation),
                                message: format!("{error:#}"),
                            });
                        }
                    }
                }
                HelperCommand::SetCursorHidden { hidden } => {
                    set_helper_cursor_hidden(hidden);
                    trace_transition(
                        "helper_cursor_hidden_set",
                        None,
                        &format!("hidden={hidden}"),
                    );
                }
                HelperCommand::Hide => {
                    trace_transition(
                        "helper_hide_received",
                        None,
                        &format!("visible_before={}", unsafe {
                            IsWindowVisible(hwnd).as_bool()
                        }),
                    );
                    renderer = None;
                    unsafe {
                        let _ = ShowWindow(hwnd, windows::Win32::UI::WindowsAndMessaging::SW_HIDE);
                    }
                    trace_transition(
                        "helper_hide_done",
                        None,
                        &format!("visible_after={}", unsafe {
                            IsWindowVisible(hwnd).as_bool()
                        }),
                    );
                }
                HelperCommand::Close => {
                    trace_transition("helper_close_received", None, "");
                    return Ok(());
                }
            }
        }

        if let Some(renderer) = renderer.as_mut() {
            renderer.render()?;
        }
        std::thread::sleep(Duration::from_millis(16));
    }
}

fn load_viewer_image(
    hwnd: HWND,
    device: &ID3D11Device,
    path: &Path,
    generation: u64,
) -> Result<(ViewerImage, HdrDisplayInfo, HelperEvent)> {
    let hdr_display = hdr_display_info(hwnd, device).unwrap_or_default();
    let wic_report = wic_frame_report(path).unwrap_or_else(|error| format!("{error:#}"));
    let image = decode_image(path, hdr_display.active).with_context(|| {
        format!(
            "could not decode image for DirectX viewer: {}",
            path.display()
        )
    })?;
    write_diagnostic_log(path, hdr_display, &image, &wic_report);
    let loaded_event = HelperEvent::Loaded {
        generation,
        base_width: image.base.width,
        base_height: image.base.height,
        gain_width: image.gain_map.as_ref().map(|gain_map| gain_map.width),
        gain_height: image.gain_map.as_ref().map(|gain_map| gain_map.height),
        hdr_active: hdr_display.active,
    };
    Ok((image, hdr_display, loaded_event))
}

fn image_path_from_args() -> Result<Option<PathBuf>> {
    let mut args = std::env::args_os().skip(1);
    Ok(args.next().map(PathBuf::from))
}

fn create_fullscreen_window(preferred_monitor: Option<HMONITOR>) -> Result<HWND> {
    let hmodule = unsafe { GetModuleHandleW(None) }.context("could not get module handle")?;
    let hinstance = HINSTANCE(hmodule.0);
    let class_name = wide_null(WINDOW_CLASS);
    let title = wide_null(WINDOW_TITLE);
    let arrow_cursor =
        unsafe { LoadCursorW(None, IDC_ARROW) }.context("could not load arrow cursor")?;

    let wnd_class = WNDCLASSW {
        style: windows::Win32::UI::WindowsAndMessaging::WNDCLASS_STYLES(
            CS_HREDRAW.0 | CS_VREDRAW.0,
        ),
        lpfnWndProc: Some(window_proc),
        hInstance: hinstance,
        hCursor: arrow_cursor,
        lpszClassName: PCWSTR(class_name.as_ptr()),
        ..Default::default()
    };
    let atom = unsafe { RegisterClassW(&wnd_class) };
    if atom == 0 {
        bail!("could not register HDR viewer window class");
    }

    let hwnd = unsafe {
        CreateWindowExW(
            WINDOW_EX_STYLE::default(),
            PCWSTR(class_name.as_ptr()),
            PCWSTR(title.as_ptr()),
            WINDOW_STYLE(WS_POPUP.0),
            0,
            0,
            1280,
            720,
            None,
            None,
            Some(hinstance),
            None,
        )
    }
    .context("could not create HDR viewer window")?;

    let monitor = preferred_monitor
        .unwrap_or_else(|| unsafe { MonitorFromWindow(hwnd, MONITOR_DEFAULTTONEAREST) });
    let mut monitor_info = MONITORINFO {
        cbSize: size_of::<MONITORINFO>() as u32,
        ..Default::default()
    };
    if !unsafe { GetMonitorInfoW(monitor, &mut monitor_info) }.as_bool() {
        bail!("could not read monitor geometry");
    }

    position_window_to_rect(hwnd, monitor_info.rcMonitor)?;

    Ok(hwnd)
}

fn position_window_to_bounds(hwnd: HWND, bounds: WindowBounds) -> Result<()> {
    unsafe {
        SetWindowPos(
            hwnd,
            Some(HWND_TOPMOST),
            bounds.x,
            bounds.y,
            bounds.width,
            bounds.height,
            Default::default(),
        )
    }
    .context("could not position HDR viewer window")
}

fn position_window_to_rect(hwnd: HWND, rect: RECT) -> Result<()> {
    position_window_to_bounds(
        hwnd,
        WindowBounds {
            x: rect.left,
            y: rect.top,
            width: rect.right - rect.left,
            height: rect.bottom - rect.top,
        },
    )
}

unsafe extern "system" fn window_proc(
    hwnd: HWND,
    message: u32,
    wparam: WPARAM,
    lparam: LPARAM,
) -> LRESULT {
    match message {
        WM_CLOSE => {
            let _ = unsafe { DestroyWindow(hwnd) };
            LRESULT(0)
        }
        WM_DESTROY => {
            unsafe { PostQuitMessage(0) };
            LRESULT(0)
        }
        WM_KEYDOWN => {
            let vk = wparam.0 as u32;
            if vk == 0x20 && key_is_repeat(lparam) {
                return LRESULT(0);
            }
            if let Some(key) = key_name(vk) {
                if emit_helper_event(HelperEvent::Key { key }) {
                    return LRESULT(0);
                }
            }
            if vk == 0x1b || vk == b'Q' as u32 {
                let _ = unsafe { DestroyWindow(hwnd) };
                return LRESULT(0);
            }
            unsafe { DefWindowProcW(hwnd, message, wparam, lparam) }
        }
        WM_MOUSEWHEEL => {
            let delta = wheel_delta(wparam);
            if delta != 0 && emit_helper_event(HelperEvent::Wheel { delta }) {
                return LRESULT(0);
            }
            unsafe { DefWindowProcW(hwnd, message, wparam, lparam) }
        }
        WM_MOUSEMOVE => {
            if helper_cursor_hidden() {
                set_helper_cursor_hidden(false);
            }
            let _ = emit_helper_event(HelperEvent::MouseMove);
            unsafe { DefWindowProcW(hwnd, message, wparam, lparam) }
        }
        WM_LBUTTONDOWN => {
            if is_close_hotspot(hwnd, lparam) && emit_helper_event(HelperEvent::Close) {
                return LRESULT(0);
            }
            unsafe { DefWindowProcW(hwnd, message, wparam, lparam) }
        }
        WM_LBUTTONUP => {
            if is_close_hotspot(hwnd, lparam) && emit_helper_event(HelperEvent::Close) {
                return LRESULT(0);
            }
            unsafe { DefWindowProcW(hwnd, message, wparam, lparam) }
        }
        WM_SETCURSOR => {
            if helper_cursor_hidden() {
                unsafe {
                    SetCursor(None);
                }
                return LRESULT(1);
            }
            if let Ok(cursor) = unsafe { LoadCursorW(None, IDC_ARROW) } {
                unsafe {
                    SetCursor(Some(cursor));
                }
                return LRESULT(1);
            }
            unsafe { DefWindowProcW(hwnd, message, wparam, lparam) }
        }
        WM_PAINT => {
            unsafe {
                let _ = ValidateRect(Some(hwnd), None);
            }
            LRESULT(0)
        }
        _ => unsafe { DefWindowProcW(hwnd, message, wparam, lparam) },
    }
}

fn key_name(vk: u32) -> Option<String> {
    let key = match vk {
        0x1b => "Escape",
        0x20 => " ",
        0x24 => "Home",
        0x25 => "ArrowLeft",
        0x27 => "ArrowRight",
        0x52 => "r",
        _ => return None,
    };
    Some(key.to_owned())
}

fn key_is_repeat(lparam: LPARAM) -> bool {
    ((lparam.0 as u32) & (1 << 30)) != 0
}

fn wheel_delta(wparam: WPARAM) -> i32 {
    (((wparam.0 >> 16) & 0xffff) as u16 as i16) as i32
}

fn is_close_hotspot(hwnd: HWND, lparam: LPARAM) -> bool {
    let (x, y) = lparam_point(lparam);
    let mut rect = RECT::default();
    if unsafe { windows::Win32::UI::WindowsAndMessaging::GetClientRect(hwnd, &mut rect) }.is_err() {
        return false;
    }
    let width = rect.right - rect.left;
    x >= width - 160 && y <= 128 && x >= 0 && y >= 0
}

fn lparam_point(lparam: LPARAM) -> (i32, i32) {
    let raw = lparam.0 as u32;
    (
        (raw & 0xffff) as u16 as i16 as i32,
        ((raw >> 16) & 0xffff) as u16 as i16 as i32,
    )
}

fn emit_helper_event(event: HelperEvent) -> bool {
    let Some(sender) = HELPER_EVENT_SENDER.get() else {
        return false;
    };
    let Ok(guard) = sender.lock() else {
        return false;
    };
    let Some(sender) = guard.as_ref() else {
        return false;
    };
    sender.send(event).is_ok()
}

fn set_helper_cursor_hidden(hidden: bool) {
    if let Ok(mut guard) = HELPER_CURSOR_HIDDEN
        .get_or_init(|| Mutex::new(false))
        .lock()
    {
        *guard = hidden;
    }
    if hidden {
        unsafe {
            SetCursor(None);
        }
    } else if let Ok(cursor) = unsafe { LoadCursorW(None, IDC_ARROW) } {
        unsafe {
            SetCursor(Some(cursor));
        }
    }
}

fn helper_cursor_hidden() -> bool {
    HELPER_CURSOR_HIDDEN
        .get_or_init(|| Mutex::new(false))
        .lock()
        .map(|guard| *guard)
        .unwrap_or(false)
}

fn run_message_loop(renderer: &mut Renderer) -> Result<()> {
    loop {
        let mut message = MSG::default();
        while unsafe { PeekMessageW(&mut message, None, 0, 0, PM_REMOVE) }.as_bool() {
            if message.message == windows::Win32::UI::WindowsAndMessaging::WM_QUIT {
                return Ok(());
            }
            unsafe {
                let _ = TranslateMessage(&message);
                DispatchMessageW(&message);
            }
        }

        renderer.render()?;
        std::thread::sleep(Duration::from_millis(16));
    }
}

impl Renderer {
    fn new(
        hwnd: HWND,
        device: ID3D11Device,
        context: ID3D11DeviceContext,
        image: ViewerImage,
        hdr_display: HdrDisplayInfo,
    ) -> Result<Self> {
        let (swap_chain_format, swap_chain, swap_chain3) =
            create_viewer_swap_chain(hwnd, &device, hdr_display.active)?;
        trace_transition(
            "helper_swap_chain_ready",
            None,
            &format!("format={swap_chain_format:?}"),
        );

        let requested_gain_metadata = if hdr_display.active {
            image.hdr_gain_metadata
        } else {
            None
        };
        let texture_view = create_image_texture(&device, &image.base)?;
        let gain_texture_view = if requested_gain_metadata.is_some() {
            image
                .gain_map
                .as_ref()
                .map(|gain_map| create_image_texture(&device, gain_map))
                .transpose()?
        } else {
            None
        };
        let gain_mapping = if gain_texture_view.is_some() {
            gain_map_shader_config(
                requested_gain_metadata,
                image.tmap_metadata.as_ref(),
                hdr_display,
            )
        } else {
            None
        };
        let shader_config = if hdr_display.active || image.color_space == ImageColorSpace::DisplayP3
        {
            Some(HdrShaderConfig {
                gain_mapping,
                color_space: image.color_space,
                source_transfer: image.base.transfer,
                sdr_white_scale: if hdr_display.active {
                    hdr_display
                        .sdr_white_level_nits
                        .unwrap_or(SC_RGB_NOMINAL_WHITE_NITS)
                        / SC_RGB_NOMINAL_WHITE_NITS
                } else {
                    1.0
                },
                hdr_output: hdr_display.active,
            })
        } else {
            None
        };
        let (vertex_shader, pixel_shader, input_layout) = create_shaders(&device, shader_config)?;
        let sampler = create_sampler(&device)?;

        let mut renderer = Self {
            hwnd,
            device,
            context,
            swap_chain,
            swap_chain3,
            swap_chain_format,
            render_target: None,
            vertex_shader,
            pixel_shader,
            input_layout,
            sampler,
            vertex_buffer: None,
            texture_view,
            gain_texture_view,
            image_width: image.base.width,
            image_height: image.base.height,
            target_width: 0,
            target_height: 0,
        };
        renderer.resize_targets()?;
        Ok(renderer)
    }

    fn render(&mut self) -> Result<()> {
        let (width, height) = client_size(self.hwnd)?;
        if width == 0 || height == 0 {
            return Ok(());
        }
        if width != self.target_width || height != self.target_height {
            self.resize_targets()?;
        }

        let rtv = self
            .render_target
            .as_ref()
            .context("render target is not available")?;
        let viewport = D3D11_VIEWPORT {
            TopLeftX: 0.0,
            TopLeftY: 0.0,
            Width: self.target_width as f32,
            Height: self.target_height as f32,
            MinDepth: 0.0,
            MaxDepth: 1.0,
        };
        let render_targets = [Some(rtv.clone())];
        let samplers = [Some(self.sampler.clone())];
        let vertex_buffer = self
            .vertex_buffer
            .as_ref()
            .context("vertex buffer is not available")?
            .clone();
        let vertex_buffers = [Some(vertex_buffer)];
        let strides = [size_of::<Vertex>() as u32];
        let offsets = [0_u32];

        unsafe {
            self.context.RSSetViewports(Some(&[viewport]));
            self.context.OMSetRenderTargets(Some(&render_targets), None);
            self.context
                .ClearRenderTargetView(rtv, &[0.0, 0.0, 0.0, 1.0]);
            self.context.IASetInputLayout(&self.input_layout);
            self.context
                .IASetPrimitiveTopology(D3D_PRIMITIVE_TOPOLOGY_TRIANGLESTRIP);
            self.context.IASetVertexBuffers(
                0,
                1,
                Some(vertex_buffers.as_ptr()),
                Some(strides.as_ptr()),
                Some(offsets.as_ptr()),
            );
            self.context.VSSetShader(&self.vertex_shader, None);
            self.context.PSSetShader(&self.pixel_shader, None);
            if let Some(gain_texture_view) = &self.gain_texture_view {
                let shader_resources = [
                    Some(self.texture_view.clone()),
                    Some(gain_texture_view.clone()),
                ];
                self.context
                    .PSSetShaderResources(0, Some(&shader_resources));
            } else {
                let shader_resources = [Some(self.texture_view.clone())];
                self.context
                    .PSSetShaderResources(0, Some(&shader_resources));
            }
            self.context.PSSetSamplers(0, Some(&samplers));
            self.context.Draw(4, 0);
            self.swap_chain.Present(1, Default::default()).ok()?;
        }

        Ok(())
    }

    fn resize_targets(&mut self) -> Result<()> {
        let (width, height) = client_size(self.hwnd)?;
        if width == 0 || height == 0 {
            return Ok(());
        }

        self.render_target = None;
        unsafe {
            self.context.ClearState();
            self.swap_chain.ResizeBuffers(
                0,
                width,
                height,
                self.swap_chain_format,
                DXGI_SWAP_CHAIN_FLAG::default(),
            )?;
        }
        if let Some(chain) = &self.swap_chain3 {
            set_swap_chain_color_space(chain, swap_chain_color_space(self.swap_chain_format))?;
        }

        let back_buffer: ID3D11Texture2D = unsafe { self.swap_chain.GetBuffer(0) }
            .context("could not get DirectX swap chain back buffer")?;
        let mut render_target = None;
        unsafe {
            self.device
                .CreateRenderTargetView(&back_buffer, None, Some(&mut render_target))
        }
        .context("could not create DirectX render target")?;

        self.render_target = render_target;
        self.vertex_buffer = Some(create_vertex_buffer(
            &self.device,
            self.image_width,
            self.image_height,
            width,
            height,
        )?);
        self.target_width = width;
        self.target_height = height;
        Ok(())
    }
}

fn gain_map_shader_config(
    hdr_gain_metadata: Option<HdrGainMetadata>,
    tmap_metadata: Option<&TmapMetadata>,
    hdr_display: HdrDisplayInfo,
) -> Option<GainMapShaderConfig> {
    if let Some(tmap_metadata) = tmap_metadata {
        return Some(GainMapShaderConfig {
            mode: GainMapShaderMode::TmapLog {
                gain_map_min_log2: tmap_metadata.gain_map_min_log2,
                gain_map_max_log2: tmap_metadata.gain_map_max_log2,
                gamma: tmap_metadata.gamma,
                offset_sdr: tmap_metadata.offset_sdr,
                offset_hdr: tmap_metadata.offset_hdr,
                weight_factor: tmap_weight_factor(tmap_metadata, hdr_display),
            },
        });
    }

    hdr_gain_metadata.map(|metadata| GainMapShaderConfig {
        mode: GainMapShaderMode::AppleLinear {
            headroom: metadata.headroom,
        },
    })
}

fn tmap_weight_factor(metadata: &TmapMetadata, hdr_display: HdrDisplayInfo) -> f32 {
    let display_boost = hdr_display
        .sdr_white_level_nits
        .filter(|white| *white > 0.0)
        .map(|white| hdr_display.max_luminance_nits.max(white) / white)
        .unwrap_or(1.0)
        .max(1.0);
    let denominator = metadata.hdr_capacity_max_log2 - metadata.hdr_capacity_min_log2;
    if denominator <= f32::EPSILON {
        return 1.0;
    }

    ((display_boost.log2() - metadata.hdr_capacity_min_log2) / denominator).clamp(0.0, 1.0)
}

fn create_d3d_device() -> Result<(ID3D11Device, ID3D11DeviceContext)> {
    let feature_levels = [D3D_FEATURE_LEVEL_11_1, D3D_FEATURE_LEVEL_11_0];
    let mut device = None;
    let mut context = None;
    let mut feature_level = D3D_FEATURE_LEVEL::default();
    unsafe {
        D3D11CreateDevice(
            None,
            D3D_DRIVER_TYPE_HARDWARE,
            HMODULE::default(),
            D3D11_CREATE_DEVICE_FLAG(D3D11_CREATE_DEVICE_BGRA_SUPPORT.0),
            Some(&feature_levels),
            D3D11_SDK_VERSION,
            Some(&mut device),
            Some(&mut feature_level),
            Some(&mut context),
        )
    }
    .context("could not create Direct3D 11 hardware device")?;

    Ok((
        device.context("Direct3D returned no device")?,
        context.context("Direct3D returned no immediate context")?,
    ))
}

fn hdr_display_info(hwnd: HWND, device: &ID3D11Device) -> Result<HdrDisplayInfo> {
    let monitor = unsafe { MonitorFromWindow(hwnd, MONITOR_DEFAULTTONEAREST) };
    let dxgi_device: IDXGIDevice = device.cast().context("could not query DXGI device")?;
    let adapter =
        unsafe { dxgi_device.GetAdapter() }.context("could not get DirectX adapter for HDR")?;

    let mut output_index = 0;
    while let Ok(output) = unsafe { adapter.EnumOutputs(output_index) } {
        let desc = unsafe { output.GetDesc() }.context("could not read DirectX output")?;
        if desc.Monitor == monitor {
            let output6: IDXGIOutput6 = output.cast().context("could not query DXGI output 6")?;
            let desc1 = unsafe { output6.GetDesc1() }.context("could not read HDR output info")?;
            return Ok(HdrDisplayInfo {
                active: desc1.ColorSpace == DXGI_COLOR_SPACE_RGB_FULL_G2084_NONE_P2020,
                color_space: desc1.ColorSpace,
                bits_per_color: desc1.BitsPerColor,
                red_primary: desc1.RedPrimary,
                green_primary: desc1.GreenPrimary,
                blue_primary: desc1.BluePrimary,
                white_point: desc1.WhitePoint,
                min_luminance_nits: desc1.MinLuminance,
                max_luminance_nits: desc1.MaxLuminance,
                max_full_frame_luminance_nits: desc1.MaxFullFrameLuminance,
                sdr_white_level_nits: sdr_white_level_nits_for_display_name(&desc1.DeviceName),
            });
        }
        output_index += 1;
    }

    Ok(HdrDisplayInfo::default())
}

fn sdr_white_level_nits_for_display_name(display_name: &[u16]) -> Option<f32> {
    let display_name = wide_array_to_string(display_name)?;
    let paths = active_display_paths().ok()?;

    for path in paths {
        let Some(source_name) = displayconfig_source_device_name(&path) else {
            continue;
        };
        if source_name.eq_ignore_ascii_case(&display_name) {
            return displayconfig_sdr_white_level_nits(&path).ok().flatten();
        }
    }

    None
}

fn active_display_paths() -> Result<Vec<DISPLAYCONFIG_PATH_INFO>> {
    loop {
        let mut path_count = 0_u32;
        let mut mode_count = 0_u32;
        let status = unsafe {
            GetDisplayConfigBufferSizes(QDC_ONLY_ACTIVE_PATHS, &mut path_count, &mut mode_count)
        };
        if status != ERROR_SUCCESS {
            bail!("could not query active display path count: {status:?}");
        }

        let mut paths = vec![DISPLAYCONFIG_PATH_INFO::default(); path_count as usize];
        let mut modes = vec![DISPLAYCONFIG_MODE_INFO::default(); mode_count as usize];
        let status = unsafe {
            QueryDisplayConfig(
                QDC_ONLY_ACTIVE_PATHS,
                &mut path_count,
                paths.as_mut_ptr(),
                &mut mode_count,
                modes.as_mut_ptr(),
                None,
            )
        };
        if status == ERROR_SUCCESS {
            paths.truncate(path_count as usize);
            return Ok(paths);
        }
        if status != ERROR_INSUFFICIENT_BUFFER {
            bail!("could not query active display paths: {status:?}");
        }
    }
}

fn displayconfig_source_device_name(path: &DISPLAYCONFIG_PATH_INFO) -> Option<String> {
    let mut source_name = DISPLAYCONFIG_SOURCE_DEVICE_NAME::default();
    source_name.header = DISPLAYCONFIG_DEVICE_INFO_HEADER {
        r#type: DISPLAYCONFIG_DEVICE_INFO_GET_SOURCE_NAME,
        size: size_of::<DISPLAYCONFIG_SOURCE_DEVICE_NAME>() as u32,
        adapterId: path.sourceInfo.adapterId,
        id: path.sourceInfo.id,
    };

    let status = unsafe { DisplayConfigGetDeviceInfo(&mut source_name.header) };
    if status != ERROR_SUCCESS.0 as i32 {
        return None;
    }

    wide_array_to_string(&source_name.viewGdiDeviceName)
}

fn displayconfig_sdr_white_level_nits(path: &DISPLAYCONFIG_PATH_INFO) -> Result<Option<f32>> {
    let mut white_level = DISPLAYCONFIG_SDR_WHITE_LEVEL::default();
    white_level.header = DISPLAYCONFIG_DEVICE_INFO_HEADER {
        r#type: DISPLAYCONFIG_DEVICE_INFO_GET_SDR_WHITE_LEVEL,
        size: size_of::<DISPLAYCONFIG_SDR_WHITE_LEVEL>() as u32,
        adapterId: path.targetInfo.adapterId,
        id: path.targetInfo.id,
    };

    let status = unsafe { DisplayConfigGetDeviceInfo(&mut white_level.header) };
    if status != ERROR_SUCCESS.0 as i32 {
        return Ok(None);
    }
    if white_level.SDRWhiteLevel == 0 {
        return Ok(None);
    }

    Ok(Some(
        white_level.SDRWhiteLevel as f32 / 1000.0 * SC_RGB_NOMINAL_WHITE_NITS,
    ))
}

fn active_hdr_monitor(device: &ID3D11Device) -> Result<Option<HMONITOR>> {
    let dxgi_device: IDXGIDevice = device.cast().context("could not query DXGI device")?;
    let adapter =
        unsafe { dxgi_device.GetAdapter() }.context("could not get DirectX adapter for HDR")?;

    let mut output_index = 0;
    while let Ok(output) = unsafe { adapter.EnumOutputs(output_index) } {
        let Ok(output6) = output.cast::<IDXGIOutput6>() else {
            output_index += 1;
            continue;
        };
        let desc1 = unsafe { output6.GetDesc1() }.context("could not read HDR output info")?;
        if desc1.ColorSpace == DXGI_COLOR_SPACE_RGB_FULL_G2084_NONE_P2020 {
            return Ok(Some(desc1.Monitor));
        }
        output_index += 1;
    }

    Ok(None)
}

fn create_swap_chain(
    hwnd: HWND,
    device: &ID3D11Device,
    format: DXGI_FORMAT,
) -> Result<IDXGISwapChain1> {
    let dxgi_device: IDXGIDevice = device.cast().context("could not query DXGI device")?;
    let adapter: IDXGIAdapter =
        unsafe { dxgi_device.GetAdapter() }.context("could not get DXGI adapter")?;
    let factory: IDXGIFactory2 =
        unsafe { adapter.GetParent() }.context("could not get DXGI factory")?;
    let desc = DXGI_SWAP_CHAIN_DESC1 {
        Width: 0,
        Height: 0,
        Format: format,
        Stereo: false.into(),
        SampleDesc: DXGI_SAMPLE_DESC {
            Count: 1,
            Quality: 0,
        },
        BufferUsage: DXGI_USAGE_RENDER_TARGET_OUTPUT,
        BufferCount: 2,
        Scaling: DXGI_SCALING_STRETCH,
        SwapEffect: DXGI_SWAP_EFFECT_FLIP_DISCARD,
        AlphaMode: DXGI_ALPHA_MODE_IGNORE,
        Flags: 0,
    };
    unsafe { factory.CreateSwapChainForHwnd(device, hwnd, &desc, None, None) }
        .context("could not create DirectX swap chain")
}

fn create_viewer_swap_chain(
    hwnd: HWND,
    device: &ID3D11Device,
    hdr_active: bool,
) -> Result<(DXGI_FORMAT, IDXGISwapChain1, Option<IDXGISwapChain3>)> {
    let formats = if hdr_active {
        &[DXGI_FORMAT_R16G16B16A16_FLOAT][..]
    } else {
        &[DXGI_FORMAT_R10G10B10A2_UNORM, DXGI_FORMAT_R8G8B8A8_UNORM][..]
    };
    let mut errors = Vec::new();
    for format in formats {
        match create_swap_chain(hwnd, device, *format).and_then(|swap_chain| {
            let swap_chain3 = swap_chain.cast::<IDXGISwapChain3>().ok();
            if let Some(chain) = &swap_chain3 {
                set_swap_chain_color_space(chain, swap_chain_color_space(*format))?;
            }
            Ok((*format, swap_chain, swap_chain3))
        }) {
            Ok(swap_chain) => return Ok(swap_chain),
            Err(error) => errors.push(format!("{format:?}: {error:#}")),
        }
    }

    bail!(
        "could not create DirectX swap chain with supported format: {}",
        errors.join("; ")
    )
}

fn swap_chain_color_space(format: DXGI_FORMAT) -> DXGI_COLOR_SPACE_TYPE {
    if format == DXGI_FORMAT_R16G16B16A16_FLOAT {
        DXGI_COLOR_SPACE_RGB_FULL_G10_NONE_P709
    } else {
        DXGI_COLOR_SPACE_RGB_FULL_G22_NONE_P709
    }
}

fn set_swap_chain_color_space(
    chain: &IDXGISwapChain3,
    color_space: DXGI_COLOR_SPACE_TYPE,
) -> Result<()> {
    let support = unsafe { chain.CheckColorSpaceSupport(color_space) }.with_context(|| {
        format!("could not query DirectX color space support for {color_space:?}")
    })?;
    if support & DXGI_SWAP_CHAIN_COLOR_SPACE_SUPPORT_FLAG_PRESENT.0 as u32 == 0 {
        bail!("DirectX swap chain does not support presenting {color_space:?}");
    }

    unsafe { chain.SetColorSpace1(color_space) }
        .with_context(|| format!("could not set DirectX swap chain color space to {color_space:?}"))
}

fn create_image_texture(
    device: &ID3D11Device,
    image: &DecodedImage,
) -> Result<ID3D11ShaderResourceView> {
    let desc = D3D11_TEXTURE2D_DESC {
        Width: image.width,
        Height: image.height,
        MipLevels: 1,
        ArraySize: 1,
        Format: image.format,
        SampleDesc: DXGI_SAMPLE_DESC {
            Count: 1,
            Quality: 0,
        },
        Usage: D3D11_USAGE_DEFAULT,
        BindFlags: D3D11_BIND_SHADER_RESOURCE.0 as u32,
        CPUAccessFlags: 0,
        MiscFlags: 0,
    };
    let initial_data = D3D11_SUBRESOURCE_DATA {
        pSysMem: image.bytes.as_ptr() as *const c_void,
        SysMemPitch: image.stride,
        SysMemSlicePitch: image.stride * image.height,
    };
    let mut texture = None;
    unsafe { device.CreateTexture2D(&desc, Some(&initial_data), Some(&mut texture)) }
        .context("could not upload image to DirectX texture")?;
    let texture = texture.context("DirectX returned no image texture")?;

    let mut view = None;
    unsafe { device.CreateShaderResourceView(&texture, None, Some(&mut view)) }
        .context("could not create DirectX image texture view")?;
    view.context("DirectX returned no image texture view")
}

fn create_vertex_buffer(
    device: &ID3D11Device,
    image_width: u32,
    image_height: u32,
    target_width: u32,
    target_height: u32,
) -> Result<ID3D11Buffer> {
    let vertices = fitted_quad_vertices(image_width, image_height, target_width, target_height);
    let desc = D3D11_BUFFER_DESC {
        ByteWidth: (vertices.len() * size_of::<Vertex>()) as u32,
        Usage: D3D11_USAGE_DEFAULT,
        BindFlags: D3D11_BIND_VERTEX_BUFFER.0 as u32,
        CPUAccessFlags: 0,
        MiscFlags: 0,
        StructureByteStride: 0,
    };
    let initial_data = D3D11_SUBRESOURCE_DATA {
        pSysMem: vertices.as_ptr() as *const c_void,
        SysMemPitch: 0,
        SysMemSlicePitch: 0,
    };
    let mut buffer = None;
    unsafe { device.CreateBuffer(&desc, Some(&initial_data), Some(&mut buffer)) }
        .context("could not create DirectX vertex buffer")?;
    buffer.context("DirectX returned no vertex buffer")
}

fn fitted_quad_vertices(
    image_width: u32,
    image_height: u32,
    target_width: u32,
    target_height: u32,
) -> [Vertex; 4] {
    let image_aspect = image_width as f32 / image_height.max(1) as f32;
    let target_aspect = target_width as f32 / target_height.max(1) as f32;
    let (x, y) = if target_aspect > image_aspect {
        (image_aspect / target_aspect, 1.0)
    } else {
        (1.0, target_aspect / image_aspect)
    };

    [
        Vertex {
            position: [-x, y],
            texcoord: [0.0, 0.0],
        },
        Vertex {
            position: [x, y],
            texcoord: [1.0, 0.0],
        },
        Vertex {
            position: [-x, -y],
            texcoord: [0.0, 1.0],
        },
        Vertex {
            position: [x, -y],
            texcoord: [1.0, 1.0],
        },
    ]
}

fn create_shaders(
    device: &ID3D11Device,
    hdr_config: Option<HdrShaderConfig>,
) -> Result<(ID3D11VertexShader, ID3D11PixelShader, ID3D11InputLayout)> {
    let vertex_blob = compile_shader(VERTEX_SHADER, s!("main"), s!("vs_5_0"))
        .context("could not compile DirectX vertex shader")?;
    let pixel_shader_source = hdr_config
        .map(hdr_pixel_shader)
        .unwrap_or_else(|| PIXEL_SHADER.to_string());
    let pixel_blob = compile_shader(&pixel_shader_source, s!("main"), s!("ps_5_0"))
        .context("could not compile DirectX pixel shader")?;
    let vertex_bytes = blob_bytes(&vertex_blob);
    let pixel_bytes = blob_bytes(&pixel_blob);

    let mut vertex_shader = None;
    unsafe { device.CreateVertexShader(vertex_bytes, None, Some(&mut vertex_shader)) }
        .context("could not create DirectX vertex shader")?;
    let mut pixel_shader = None;
    unsafe { device.CreatePixelShader(pixel_bytes, None, Some(&mut pixel_shader)) }
        .context("could not create DirectX pixel shader")?;

    let input_elements = [
        D3D11_INPUT_ELEMENT_DESC {
            SemanticName: s!("POSITION"),
            SemanticIndex: 0,
            Format: DXGI_FORMAT_R32G32_FLOAT,
            InputSlot: 0,
            AlignedByteOffset: 0,
            InputSlotClass: D3D11_INPUT_PER_VERTEX_DATA,
            InstanceDataStepRate: 0,
        },
        D3D11_INPUT_ELEMENT_DESC {
            SemanticName: s!("TEXCOORD"),
            SemanticIndex: 0,
            Format: DXGI_FORMAT_R32G32_FLOAT,
            InputSlot: 0,
            AlignedByteOffset: 8,
            InputSlotClass: D3D11_INPUT_PER_VERTEX_DATA,
            InstanceDataStepRate: 0,
        },
    ];
    let mut input_layout = None;
    unsafe { device.CreateInputLayout(&input_elements, vertex_bytes, Some(&mut input_layout)) }
        .context("could not create DirectX input layout")?;

    Ok((
        vertex_shader.context("DirectX returned no vertex shader")?,
        pixel_shader.context("DirectX returned no pixel shader")?,
        input_layout.context("DirectX returned no input layout")?,
    ))
}

fn hdr_pixel_shader(config: HdrShaderConfig) -> String {
    let color_transform = match config.color_space {
        ImageColorSpace::Srgb => "",
        ImageColorSpace::DisplayP3 => "    color = displayP3ToScRgb(color);\n",
    };
    let source_decode = match config.source_transfer {
        TextureTransfer::SrgbEncoded => "    float3 color = srgbToLinear(base.rgb);\n",
        TextureTransfer::HardwareSrgbDecoded | TextureTransfer::Linear => {
            "    float3 color = base.rgb;\n"
        }
    };
    let gain_sampling = config
        .gain_mapping
        .map(|gain_mapping| match gain_mapping.mode {
            GainMapShaderMode::AppleLinear { headroom } => {
                format!(
                    "    float gain = linearizeRec709(gainTexture.Sample(imageSampler, texcoord).r);\n    float multiplier = 1.0 + (({headroom:.6}) - 1.0) * gain;\n    color *= multiplier;\n"
                )
            }
            GainMapShaderMode::TmapLog {
                gain_map_min_log2,
                gain_map_max_log2,
                gamma,
                offset_sdr,
                offset_hdr,
                weight_factor,
            } => {
                format!(
                    "    float recovery = saturate(linearizeRec709(gainTexture.Sample(imageSampler, texcoord).r));\n    float logRecovery = pow(max(recovery, 0.0), {:.6});\n    float logBoost = lerp({:.6}, {:.6}, logRecovery);\n    float multiplier = exp2(logBoost * {:.6});\n    color = max((color + {:.8}) * multiplier - {:.8}, 0.0);\n",
                    1.0 / gamma.max(0.0001),
                    gain_map_min_log2,
                    gain_map_max_log2,
                    weight_factor,
                    offset_sdr,
                    offset_hdr
                )
            }
        })
        .unwrap_or_default();
    let output_encode = if config.hdr_output {
        format!("    color *= {:.6};\n", config.sdr_white_scale)
    } else {
        "    color = linearToSrgb(saturate(color));\n".to_owned()
    };
    format!(
        r#"
Texture2D imageTexture : register(t0);
Texture2D gainTexture : register(t1);
SamplerState imageSampler : register(s0);

float srgbToLinearChannel(float value) {{
    return value <= 0.04045 ? value / 12.92 : pow((value + 0.055) / 1.055, 2.4);
}}

float3 srgbToLinear(float3 value) {{
    return float3(
        srgbToLinearChannel(value.r),
        srgbToLinearChannel(value.g),
        srgbToLinearChannel(value.b)
    );
}}

float linearToSrgbChannel(float value) {{
    return value <= 0.0031308 ? value * 12.92 : 1.055 * pow(value, 1.0 / 2.4) - 0.055;
}}

float3 linearToSrgb(float3 value) {{
    return float3(
        linearToSrgbChannel(value.r),
        linearToSrgbChannel(value.g),
        linearToSrgbChannel(value.b)
    );
}}

float linearizeRec709(float value) {{
    return value < 0.081 ? value / 4.5 : pow((value + 0.099) / 1.099, 1.0 / 0.45);
}}

float3 displayP3ToScRgb(float3 p3) {{
    return float3(
        1.22494018 * p3.r - 0.22494018 * p3.g + 0.00000000 * p3.b,
       -0.04205695 * p3.r + 1.04205695 * p3.g + 0.00000000 * p3.b,
       -0.01963755 * p3.r - 0.07863605 * p3.g + 1.09827360 * p3.b
    );
}}

float4 main(float4 position : SV_POSITION, float2 texcoord : TEXCOORD0) : SV_TARGET {{
    float4 base = imageTexture.Sample(imageSampler, texcoord);
{source_decode}{gain_sampling}{color_transform}{output_encode}    return float4(color, base.a);
}}
"#,
        source_decode = source_decode,
        gain_sampling = gain_sampling,
        color_transform = color_transform,
        output_encode = output_encode
    )
}

fn create_sampler(device: &ID3D11Device) -> Result<ID3D11SamplerState> {
    let desc = D3D11_SAMPLER_DESC {
        Filter: D3D11_FILTER_MIN_MAG_MIP_LINEAR,
        AddressU: D3D11_TEXTURE_ADDRESS_CLAMP,
        AddressV: D3D11_TEXTURE_ADDRESS_CLAMP,
        AddressW: D3D11_TEXTURE_ADDRESS_CLAMP,
        MaxLOD: f32::MAX,
        ..Default::default()
    };
    let mut sampler = None;
    unsafe { device.CreateSamplerState(&desc, Some(&mut sampler)) }
        .context("could not create DirectX sampler")?;
    sampler.context("DirectX returned no sampler")
}

fn compile_shader(source: &str, entry: PCSTR, target: PCSTR) -> Result<ID3DBlob> {
    let mut code = None;
    let mut errors = None;
    unsafe {
        D3DCompile(
            source.as_ptr() as *const c_void,
            source.len(),
            PCSTR::null(),
            None,
            None,
            entry,
            target,
            0,
            0,
            &mut code,
            Some(&mut errors),
        )
    }
    .map_err(|error| anyhow::anyhow!("shader compile failed: {error}; {}", blob_message(errors)))?;
    code.context("shader compiler returned no bytecode")
}

fn blob_bytes(blob: &ID3DBlob) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(blob.GetBufferPointer() as *const u8, blob.GetBufferSize())
    }
}

fn blob_message(blob: Option<ID3DBlob>) -> String {
    let Some(blob) = blob else {
        return String::new();
    };
    String::from_utf8_lossy(blob_bytes(&blob)).to_string()
}

fn decode_image(path: &Path, hdr_active: bool) -> Result<ViewerImage> {
    let metadata = image_metadata_from_file(path);
    let hdr_gain_metadata = metadata.hdr_gain_metadata;
    let color_space = metadata.color_space;
    let tmap_metadata = metadata.tmap_metadata;

    if hdr_active {
        let base = decode_first_available_wic_image(path, hdr_base_decode_formats())?;
        let mut gain_map = if hdr_gain_metadata.is_some() {
            decode_gain_map(path).unwrap_or(None)
        } else {
            None
        };
        if let Some(gain_map) = gain_map.as_mut() {
            upsample_gain_map_to_base_size(&base, gain_map);
        }
        return Ok(ViewerImage {
            base,
            gain_map,
            hdr_gain_metadata,
            tmap_metadata,
            color_space,
        });
    }

    let base = decode_sdr_base_image(path)?;
    Ok(ViewerImage {
        base,
        gain_map: None,
        hdr_gain_metadata,
        tmap_metadata,
        color_space,
    })
}

fn hdr_base_decode_formats() -> &'static [WicDecodeFormat] {
    &[
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat32bppR10G10B10A2,
            dxgi_format: DXGI_FORMAT_R10G10B10A2_UNORM,
            bytes_per_pixel: 4,
            transfer: TextureTransfer::SrgbEncoded,
        },
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat64bppRGBA,
            dxgi_format: DXGI_FORMAT_R16G16B16A16_UNORM,
            bytes_per_pixel: 8,
            transfer: TextureTransfer::SrgbEncoded,
        },
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat32bppRGBA,
            dxgi_format: DXGI_FORMAT_R8G8B8A8_UNORM_SRGB,
            bytes_per_pixel: 4,
            transfer: TextureTransfer::HardwareSrgbDecoded,
        },
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat64bppRGBAHalf,
            dxgi_format: DXGI_FORMAT_R16G16B16A16_FLOAT,
            bytes_per_pixel: 8,
            transfer: TextureTransfer::Linear,
        },
    ]
}

fn decode_sdr_base_image(path: &Path) -> Result<DecodedImage> {
    decode_first_available_wic_image(path, sdr_base_decode_formats())
}

fn decode_first_available_wic_image(
    path: &Path,
    formats: &[WicDecodeFormat],
) -> Result<DecodedImage> {
    let mut errors = Vec::new();
    for format in formats {
        match decode_wic_image(
            path,
            format.wic_format,
            format.dxgi_format,
            format.bytes_per_pixel,
            format.transfer,
            false,
        ) {
            Ok(image) => return Ok(image),
            Err(error) => errors.push(format!("{:?}: {error:#}", format.dxgi_format)),
        }
    }

    bail!(
        "could not decode image into a supported DirectX texture format: {}",
        errors.join("; ")
    )
}

fn sdr_base_decode_formats() -> &'static [WicDecodeFormat] {
    &[
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat32bppR10G10B10A2,
            dxgi_format: DXGI_FORMAT_R10G10B10A2_UNORM,
            bytes_per_pixel: 4,
            transfer: TextureTransfer::SrgbEncoded,
        },
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat64bppRGBA,
            dxgi_format: DXGI_FORMAT_R16G16B16A16_UNORM,
            bytes_per_pixel: 8,
            transfer: TextureTransfer::SrgbEncoded,
        },
        WicDecodeFormat {
            wic_format: GUID_WICPixelFormat32bppRGBA,
            dxgi_format: DXGI_FORMAT_R8G8B8A8_UNORM,
            bytes_per_pixel: 4,
            transfer: TextureTransfer::SrgbEncoded,
        },
    ]
}

fn image_metadata_from_file(path: &Path) -> ImageMetadata {
    let Ok(bytes) = fs::read(path) else {
        return ImageMetadata {
            hdr_gain_metadata: None,
            tmap_metadata: None,
            color_space: ImageColorSpace::Srgb,
        };
    };
    let text = String::from_utf8_lossy(&bytes);
    let hdr_gain_metadata = apple_hdr_gain_metadata_from_text(&text);
    let tmap_metadata = hdr_gain_metadata
        .and_then(|metadata| apple_tmap_metadata_from_heif_bytes(&bytes, metadata.headroom));
    let color_space = image_color_space_from_wic(path)
        .ok()
        .flatten()
        .unwrap_or_else(|| image_color_space_from_metadata_bytes(&bytes));

    ImageMetadata {
        hdr_gain_metadata,
        tmap_metadata,
        color_space,
    }
}

fn apple_hdr_gain_metadata_from_text(text: &str) -> Option<HdrGainMetadata> {
    let version =
        extract_xml_u32_any(text, &["HDRGainMap:HDRGainMapVersion", "HDRGainMapVersion"])?;
    let headroom = extract_xml_f32_any(
        text,
        &["HDRGainMap:HDRGainMapHeadroom", "HDRGainMapHeadroom"],
    )?;
    if !headroom.is_finite() || headroom <= 1.0 {
        return None;
    }

    Some(HdrGainMetadata {
        headroom,
        version: Some(version),
        source: HdrGainMetadataSource::AppleXmpHeadroom,
    })
}

fn apple_tmap_metadata_from_heif_bytes(bytes: &[u8], headroom: f32) -> Option<TmapMetadata> {
    if !headroom.is_finite() || headroom <= 1.0 {
        return None;
    }

    let mut hd_gain_map_metadata = None;
    let mut colr_payloads = Vec::new();
    collect_isobmff_box_payloads(bytes, 0, bytes.len(), *b"colr", &mut colr_payloads);
    for payload in colr_payloads {
        let Some(icc_profile) = payload.strip_prefix(b"prof") else {
            continue;
        };
        let Some(hd_gain_map_info) = icc_tag(icc_profile, *b"hdgm") else {
            continue;
        };
        if !icc_profile_is_pq_display_p3(icc_profile) {
            continue;
        }
        hd_gain_map_metadata = parse_hd_gain_map_info(hd_gain_map_info, headroom);
        break;
    }

    let mut metadata = parse_iso21496_tmap_metadata_from_heif_bytes(bytes, headroom)
        .or_else(|| hd_gain_map_metadata.clone())?;
    if metadata.adaptive_curve.is_none() {
        metadata.adaptive_curve = hd_gain_map_metadata.and_then(|metadata| metadata.adaptive_curve);
    }
    Some(metadata)
}

#[derive(Clone, Copy, Debug)]
struct Iso21496GainMapMetadata {
    base_hdr_headroom_log2: f32,
    alternate_hdr_headroom_log2: f32,
    gain_map_min_log2: f32,
    gain_map_max_log2: f32,
    gamma: f32,
    base_offset: f32,
    alternate_offset: f32,
    use_base_color_space: bool,
}

fn parse_iso21496_tmap_metadata_from_heif_bytes(
    bytes: &[u8],
    headroom: f32,
) -> Option<TmapMetadata> {
    let mut idat_payloads = Vec::new();
    collect_isobmff_box_payloads(bytes, 0, bytes.len(), *b"idat", &mut idat_payloads);
    parse_iso21496_tmap_metadata_from_payloads(&idat_payloads, headroom)
}

fn parse_iso21496_tmap_metadata_from_payloads(
    payloads: &[&[u8]],
    headroom: f32,
) -> Option<TmapMetadata> {
    let expected_headroom_log2 = headroom.log2();
    for payload in payloads {
        if payload.len() < 61 {
            continue;
        }
        for offset in 0..=payload.len() - 61 {
            let Some(metadata) = parse_iso21496_gain_map_metadata(&payload[offset..]) else {
                continue;
            };
            if !iso21496_gain_map_metadata_matches(&metadata, expected_headroom_log2) {
                continue;
            }
            return Some(iso21496_tmap_metadata(metadata));
        }
    }

    None
}

fn parse_iso21496_gain_map_metadata(bytes: &[u8]) -> Option<Iso21496GainMapMetadata> {
    const MULTI_CHANNEL_FLAG: u8 = 1 << 7;
    const USE_BASE_COLOR_SPACE_FLAG: u8 = 1 << 6;
    const SINGLE_CHANNEL_METADATA_LEN: usize = 61;

    if bytes.len() < SINGLE_CHANNEL_METADATA_LEN || read_u16_be_at(bytes, 0)? != 0 {
        return None;
    }

    let flags = *bytes.get(4)?;
    if flags & MULTI_CHANNEL_FLAG != 0 {
        return None;
    }

    let base_hdr_headroom_log2 = read_positive_rational_be_at(bytes, 5)?;
    let alternate_hdr_headroom_log2 = read_positive_rational_be_at(bytes, 13)?;
    let gain_map_min_log2 = read_rational_be_at(bytes, 21)?;
    let gain_map_max_log2 = read_rational_be_at(bytes, 29)?;
    let gamma = read_positive_rational_be_at(bytes, 37)?;
    let base_offset = read_rational_be_at(bytes, 45)?;
    let alternate_offset = read_rational_be_at(bytes, 53)?;

    Some(Iso21496GainMapMetadata {
        base_hdr_headroom_log2,
        alternate_hdr_headroom_log2,
        gain_map_min_log2,
        gain_map_max_log2,
        gamma,
        base_offset,
        alternate_offset,
        use_base_color_space: flags & USE_BASE_COLOR_SPACE_FLAG != 0,
    })
}

fn iso21496_gain_map_metadata_matches(
    metadata: &Iso21496GainMapMetadata,
    expected_headroom_log2: f32,
) -> bool {
    let max_headroom_log2 = metadata
        .base_hdr_headroom_log2
        .max(metadata.alternate_hdr_headroom_log2);
    metadata.base_hdr_headroom_log2.is_finite()
        && metadata.alternate_hdr_headroom_log2.is_finite()
        && metadata.gain_map_min_log2.is_finite()
        && metadata.gain_map_max_log2.is_finite()
        && metadata.gamma.is_finite()
        && metadata.base_offset.is_finite()
        && metadata.alternate_offset.is_finite()
        && metadata.gamma > 0.0
        && metadata.use_base_color_space
        && metadata.gain_map_max_log2 >= metadata.gain_map_min_log2
        && max_headroom_log2 > 0.0
        && (max_headroom_log2 - expected_headroom_log2).abs() < 0.05
}

fn iso21496_tmap_metadata(metadata: Iso21496GainMapMetadata) -> TmapMetadata {
    let base_is_sdr = metadata.base_hdr_headroom_log2 < metadata.alternate_hdr_headroom_log2;
    let hdr_capacity_min_log2 = metadata
        .base_hdr_headroom_log2
        .min(metadata.alternate_hdr_headroom_log2);
    let hdr_capacity_max_log2 = metadata
        .base_hdr_headroom_log2
        .max(metadata.alternate_hdr_headroom_log2);

    TmapMetadata {
        gain_map_min_log2: metadata.gain_map_min_log2,
        gain_map_max_log2: metadata.gain_map_max_log2,
        gamma: metadata.gamma,
        offset_sdr: if base_is_sdr {
            metadata.base_offset
        } else {
            metadata.alternate_offset
        },
        offset_hdr: if base_is_sdr {
            metadata.alternate_offset
        } else {
            metadata.base_offset
        },
        hdr_capacity_min_log2,
        hdr_capacity_max_log2,
        adaptive_curve: None,
        source: TmapMetadataSource::AppleHeifTmapIso21496,
    }
}

fn collect_isobmff_box_payloads<'a>(
    bytes: &'a [u8],
    start: usize,
    end: usize,
    box_type: [u8; 4],
    out: &mut Vec<&'a [u8]>,
) {
    let mut offset = start;
    while offset.checked_add(8).is_some_and(|value| value <= end) {
        let Some(size32) = read_u32_be_at(bytes, offset) else {
            return;
        };
        let Some(kind) = bytes.get(offset + 4..offset + 8) else {
            return;
        };
        let mut header_size = 8_usize;
        let mut size = size32 as usize;
        if size32 == 1 {
            let Some(size64) = read_u64_be_at(bytes, offset + 8) else {
                return;
            };
            let Ok(size64) = usize::try_from(size64) else {
                return;
            };
            size = size64;
            header_size = 16;
        } else if size32 == 0 {
            size = end.saturating_sub(offset);
        }

        let Some(box_end) = offset.checked_add(size) else {
            return;
        };
        if size < header_size || box_end > end {
            return;
        }

        if kind == box_type {
            out.push(&bytes[offset + header_size..box_end]);
        }

        if is_isobmff_container(kind) {
            let child_start = offset
                .checked_add(header_size)
                .and_then(|value| value.checked_add(if kind == b"meta" { 4 } else { 0 }))
                .unwrap_or(box_end);
            if child_start <= box_end {
                collect_isobmff_box_payloads(bytes, child_start, box_end, box_type, out);
            }
        }

        offset = box_end;
    }
}

fn is_isobmff_container(kind: &[u8]) -> bool {
    matches!(
        kind,
        b"meta" | b"iprp" | b"ipco" | b"iinf" | b"iref" | b"grpl"
    )
}

fn icc_tag(profile: &[u8], signature: [u8; 4]) -> Option<&[u8]> {
    let tag_count = read_u32_be_at(profile, 128)? as usize;
    let tag_table_end = 132_usize.checked_add(tag_count.checked_mul(12)?)?;
    if profile.len() < tag_table_end {
        return None;
    }

    for index in 0..tag_count {
        let offset = 132 + index * 12;
        if profile.get(offset..offset + 4)? != signature {
            continue;
        }
        let tag_offset = read_u32_be_at(profile, offset + 4)? as usize;
        let tag_size = read_u32_be_at(profile, offset + 8)? as usize;
        let tag_end = tag_offset.checked_add(tag_size)?;
        return profile.get(tag_offset..tag_end);
    }

    None
}

fn icc_profile_is_pq_display_p3(profile: &[u8]) -> bool {
    let Some(cicp) = icc_tag(profile, *b"cicp") else {
        return bytes_contain_ascii_or_utf16(profile, "Display P3 Primaries; PQ");
    };
    cicp.len() >= 12 && cicp[8] == 12 && cicp[9] == 16
}

fn parse_hd_gain_map_info(bytes: &[u8], headroom: f32) -> Option<TmapMetadata> {
    if bytes.len() < 16 || bytes.get(0..4)? != b"gmap" {
        return None;
    }

    let headroom_log2 = headroom.log2();
    Some(TmapMetadata {
        gain_map_min_log2: 0.0,
        gain_map_max_log2: headroom_log2,
        gamma: 1.0,
        offset_sdr: 0.0,
        offset_hdr: 0.0,
        hdr_capacity_min_log2: 0.0,
        hdr_capacity_max_log2: headroom_log2,
        adaptive_curve: parse_adaptive_gain_curve(bytes),
        source: TmapMetadataSource::AppleHeifHdGainMapInfo,
    })
}

fn parse_adaptive_gain_curve(bytes: &[u8]) -> Option<AdaptiveGainCurve> {
    let mut best_nodes = None;
    for count_offset in 8..bytes.len().saturating_sub(4) {
        let count = read_u32_be_at(bytes, count_offset)? as usize;
        if !(2..=16).contains(&count) {
            continue;
        }
        let nodes_offset = count_offset + 4;
        let nodes_len = count.checked_mul(12)?;
        if nodes_offset
            .checked_add(nodes_len)
            .map_or(true, |end| end > bytes.len())
        {
            continue;
        }

        let mut nodes = Vec::with_capacity(count);
        for index in 0..count {
            let offset = nodes_offset + index * 12;
            let x = read_f32_be_at(bytes, offset)?;
            let y = read_f32_be_at(bytes, offset + 4)?;
            let slope = read_f32_be_at(bytes, offset + 8)?;
            nodes.push(AdaptiveGainCurveNode { x, y, slope });
        }
        if adaptive_gain_curve_nodes_are_plausible(&nodes) {
            best_nodes = Some(nodes);
        }
    }

    best_nodes.map(|nodes| AdaptiveGainCurve { nodes })
}

fn adaptive_gain_curve_nodes_are_plausible(nodes: &[AdaptiveGainCurveNode]) -> bool {
    let mut previous_x = -f32::EPSILON;
    nodes.iter().all(|node| {
        let plausible = node.x.is_finite()
            && node.y.is_finite()
            && node.slope.is_finite()
            && node.x >= previous_x
            && (-0.001..=1.001).contains(&node.x)
            && (-0.001..=1.5).contains(&node.y)
            && (-16.0..=16.0).contains(&node.slope);
        previous_x = node.x;
        plausible
    })
}

fn read_u16_be_at(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32_be_at(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64_be_at(bytes: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_be_bytes(
        bytes.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn read_rational_be_at(bytes: &[u8], offset: usize) -> Option<f32> {
    let numerator = read_i32_be_at(bytes, offset)? as f64;
    let denominator = read_u32_be_at(bytes, offset + 4)? as f64;
    if denominator == 0.0 {
        return None;
    }
    Some((numerator / denominator) as f32)
}

fn read_positive_rational_be_at(bytes: &[u8], offset: usize) -> Option<f32> {
    let numerator = read_u32_be_at(bytes, offset)? as f64;
    let denominator = read_u32_be_at(bytes, offset + 4)? as f64;
    if denominator == 0.0 {
        return None;
    }
    Some((numerator / denominator) as f32)
}

fn read_i32_be_at(bytes: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_be_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_f32_be_at(bytes: &[u8], offset: usize) -> Option<f32> {
    Some(f32::from_bits(read_u32_be_at(bytes, offset)?))
}

fn image_color_space_from_wic(path: &Path) -> Result<Option<ImageColorSpace>> {
    let factory = create_wic_factory()?;
    let frame = open_wic_frame(&factory, path)?;
    let mut context_count = 0_u32;
    let _ = unsafe { frame.GetColorContexts(&mut [], &mut context_count) };
    if context_count == 0 {
        return Ok(None);
    }

    let mut contexts = (0..context_count)
        .map(|_| unsafe { factory.CreateColorContext() }.map(Some))
        .collect::<windows::core::Result<Vec<_>>>()
        .context("could not create WIC color contexts")?;
    let mut actual_count = 0_u32;
    unsafe { frame.GetColorContexts(&mut contexts, &mut actual_count) }
        .context("could not read WIC color contexts")?;

    for context in contexts.into_iter().take(actual_count as usize).flatten() {
        if color_context_is_display_p3(&context)? {
            return Ok(Some(ImageColorSpace::DisplayP3));
        }
    }

    Ok(None)
}

fn color_context_is_display_p3(context: &IWICColorContext) -> Result<bool> {
    if unsafe { context.GetType() }.context("could not read WIC color context type")?
        != WICColorContextProfile
    {
        return Ok(false);
    }

    let mut byte_count = 0_u32;
    let _ = unsafe { context.GetProfileBytes(&mut [], &mut byte_count) };
    if byte_count == 0 {
        return Ok(false);
    }

    let mut bytes = vec![0_u8; byte_count as usize];
    unsafe { context.GetProfileBytes(&mut bytes, &mut byte_count) }
        .context("could not read WIC ICC profile")?;
    Ok(bytes_contain_ascii_or_utf16(&bytes, "Display P3")
        || bytes_contain_ascii_or_utf16(&bytes, "DisplayP3"))
}

fn image_color_space_from_metadata_bytes(bytes: &[u8]) -> ImageColorSpace {
    if bytes_contain_ascii_or_utf16(bytes, "Display P3")
        || bytes_contain_ascii_or_utf16(bytes, "DisplayP3")
    {
        ImageColorSpace::DisplayP3
    } else {
        ImageColorSpace::Srgb
    }
}

fn bytes_contain_ascii_or_utf16(bytes: &[u8], needle: &str) -> bool {
    let needle = needle.as_bytes();
    bytes.windows(needle.len()).any(|window| window == needle)
        || bytes_contain_utf16(bytes, needle, Utf16ByteOrder::LittleEndian)
        || bytes_contain_utf16(bytes, needle, Utf16ByteOrder::BigEndian)
}

#[derive(Clone, Copy)]
enum Utf16ByteOrder {
    LittleEndian,
    BigEndian,
}

fn bytes_contain_utf16(bytes: &[u8], needle: &[u8], byte_order: Utf16ByteOrder) -> bool {
    let encoded_len = needle.len().saturating_mul(2);
    if needle.is_empty() || bytes.len() < encoded_len {
        return false;
    }

    bytes.windows(encoded_len).any(|window| {
        needle.iter().enumerate().all(|(index, byte)| {
            let pair = &window[index * 2..index * 2 + 2];
            match byte_order {
                Utf16ByteOrder::LittleEndian => pair[0] == *byte && pair[1] == 0,
                Utf16ByteOrder::BigEndian => pair[0] == 0 && pair[1] == *byte,
            }
        })
    })
}

fn extract_xml_f32_any(text: &str, names: &[&str]) -> Option<f32> {
    names
        .iter()
        .find_map(|name| extract_xml_value(text, name))
        .and_then(|value| value.parse::<f32>().ok())
}

fn extract_xml_u32_any(text: &str, names: &[&str]) -> Option<u32> {
    names
        .iter()
        .find_map(|name| extract_xml_value(text, name))
        .and_then(|value| value.parse::<u32>().ok())
}

fn extract_xml_value(text: &str, name: &str) -> Option<String> {
    for (index, _) in text.match_indices(name) {
        if text[..index].chars().next_back() == Some('/') {
            continue;
        }

        let after = &text[index + name.len()..];
        let after_trimmed = after.trim_start();
        if let Some(value) = extract_xml_attribute_value(after_trimmed) {
            return Some(value);
        }

        let tag_end = after.find('>')?;
        let content = &after[tag_end + 1..];
        let value_end = content.find('<')?;
        let value = content[..value_end].trim();
        if !value.is_empty() {
            return Some(value.to_owned());
        }
    }

    None
}

fn extract_xml_attribute_value(after_name: &str) -> Option<String> {
    let after_equals = after_name.strip_prefix('=')?.trim_start();
    let mut chars = after_equals.chars();
    let quote = chars.next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = quote.len_utf8();
    let value_end = after_equals[value_start..].find(quote)? + value_start;
    let value = after_equals[value_start..value_end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn decode_gain_map(path: &Path) -> Result<Option<DecodedImage>> {
    let factory = create_wic_factory()?;
    let decoder = open_wic_decoder(&factory, path)?;
    let frame_count = unsafe { decoder.GetFrameCount() }.context("could not read frame count")?;
    for index in 0..frame_count {
        let frame = unsafe { decoder.GetFrame(index) }
            .with_context(|| format!("could not read image frame {index}"))?;
        if let Ok(image) = decode_wic_frame_image(
            &factory,
            &frame,
            GUID_WIC_PIXEL_FORMAT_8BPP_GAIN,
            DXGI_FORMAT_R8_UNORM,
            1,
            TextureTransfer::SrgbEncoded,
            true,
        ) {
            return Ok(Some(image));
        }
    }

    Ok(None)
}

fn upsample_gain_map_to_base_size(base: &DecodedImage, gain_map: &mut DecodedImage) {
    if gain_map.width == base.width && gain_map.height == base.height {
        return;
    }
    if gain_map.format != DXGI_FORMAT_R8_UNORM {
        return;
    }
    let Some(bytes) = upscale_r8_bilinear(gain_map, base.width, base.height) else {
        return;
    };

    gain_map.width = base.width;
    gain_map.height = base.height;
    gain_map.stride = base.width;
    gain_map.bytes = bytes;
}

#[derive(Clone, Copy, Debug)]
struct ColorDiagnostics {
    sample_count: u64,
    sample_step: u32,
    base_out_of_srgb_percent: f32,
    base_max_positive_srgb_overshoot: f32,
    base_max_negative_srgb_overshoot: f32,
    hdr_above_sdr_white_percent: f32,
    hdr_max_sc_rgb_channel: f32,
    gain_linear_min: f32,
    gain_linear_avg: f32,
    gain_linear_max: f32,
}

fn color_diagnostics(image: &ViewerImage) -> Option<ColorDiagnostics> {
    if image.base.width == 0 || image.base.height == 0 {
        return None;
    }

    let area = u64::from(image.base.width) * u64::from(image.base.height);
    let sample_step = ((area as f64 / 250_000.0).sqrt().ceil() as u32).max(1);
    let mut sample_count = 0_u64;
    let mut base_out_of_srgb_count = 0_u64;
    let mut hdr_above_sdr_white_count = 0_u64;
    let mut base_max_positive_srgb_overshoot = 0.0_f32;
    let mut base_max_negative_srgb_overshoot = 0.0_f32;
    let mut hdr_max_sc_rgb_channel = 0.0_f32;
    let mut gain_linear_min = f32::INFINITY;
    let mut gain_linear_sum = 0.0_f64;
    let mut gain_linear_max = 0.0_f32;
    let mut gain_sample_count = 0_u64;

    for y in (0..image.base.height).step_by(sample_step as usize) {
        for x in (0..image.base.width).step_by(sample_step as usize) {
            let mut color = decoded_linear_rgb_at(&image.base, x, y)?;
            if image.color_space == ImageColorSpace::DisplayP3 {
                color = display_p3_to_sc_rgb(color);
            }

            sample_count += 1;
            let positive_overshoot = color.iter().map(|value| value - 1.0).fold(0.0, f32::max);
            let negative_overshoot = color.iter().map(|value| -value).fold(0.0, f32::max);
            base_max_positive_srgb_overshoot =
                base_max_positive_srgb_overshoot.max(positive_overshoot);
            base_max_negative_srgb_overshoot =
                base_max_negative_srgb_overshoot.max(negative_overshoot);
            if positive_overshoot > 0.001 || negative_overshoot > 0.001 {
                base_out_of_srgb_count += 1;
            }

            let mut hdr_color = color;
            if let (Some(gain_map), Some(metadata)) = (&image.gain_map, image.hdr_gain_metadata) {
                let gain = gain_map_linear_at(gain_map, x, y)?;
                let multiplier = if let Some(tmap_metadata) = &image.tmap_metadata {
                    let log_recovery = gain.powf(1.0 / tmap_metadata.gamma.max(0.0001));
                    let log_boost = tmap_metadata.gain_map_min_log2
                        + (tmap_metadata.gain_map_max_log2 - tmap_metadata.gain_map_min_log2)
                            * log_recovery;
                    2.0_f32.powf(log_boost)
                } else {
                    1.0 + (metadata.headroom - 1.0) * gain
                };
                hdr_color = [
                    ((hdr_color[0] + image.tmap_metadata.as_ref().map_or(0.0, |m| m.offset_sdr))
                        * multiplier
                        - image.tmap_metadata.as_ref().map_or(0.0, |m| m.offset_hdr))
                    .max(0.0),
                    ((hdr_color[1] + image.tmap_metadata.as_ref().map_or(0.0, |m| m.offset_sdr))
                        * multiplier
                        - image.tmap_metadata.as_ref().map_or(0.0, |m| m.offset_hdr))
                    .max(0.0),
                    ((hdr_color[2] + image.tmap_metadata.as_ref().map_or(0.0, |m| m.offset_sdr))
                        * multiplier
                        - image.tmap_metadata.as_ref().map_or(0.0, |m| m.offset_hdr))
                    .max(0.0),
                ];
                gain_linear_min = gain_linear_min.min(gain);
                gain_linear_sum += f64::from(gain);
                gain_linear_max = gain_linear_max.max(gain);
                gain_sample_count += 1;
            }

            let max_channel = hdr_color.iter().copied().fold(f32::NEG_INFINITY, f32::max);
            hdr_max_sc_rgb_channel = hdr_max_sc_rgb_channel.max(max_channel);
            if max_channel > 1.001 {
                hdr_above_sdr_white_count += 1;
            }
        }
    }

    if sample_count == 0 {
        return None;
    }

    Some(ColorDiagnostics {
        sample_count,
        sample_step,
        base_out_of_srgb_percent: base_out_of_srgb_count as f32 / sample_count as f32 * 100.0,
        base_max_positive_srgb_overshoot,
        base_max_negative_srgb_overshoot,
        hdr_above_sdr_white_percent: hdr_above_sdr_white_count as f32 / sample_count as f32 * 100.0,
        hdr_max_sc_rgb_channel,
        gain_linear_min: if gain_sample_count == 0 {
            0.0
        } else {
            gain_linear_min
        },
        gain_linear_avg: if gain_sample_count == 0 {
            0.0
        } else {
            (gain_linear_sum / gain_sample_count as f64) as f32
        },
        gain_linear_max,
    })
}

fn decoded_linear_rgb_at(image: &DecodedImage, x: u32, y: u32) -> Option<[f32; 3]> {
    let encoded = decoded_rgb_at(image, x, y)?;
    match image.transfer {
        TextureTransfer::SrgbEncoded | TextureTransfer::HardwareSrgbDecoded => Some([
            srgb_to_linear_channel(encoded[0]),
            srgb_to_linear_channel(encoded[1]),
            srgb_to_linear_channel(encoded[2]),
        ]),
        TextureTransfer::Linear => Some(encoded),
    }
}

fn decoded_rgb_at(image: &DecodedImage, x: u32, y: u32) -> Option<[f32; 3]> {
    if x >= image.width || y >= image.height {
        return None;
    }
    let bytes_per_pixel = dxgi_bytes_per_pixel(image.format)?;
    let offset = usize::try_from(y)
        .ok()?
        .checked_mul(usize::try_from(image.stride).ok()?)?
        .checked_add(usize::try_from(x).ok()?.checked_mul(bytes_per_pixel)?)?;
    let pixel = image
        .bytes
        .get(offset..offset.checked_add(bytes_per_pixel)?)?;

    if image.format == DXGI_FORMAT_R8G8B8A8_UNORM || image.format == DXGI_FORMAT_R8G8B8A8_UNORM_SRGB
    {
        return Some([
            pixel[0] as f32 / 255.0,
            pixel[1] as f32 / 255.0,
            pixel[2] as f32 / 255.0,
        ]);
    }
    if image.format == DXGI_FORMAT_R10G10B10A2_UNORM {
        let value = u32::from_le_bytes(pixel.try_into().ok()?);
        return Some([
            (value & 0x3ff) as f32 / 1023.0,
            ((value >> 10) & 0x3ff) as f32 / 1023.0,
            ((value >> 20) & 0x3ff) as f32 / 1023.0,
        ]);
    }
    if image.format == DXGI_FORMAT_R16G16B16A16_UNORM {
        return Some([
            u16::from_le_bytes(pixel[0..2].try_into().ok()?) as f32 / 65535.0,
            u16::from_le_bytes(pixel[2..4].try_into().ok()?) as f32 / 65535.0,
            u16::from_le_bytes(pixel[4..6].try_into().ok()?) as f32 / 65535.0,
        ]);
    }
    if image.format == DXGI_FORMAT_R16G16B16A16_FLOAT {
        return Some([
            f16_to_f32(u16::from_le_bytes(pixel[0..2].try_into().ok()?)),
            f16_to_f32(u16::from_le_bytes(pixel[2..4].try_into().ok()?)),
            f16_to_f32(u16::from_le_bytes(pixel[4..6].try_into().ok()?)),
        ]);
    }

    None
}

fn dxgi_bytes_per_pixel(format: DXGI_FORMAT) -> Option<usize> {
    if format == DXGI_FORMAT_R8G8B8A8_UNORM
        || format == DXGI_FORMAT_R8G8B8A8_UNORM_SRGB
        || format == DXGI_FORMAT_R10G10B10A2_UNORM
    {
        Some(4)
    } else if format == DXGI_FORMAT_R16G16B16A16_UNORM || format == DXGI_FORMAT_R16G16B16A16_FLOAT {
        Some(8)
    } else {
        None
    }
}

fn gain_map_linear_at(gain_map: &DecodedImage, x: u32, y: u32) -> Option<f32> {
    if gain_map.format != DXGI_FORMAT_R8_UNORM || x >= gain_map.width || y >= gain_map.height {
        return None;
    }
    let offset = usize::try_from(y)
        .ok()?
        .checked_mul(usize::try_from(gain_map.stride).ok()?)?
        .checked_add(usize::try_from(x).ok()?)?;
    let encoded = *gain_map.bytes.get(offset)? as f32 / 255.0;
    Some(linearize_rec709_channel(encoded))
}

fn srgb_to_linear_channel(value: f32) -> f32 {
    if value <= 0.04045 {
        value / 12.92
    } else {
        ((value + 0.055) / 1.055).powf(2.4)
    }
}

fn linearize_rec709_channel(value: f32) -> f32 {
    if value < 0.081 {
        value / 4.5
    } else {
        ((value + 0.099) / 1.099).powf(1.0 / 0.45)
    }
}

fn display_p3_to_sc_rgb(p3: [f32; 3]) -> [f32; 3] {
    [
        1.22494018 * p3[0] - 0.22494018 * p3[1],
        -0.04205695 * p3[0] + 1.04205695 * p3[1],
        -0.01963755 * p3[0] - 0.07863605 * p3[1] + 1.09827360 * p3[2],
    ]
}

fn f16_to_f32(value: u16) -> f32 {
    let sign = ((value >> 15) & 0x1) as u32;
    let exponent = ((value >> 10) & 0x1f) as i32;
    let fraction = (value & 0x03ff) as u32;
    let sign_factor = if sign == 0 { 1.0 } else { -1.0 };

    match exponent {
        0 if fraction == 0 => sign_factor * 0.0,
        0 => sign_factor * (fraction as f32 / 1024.0) * 2_f32.powi(-14),
        31 if fraction == 0 => sign_factor * f32::INFINITY,
        31 => f32::NAN,
        _ => sign_factor * (1.0 + fraction as f32 / 1024.0) * 2_f32.powi(exponent - 15),
    }
}

fn upscale_r8_bilinear(
    source: &DecodedImage,
    target_width: u32,
    target_height: u32,
) -> Option<Vec<u8>> {
    if source.width == 0 || source.height == 0 || target_width == 0 || target_height == 0 {
        return None;
    }
    let expected_source_len = usize::try_from(source.stride)
        .ok()?
        .checked_mul(usize::try_from(source.height).ok()?)?;
    if source.bytes.len() < expected_source_len {
        return None;
    }

    let target_len = usize::try_from(target_width)
        .ok()?
        .checked_mul(usize::try_from(target_height).ok()?)?;
    let mut target = vec![0_u8; target_len];
    let scale_x = source.width as f32 / target_width as f32;
    let scale_y = source.height as f32 / target_height as f32;
    let source_stride = source.stride as usize;

    for y in 0..target_height {
        let source_y = ((y as f32 + 0.5) * scale_y - 0.5).max(0.0);
        let y0 = source_y.floor() as u32;
        let y1 = (y0 + 1).min(source.height - 1);
        let weight_y = source_y - y0 as f32;

        for x in 0..target_width {
            let source_x = ((x as f32 + 0.5) * scale_x - 0.5).max(0.0);
            let x0 = source_x.floor() as u32;
            let x1 = (x0 + 1).min(source.width - 1);
            let weight_x = source_x - x0 as f32;

            let top_left = source.bytes[y0 as usize * source_stride + x0 as usize] as f32;
            let top_right = source.bytes[y0 as usize * source_stride + x1 as usize] as f32;
            let bottom_left = source.bytes[y1 as usize * source_stride + x0 as usize] as f32;
            let bottom_right = source.bytes[y1 as usize * source_stride + x1 as usize] as f32;
            let top = top_left + (top_right - top_left) * weight_x;
            let bottom = bottom_left + (bottom_right - bottom_left) * weight_x;
            target[y as usize * target_width as usize + x as usize] =
                (top + (bottom - top) * weight_y).round().clamp(0.0, 255.0) as u8;
        }
    }

    Some(target)
}

fn wic_frame_report(path: &Path) -> Result<String> {
    let factory = create_wic_factory()?;
    let decoder = open_wic_decoder(&factory, path)?;
    let frame_count = unsafe { decoder.GetFrameCount() }.context("could not read frame count")?;
    let mut report = String::new();
    let _ = writeln!(report, "wic_frame_count={frame_count}");

    for index in 0..frame_count {
        let frame = unsafe { decoder.GetFrame(index) }
            .with_context(|| format!("could not read image frame {index}"))?;
        let mut width = 0_u32;
        let mut height = 0_u32;
        unsafe { frame.GetSize(&mut width, &mut height) }
            .with_context(|| format!("could not read image frame {index} size"))?;
        let source: IWICBitmapSource = frame.cast()?;
        let pixel_format = unsafe { source.GetPixelFormat() }
            .with_context(|| format!("could not read image frame {index} pixel format"))?;
        let _ = writeln!(
            report,
            "frame[{index}] size={width}x{height} pixel_format={pixel_format:?}"
        );

        if let Ok(transform) = frame.cast::<IWICBitmapSourceTransform>() {
            let mut closest_width = width;
            let mut closest_height = height;
            let closest_size_result =
                unsafe { transform.GetClosestSize(&mut closest_width, &mut closest_height) };
            let _ = writeln!(
                report,
                "frame[{index}] transform closest_size_ok={} closest_size={closest_width}x{closest_height}",
                closest_size_result.is_ok(),
            );
            for (label, candidate) in [
                ("r10g10b10a2", GUID_WICPixelFormat32bppR10G10B10A2),
                ("rgba64", GUID_WICPixelFormat64bppRGBA),
                ("rgba32", GUID_WICPixelFormat32bppRGBA),
                ("rgba64half", GUID_WICPixelFormat64bppRGBAHalf),
                ("gain8", GUID_WIC_PIXEL_FORMAT_8BPP_GAIN),
            ] {
                let mut closest_format = candidate;
                let closest_format_result =
                    unsafe { transform.GetClosestPixelFormat(&mut closest_format) };
                let _ = writeln!(
                    report,
                    "frame[{index}] transform candidate={label} closest_format_ok={} closest_format={closest_format:?}",
                    closest_format_result.is_ok(),
                );
            }
        }
    }

    Ok(report)
}

fn write_diagnostic_log(
    path: &Path,
    hdr_display: HdrDisplayInfo,
    image: &ViewerImage,
    wic_report: &str,
) {
    let Some(log_path) = hdr_viewer_log_path_from_env() else {
        return;
    };

    let mut message = String::new();
    let _ = writeln!(message, "path={}", path.display());
    let _ = writeln!(
        message,
        "hdr_active={} display_color_space={:?} display_bits_per_color={} display_min_luminance_nits={} display_max_luminance_nits={} display_max_full_frame_luminance_nits={} display_sdr_white_level_nits={:?} sc_rgb_sdr_white_scale={}",
        hdr_display.active,
        hdr_display.color_space,
        hdr_display.bits_per_color,
        hdr_display.min_luminance_nits,
        hdr_display.max_luminance_nits,
        hdr_display.max_full_frame_luminance_nits,
        hdr_display.sdr_white_level_nits,
        hdr_display
            .sdr_white_level_nits
            .unwrap_or(SC_RGB_NOMINAL_WHITE_NITS)
            / SC_RGB_NOMINAL_WHITE_NITS
    );
    let _ = writeln!(
        message,
        "display_primaries red=({:.6},{:.6}) green=({:.6},{:.6}) blue=({:.6},{:.6}) white=({:.6},{:.6})",
        hdr_display.red_primary[0],
        hdr_display.red_primary[1],
        hdr_display.green_primary[0],
        hdr_display.green_primary[1],
        hdr_display.blue_primary[0],
        hdr_display.blue_primary[1],
        hdr_display.white_point[0],
        hdr_display.white_point[1],
    );
    let _ = writeln!(
        message,
        "base_texture={}x{} stride={} format={:?} transfer={:?}",
        image.base.width,
        image.base.height,
        image.base.stride,
        image.base.format,
        image.base.transfer
    );
    if let Some(gain_map) = &image.gain_map {
        let _ = writeln!(
            message,
            "gain_texture={}x{} stride={} format={:?}",
            gain_map.width, gain_map.height, gain_map.stride, gain_map.format
        );
    } else {
        let _ = writeln!(message, "gain_texture=none");
    }
    let _ = writeln!(message, "image_color_space={:?}", image.color_space);
    if let Some(stats) = color_diagnostics(image) {
        let _ = writeln!(
            message,
            "color_diagnostics samples={} sample_step={} base_out_of_srgb_percent={:.4} base_max_positive_srgb_overshoot={:.6} base_max_negative_srgb_overshoot={:.6} hdr_above_sdr_white_percent={:.4} hdr_max_sc_rgb_channel={:.6} gain_linear_min={:.6} gain_linear_avg={:.6} gain_linear_max={:.6}",
            stats.sample_count,
            stats.sample_step,
            stats.base_out_of_srgb_percent,
            stats.base_max_positive_srgb_overshoot,
            stats.base_max_negative_srgb_overshoot,
            stats.hdr_above_sdr_white_percent,
            stats.hdr_max_sc_rgb_channel,
            stats.gain_linear_min,
            stats.gain_linear_avg,
            stats.gain_linear_max,
        );
    } else {
        let _ = writeln!(message, "color_diagnostics=unavailable");
    }
    if let Some(metadata) = image.hdr_gain_metadata {
        let _ = writeln!(
            message,
            "hdr_gain_metadata=headroom={} version={:?} source={:?}",
            metadata.headroom, metadata.version, metadata.source
        );
    } else {
        let _ = writeln!(message, "hdr_gain_metadata=none");
    }
    if let Some(metadata) = &image.tmap_metadata {
        let _ = writeln!(
            message,
            "tmap_metadata=present source={:?} gain_map_min_log2={:.6} gain_map_max_log2={:.6} gamma={:.6} offset_sdr={:.8} offset_hdr={:.8} hdr_capacity_min_log2={:.6} hdr_capacity_max_log2={:.6} adaptive_curve_nodes={}",
            metadata.source,
            metadata.gain_map_min_log2,
            metadata.gain_map_max_log2,
            metadata.gamma,
            metadata.offset_sdr,
            metadata.offset_hdr,
            metadata.hdr_capacity_min_log2,
            metadata.hdr_capacity_max_log2,
            metadata
                .adaptive_curve
                .as_ref()
                .map(|curve| curve.nodes.len())
                .unwrap_or(0)
        );
        if let Some(curve) = &metadata.adaptive_curve {
            for (index, node) in curve.nodes.iter().enumerate() {
                let _ = writeln!(
                    message,
                    "tmap_adaptive_curve_node[{index}] x={:.6} y={:.6} slope={:.6}",
                    node.x, node.y, node.slope
                );
            }
        }
    } else {
        let _ = writeln!(message, "tmap_metadata=none");
    }
    let _ = writeln!(
        message,
        "hdr_reconstruction_enabled={}",
        hdr_display.active && image.gain_map.is_some() && image.hdr_gain_metadata.is_some()
    );
    let _ = writeln!(
        message,
        "hdr_reconstruction_mode={}",
        if hdr_display.active && image.gain_map.is_some() && image.tmap_metadata.is_some() {
            "heif_tmap_log_gain"
        } else if hdr_display.active
            && image.gain_map.is_some()
            && image.hdr_gain_metadata.is_some()
        {
            "apple_xmp_linear_gain"
        } else {
            "none"
        }
    );
    let _ = writeln!(
        message,
        "sdr_color_management_enabled={}",
        !hdr_display.active && image.color_space == ImageColorSpace::DisplayP3
    );
    let _ = writeln!(message, "{wic_report}");

    append_log_message(
        &log_path,
        &format!(
            "[{}] helper_diagnostic_begin\n{}helper_diagnostic_end\n",
            unix_time_ms(),
            message
        ),
    );
}

fn trace_transition(label: &str, generation: Option<u64>, detail: &str) {
    let Some(log_path) = hdr_viewer_log_path_from_env() else {
        return;
    };
    let generation = generation
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned());
    append_log_message(
        &log_path,
        &format!(
            "[{}] helper_trace label={} generation={} {}\n",
            unix_time_ms(),
            label,
            generation,
            detail
        ),
    );
}

fn hdr_viewer_log_path_from_env() -> Option<PathBuf> {
    let value = std::env::var_os(HDR_VIEWER_LOG_ENV)?;
    let normalized = value.to_string_lossy().trim().to_ascii_lowercase();
    if normalized.is_empty() || matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

fn append_log_message(log_path: &Path, message: &str) {
    if let Some(parent) = log_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = file.write_all(message.as_bytes());
    }
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn decode_wic_image(
    path: &Path,
    wic_format: GUID,
    dxgi_format: DXGI_FORMAT,
    bytes_per_pixel: u32,
    transfer: TextureTransfer,
    allow_closest_size: bool,
) -> Result<DecodedImage> {
    let factory = create_wic_factory()?;
    let frame = open_wic_frame(&factory, path)?;
    decode_wic_frame_image(
        &factory,
        &frame,
        wic_format,
        dxgi_format,
        bytes_per_pixel,
        transfer,
        allow_closest_size,
    )
}

fn decode_wic_frame_image(
    factory: &IWICImagingFactory,
    frame: &IWICBitmapFrameDecode,
    wic_format: GUID,
    dxgi_format: DXGI_FORMAT,
    bytes_per_pixel: u32,
    transfer: TextureTransfer,
    allow_closest_size: bool,
) -> Result<DecodedImage> {
    let source: IWICBitmapSource = frame.cast()?;
    if let Some(image) = decode_wic_source_transform(
        frame,
        wic_format,
        dxgi_format,
        bytes_per_pixel,
        transfer,
        allow_closest_size,
    )? {
        return Ok(image);
    }

    let converter = unsafe { factory.CreateFormatConverter() }
        .context("could not create WIC format converter")?;
    unsafe {
        converter.Initialize(
            &source,
            &wic_format,
            WICBitmapDitherTypeNone,
            Option::<&IWICPalette>::None,
            0.0,
            WICBitmapPaletteTypeCustom,
        )
    }
    .context("could not convert image into DirectX texture format")?;

    let mut width = 0_u32;
    let mut height = 0_u32;
    unsafe { converter.GetSize(&mut width, &mut height) }.context("could not read image size")?;
    let stride = width
        .checked_mul(bytes_per_pixel)
        .context("image is too wide for DirectX texture upload")?;
    let byte_len = stride
        .checked_mul(height)
        .context("image is too large for DirectX texture upload")?;
    let mut bytes = vec![0_u8; byte_len as usize];
    unsafe { converter.CopyPixels(null(), stride, &mut bytes) }
        .context("could not copy decoded image pixels")?;

    Ok(DecodedImage {
        width,
        height,
        stride,
        format: dxgi_format,
        transfer,
        bytes,
    })
}

fn create_wic_factory() -> Result<IWICImagingFactory> {
    unsafe { CoCreateInstance(&CLSID_WICImagingFactory, None, CLSCTX_INPROC_SERVER) }
        .context("could not create WIC factory")
}

fn open_wic_frame(factory: &IWICImagingFactory, path: &Path) -> Result<IWICBitmapFrameDecode> {
    let decoder = open_wic_decoder(factory, path)?;
    largest_wic_frame(&decoder)
}

fn open_wic_decoder(factory: &IWICImagingFactory, path: &Path) -> Result<IWICBitmapDecoder> {
    let wide_path = wide_null(path.as_os_str());
    unsafe {
        factory.CreateDecoderFromFilename(
            PCWSTR(wide_path.as_ptr()),
            None,
            GENERIC_READ,
            WICDecodeMetadataCacheOnLoad,
        )
    }
    .context("could not create WIC decoder")
}

fn largest_wic_frame(decoder: &IWICBitmapDecoder) -> Result<IWICBitmapFrameDecode> {
    let frame_count = unsafe { decoder.GetFrameCount() }.context("could not read frame count")?;
    if frame_count == 0 {
        bail!("image contains no decodable frames");
    }

    let mut best_frame = None;
    let mut best_area = 0_u64;
    for index in 0..frame_count {
        let frame = unsafe { decoder.GetFrame(index) }
            .with_context(|| format!("could not read image frame {index}"))?;
        let mut width = 0_u32;
        let mut height = 0_u32;
        unsafe { frame.GetSize(&mut width, &mut height) }
            .with_context(|| format!("could not read image frame {index} size"))?;
        let area = u64::from(width) * u64::from(height);
        if area > best_area || best_frame.is_none() {
            best_area = area;
            best_frame = Some(frame);
        }
    }

    best_frame.context("image contains no decodable frames")
}

fn decode_wic_source_transform(
    frame: &IWICBitmapFrameDecode,
    wic_format: GUID,
    dxgi_format: DXGI_FORMAT,
    bytes_per_pixel: u32,
    transfer: TextureTransfer,
    allow_closest_size: bool,
) -> Result<Option<DecodedImage>> {
    let Ok(source_transform) = frame.cast::<IWICBitmapSourceTransform>() else {
        return Ok(None);
    };
    let mut closest_format = wic_format;
    if unsafe { source_transform.GetClosestPixelFormat(&mut closest_format) }.is_err()
        || closest_format != wic_format
    {
        return Ok(None);
    }

    let mut native_width = 0_u32;
    let mut native_height = 0_u32;
    unsafe { frame.GetSize(&mut native_width, &mut native_height) }
        .context("could not read image size")?;
    if let Some(image) = copy_wic_source_transform_pixels(
        &source_transform,
        wic_format,
        dxgi_format,
        bytes_per_pixel,
        transfer,
        native_width,
        native_height,
    )? {
        return Ok(Some(image));
    }

    if !allow_closest_size {
        return Ok(None);
    }

    let mut width = native_width;
    let mut height = native_height;
    if unsafe { source_transform.GetClosestSize(&mut width, &mut height) }.is_err()
        || width == 0
        || height == 0
        || (width == native_width && height == native_height)
    {
        return Ok(None);
    }

    copy_wic_source_transform_pixels(
        &source_transform,
        wic_format,
        dxgi_format,
        bytes_per_pixel,
        transfer,
        width,
        height,
    )
}

fn copy_wic_source_transform_pixels(
    source_transform: &IWICBitmapSourceTransform,
    wic_format: GUID,
    dxgi_format: DXGI_FORMAT,
    bytes_per_pixel: u32,
    transfer: TextureTransfer,
    width: u32,
    height: u32,
) -> Result<Option<DecodedImage>> {
    let stride = width
        .checked_mul(bytes_per_pixel)
        .context("image is too wide for DirectX texture upload")?;
    let byte_len = stride
        .checked_mul(height)
        .context("image is too large for DirectX texture upload")?;
    let mut bytes = vec![0_u8; byte_len as usize];
    if unsafe {
        source_transform.CopyPixels(
            null(),
            width,
            height,
            &wic_format,
            WICBitmapTransformRotate0,
            stride,
            &mut bytes,
        )
    }
    .is_err()
    {
        return Ok(None);
    }

    Ok(Some(DecodedImage {
        width,
        height,
        stride,
        format: dxgi_format,
        transfer,
        bytes,
    }))
}

fn client_size(hwnd: HWND) -> Result<(u32, u32)> {
    let mut rect = RECT::default();
    unsafe { windows::Win32::UI::WindowsAndMessaging::GetClientRect(hwnd, &mut rect) }
        .context("could not read HDR viewer client size")?;
    Ok((
        (rect.right - rect.left).max(0) as u32,
        (rect.bottom - rect.top).max(0) as u32,
    ))
}

fn show_error_message(message: &str) {
    let text = wide_null(message);
    let caption = wide_null("Picturious HDR Viewer");
    unsafe {
        MessageBoxW(
            None,
            PCWSTR(text.as_ptr()),
            PCWSTR(caption.as_ptr()),
            Default::default(),
        );
    }
}

fn wide_null(value: impl AsRef<OsStr>) -> Vec<u16> {
    value
        .as_ref()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn wide_array_to_string(value: &[u16]) -> Option<String> {
    let len = value
        .iter()
        .position(|character| *character == 0)
        .unwrap_or(value.len());
    if len == 0 {
        return None;
    }

    String::from_utf16(&value[..len]).ok()
}

const VERTEX_SHADER: &str = r#"
struct VsIn {
    float2 position : POSITION;
    float2 texcoord : TEXCOORD0;
};

struct VsOut {
    float4 position : SV_POSITION;
    float2 texcoord : TEXCOORD0;
};

VsOut main(VsIn input) {
    VsOut output;
    output.position = float4(input.position, 0.0, 1.0);
    output.texcoord = input.texcoord;
    return output;
}
"#;

const PIXEL_SHADER: &str = r#"
Texture2D imageTexture : register(t0);
SamplerState imageSampler : register(s0);

float4 main(float4 position : SV_POSITION, float2 texcoord : TEXCOORD0) : SV_TARGET {
    return imageTexture.Sample(imageSampler, texcoord);
}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fitted_quad_preserves_wide_image_aspect() {
        let vertices = fitted_quad_vertices(4000, 2000, 3840, 2160);
        assert_eq!(vertices[0].position[0], -1.0);
        assert!(vertices[0].position[1] < 1.0);
    }

    #[test]
    fn fitted_quad_preserves_tall_image_aspect() {
        let vertices = fitted_quad_vertices(2000, 4000, 3840, 2160);
        assert_eq!(vertices[0].position[1], 1.0);
        assert!(vertices[0].position[0] > -1.0);
    }

    #[test]
    fn apple_hdr_gain_metadata_extracts_prefixed_xmp_values() {
        let text = r#"
            <rdf:Description
                HDRGainMap:HDRGainMapVersion="131072"
                HDRGainMap:HDRGainMapHeadroom="3.198659" />
        "#;

        let metadata = apple_hdr_gain_metadata_from_text(text).unwrap();

        assert_eq!(metadata.version, Some(131072));
        assert!((metadata.headroom - 3.198659).abs() < f32::EPSILON);
    }

    #[test]
    fn apple_hdr_gain_metadata_rejects_missing_headroom() {
        let text = r#"<rdf:Description HDRGainMap:HDRGainMapVersion="131072" />"#;

        assert!(apple_hdr_gain_metadata_from_text(text).is_none());
    }

    #[test]
    fn image_color_space_detects_display_p3_metadata() {
        assert_eq!(
            image_color_space_from_metadata_bytes(b"Display P3 Primaries; PQ"),
            ImageColorSpace::DisplayP3
        );
    }

    #[test]
    fn image_color_space_detects_utf16_display_p3_metadata() {
        let utf16be_display_p3 = [
            0, b'D', 0, b'i', 0, b's', 0, b'p', 0, b'l', 0, b'a', 0, b'y', 0, b' ', 0, b'P', 0,
            b'3',
        ];

        assert_eq!(
            image_color_space_from_metadata_bytes(&utf16be_display_p3),
            ImageColorSpace::DisplayP3
        );
    }

    #[test]
    fn hd_gain_map_info_extracts_adaptive_curve_nodes() {
        let mut bytes = vec![0_u8; 4 + 4 + 146 + 4 + 2 * 12];
        bytes[0..4].copy_from_slice(b"gmap");
        bytes[154..158].copy_from_slice(&2_u32.to_be_bytes());
        for (index, node) in [
            AdaptiveGainCurveNode {
                x: 0.0,
                y: 0.2,
                slope: 1.5,
            },
            AdaptiveGainCurveNode {
                x: 1.0,
                y: 0.95,
                slope: 0.0,
            },
        ]
        .iter()
        .enumerate()
        {
            let offset = 158 + index * 12;
            bytes[offset..offset + 4].copy_from_slice(&node.x.to_bits().to_be_bytes());
            bytes[offset + 4..offset + 8].copy_from_slice(&node.y.to_bits().to_be_bytes());
            bytes[offset + 8..offset + 12].copy_from_slice(&node.slope.to_bits().to_be_bytes());
        }

        let metadata = parse_hd_gain_map_info(&bytes, 4.0).unwrap();
        let curve = metadata.adaptive_curve.unwrap();

        assert_eq!(curve.nodes.len(), 2);
        assert_eq!(curve.nodes[0].x, 0.0);
        assert!((curve.nodes[1].y - 0.95).abs() < f32::EPSILON);
        assert_eq!(metadata.gain_map_max_log2, 2.0);
    }

    #[test]
    fn iso21496_tmap_metadata_extracts_gain_parameters() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.push(1 << 6);
        for (numerator, denominator, signed) in [
            (0, 1_000_000, false),
            (2_239_967, 1_000_000, false),
            (-1_616, 1_000_000, true),
            (2_239_967, 1_000_000, true),
            (803_223, 1_000_000, false),
            (10, 1_000_000, true),
            (10, 1_000_000, true),
        ] {
            if signed {
                bytes.extend_from_slice(&(numerator as i32).to_be_bytes());
            } else {
                bytes.extend_from_slice(&(numerator as u32).to_be_bytes());
            }
            bytes.extend_from_slice(&(denominator as u32).to_be_bytes());
        }

        let metadata = parse_iso21496_gain_map_metadata(&bytes).unwrap();
        let tmap_metadata = iso21496_tmap_metadata(metadata);

        assert!(iso21496_gain_map_metadata_matches(&metadata, 2.239967));
        assert_eq!(
            tmap_metadata.source,
            TmapMetadataSource::AppleHeifTmapIso21496
        );
        assert!((tmap_metadata.gain_map_min_log2 + 0.001616).abs() < 0.000001);
        assert!((tmap_metadata.gain_map_max_log2 - 2.239967).abs() < 0.000001);
        assert!((tmap_metadata.gamma - 0.803223).abs() < 0.000001);
        assert!((tmap_metadata.offset_sdr - 0.00001).abs() < 0.000001);
        assert!((tmap_metadata.offset_hdr - 0.00001).abs() < 0.000001);

        let mut idat_payload = vec![0_u8; 25];
        idat_payload.extend_from_slice(&bytes);
        let heif_metadata =
            parse_iso21496_tmap_metadata_from_payloads(&[idat_payload.as_slice()], 4.723863)
                .unwrap();

        assert!((heif_metadata.gamma - 0.803223).abs() < 0.000001);
    }

    #[test]
    fn hdr_display_p3_gain_map_shader_compiles() {
        let source = hdr_pixel_shader(HdrShaderConfig {
            gain_mapping: Some(GainMapShaderConfig {
                mode: GainMapShaderMode::AppleLinear { headroom: 3.198659 },
            }),
            color_space: ImageColorSpace::DisplayP3,
            source_transfer: TextureTransfer::HardwareSrgbDecoded,
            sdr_white_scale: 2.5,
            hdr_output: true,
        });

        compile_shader(&source, s!("main"), s!("ps_5_0")).unwrap();
    }

    #[test]
    fn sdr_display_p3_color_management_shader_compiles() {
        let source = hdr_pixel_shader(HdrShaderConfig {
            gain_mapping: None,
            color_space: ImageColorSpace::DisplayP3,
            source_transfer: TextureTransfer::SrgbEncoded,
            sdr_white_scale: 1.0,
            hdr_output: false,
        });

        compile_shader(&source, s!("main"), s!("ps_5_0")).unwrap();
    }

    #[test]
    fn hdr_tmap_log_gain_shader_compiles() {
        let source = hdr_pixel_shader(HdrShaderConfig {
            gain_mapping: Some(GainMapShaderConfig {
                mode: GainMapShaderMode::TmapLog {
                    gain_map_min_log2: -0.001616,
                    gain_map_max_log2: 2.239967,
                    gamma: 0.803223,
                    offset_sdr: 0.00001,
                    offset_hdr: 0.00001,
                    weight_factor: 1.0,
                },
            }),
            color_space: ImageColorSpace::DisplayP3,
            source_transfer: TextureTransfer::SrgbEncoded,
            sdr_white_scale: 2.8,
            hdr_output: true,
        });

        compile_shader(&source, s!("main"), s!("ps_5_0")).unwrap();
    }

    #[test]
    fn sdr_decode_prefers_10bit_then_16bit_then_8bit() {
        let formats = sdr_base_decode_formats();

        assert_eq!(formats[0].wic_format, GUID_WICPixelFormat32bppR10G10B10A2);
        assert_eq!(formats[0].dxgi_format, DXGI_FORMAT_R10G10B10A2_UNORM);
        assert_eq!(formats[0].bytes_per_pixel, 4);
        assert_eq!(formats[0].transfer, TextureTransfer::SrgbEncoded);
        assert_eq!(formats[1].wic_format, GUID_WICPixelFormat64bppRGBA);
        assert_eq!(formats[1].dxgi_format, DXGI_FORMAT_R16G16B16A16_UNORM);
        assert_eq!(formats[1].bytes_per_pixel, 8);
        assert_eq!(formats[1].transfer, TextureTransfer::SrgbEncoded);
        assert_eq!(formats[2].wic_format, GUID_WICPixelFormat32bppRGBA);
        assert_eq!(formats[2].dxgi_format, DXGI_FORMAT_R8G8B8A8_UNORM);
        assert_eq!(formats[2].bytes_per_pixel, 4);
        assert_eq!(formats[2].transfer, TextureTransfer::SrgbEncoded);
    }

    #[test]
    fn hdr_decode_prefers_10bit_then_16bit_before_8bit_srgb() {
        let formats = hdr_base_decode_formats();

        assert_eq!(formats[0].wic_format, GUID_WICPixelFormat32bppR10G10B10A2);
        assert_eq!(formats[0].dxgi_format, DXGI_FORMAT_R10G10B10A2_UNORM);
        assert_eq!(formats[0].transfer, TextureTransfer::SrgbEncoded);
        assert_eq!(formats[1].wic_format, GUID_WICPixelFormat64bppRGBA);
        assert_eq!(formats[1].dxgi_format, DXGI_FORMAT_R16G16B16A16_UNORM);
        assert_eq!(formats[1].transfer, TextureTransfer::SrgbEncoded);
        assert_eq!(formats[2].wic_format, GUID_WICPixelFormat32bppRGBA);
        assert_eq!(formats[2].dxgi_format, DXGI_FORMAT_R8G8B8A8_UNORM_SRGB);
        assert_eq!(formats[2].transfer, TextureTransfer::HardwareSrgbDecoded);
    }

    #[test]
    fn display_p3_primary_extends_outside_srgb() {
        let red = display_p3_to_sc_rgb([1.0, 0.0, 0.0]);
        let green = display_p3_to_sc_rgb([0.0, 1.0, 0.0]);

        assert!(red[0] > 1.0);
        assert!(red[1] < 0.0);
        assert!(green[0] < 0.0);
        assert!(green[1] > 1.0);
    }

    #[test]
    fn sdr_swap_chain_prefers_10bit_color_space() {
        assert_eq!(
            swap_chain_color_space(DXGI_FORMAT_R10G10B10A2_UNORM),
            DXGI_COLOR_SPACE_RGB_FULL_G22_NONE_P709
        );
        assert_eq!(
            swap_chain_color_space(DXGI_FORMAT_R16G16B16A16_FLOAT),
            DXGI_COLOR_SPACE_RGB_FULL_G10_NONE_P709
        );
    }

    #[test]
    fn r8_gain_map_upscale_preserves_edges() {
        let source = DecodedImage {
            width: 2,
            height: 2,
            stride: 2,
            format: DXGI_FORMAT_R8_UNORM,
            transfer: TextureTransfer::SrgbEncoded,
            bytes: vec![0, 64, 128, 255],
        };

        let upscaled = upscale_r8_bilinear(&source, 4, 4).unwrap();

        assert_eq!(upscaled[0], 0);
        assert_eq!(upscaled[15], 255);
        assert!(upscaled[5] > 0);
        assert!(upscaled[10] < 255);
    }
}
