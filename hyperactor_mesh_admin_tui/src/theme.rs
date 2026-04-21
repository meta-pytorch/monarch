/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use clap::ValueEnum;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;

use crate::model::NodeType;

/// Selectable color theme.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum ThemeName {
    /// Nord — an arctic, north-bluish color palette.
    #[default]
    Nord,
    /// doom-nord-light — desaturated Nord accents for light backgrounds.
    DoomNordLight,
}

impl std::fmt::Display for ThemeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThemeName::Nord => write!(f, "nord"),
            ThemeName::DoomNordLight => write!(f, "doom-nord-light"),
        }
    }
}

/// Selectable display language.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum LangName {
    /// English (default).
    #[default]
    En,
    /// 简体中文 (Simplified Chinese).
    Zh,
}

impl std::fmt::Display for LangName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LangName::En => write!(f, "en"),
            LangName::Zh => write!(f, "zh"),
        }
    }
}

/// All user-visible text in the TUI.
///
/// Gathered into a single struct so that localisation (or
/// white-labelling) is a drop-in replacement — construct an
/// alternative `Labels` and pass it to `App`.
pub(crate) struct Labels {
    // App identity
    pub(crate) app_name: &'static str,

    // Chrome / decoration
    pub(crate) separator: &'static str,
    pub(crate) selection_caret: &'static str,
    pub(crate) refresh_icon: &'static str,
    pub(crate) no_selection: &'static str,

    // Header stat labels
    pub(crate) uptime: &'static str,
    pub(crate) system: &'static str,
    pub(crate) sys_on: &'static str,
    pub(crate) sys_off: &'static str,
    pub(crate) stopped: &'static str,
    pub(crate) stopped_on: &'static str,
    pub(crate) stopped_off: &'static str,

    // Detail pane labels
    pub(crate) hosts: &'static str,
    pub(crate) started_by: &'static str,
    pub(crate) uptime_detail: &'static str,
    pub(crate) started_at: &'static str,
    pub(crate) data_as_of: &'static str,
    pub(crate) address: &'static str,
    pub(crate) procs: &'static str,
    pub(crate) name: &'static str,
    pub(crate) actors: &'static str,
    pub(crate) actor_type: &'static str,
    pub(crate) messages: &'static str,
    pub(crate) created: &'static str,
    pub(crate) last_handler: &'static str,
    pub(crate) children: &'static str,
    pub(crate) status: &'static str,
    pub(crate) processing_time: &'static str,

    // Proc debug stats labels
    pub(crate) rss: &'static str,
    pub(crate) vm_size: &'static str,
    pub(crate) queue_depth: &'static str,
    pub(crate) peak_depth: &'static str,
    pub(crate) last_busy: &'static str,

    // Failure detail labels
    pub(crate) error_message: &'static str,
    pub(crate) root_cause: &'static str,
    pub(crate) failed_at: &'static str,
    pub(crate) propagated: &'static str,
    pub(crate) poisoned: &'static str,
    pub(crate) failed_actors: &'static str,
    pub(crate) yes: &'static str,
    pub(crate) no: &'static str,

    // Diagnostic pane status / summary strings
    pub(crate) diag_running: &'static str,
    pub(crate) diag_live_updates: &'static str,
    pub(crate) diag_completed_at: &'static str,
    pub(crate) diag_static_snapshot: &'static str,
    pub(crate) diag_checks_all: &'static str,
    pub(crate) diag_checks_passed: &'static str,
    pub(crate) diag_admin_label: &'static str,
    pub(crate) diag_mesh_label: &'static str,
    pub(crate) diag_status_healthy: &'static str,
    pub(crate) diag_status_failing: &'static str,
    pub(crate) diag_status_na: &'static str,

    // Diagnostic pane node annotations
    pub(crate) diag_note_admin_server: &'static str,
    pub(crate) diag_note_host_agent: &'static str,
    pub(crate) diag_note_admin_service_proc: &'static str,
    pub(crate) diag_note_local_client_proc: &'static str,
    pub(crate) diag_note_introspection_handler: &'static str,
    pub(crate) diag_note_actor_lifecycle_manager: &'static str,
    pub(crate) diag_note_root_client_bridge: &'static str,
    pub(crate) diag_note_comm_actor: &'static str,
    pub(crate) diag_note_proc_agent: &'static str,
    pub(crate) diag_note_user_proc: &'static str,
    pub(crate) diag_note_user_actor: &'static str,

    // Pane titles
    pub(crate) pane_topology: &'static str,
    pub(crate) pane_details: &'static str,
    pub(crate) pane_error: &'static str,
    pub(crate) pane_root_details: &'static str,
    pub(crate) pane_host_details: &'static str,
    pub(crate) pane_proc_details: &'static str,
    pub(crate) pane_actor_details: &'static str,
    pub(crate) pane_flight_recorder: &'static str,
    pub(crate) pane_diagnostics: &'static str,

    // Footer
    pub(crate) footer_help_text: &'static str,
    pub(crate) footer_diag_running_help_text: &'static str,
    pub(crate) footer_diag_completed_help_text: &'static str,
    pub(crate) footer_pyspy_help_text: &'static str,
    pub(crate) footer_config_help_text: &'static str,
}

impl Labels {
    /// English (default) label set.
    pub(crate) fn en() -> Self {
        Self {
            app_name: "mesh-admin",
            separator: " • ",
            selection_caret: "▸ ",
            refresh_icon: "⟳ ",
            no_selection: "No selection",
            uptime: "up: ",
            system: "sys:",
            sys_on: "on",
            sys_off: "off",
            stopped: "stopped:",
            stopped_on: "on",
            stopped_off: "off",
            hosts: "Hosts: ",
            started_by: "Started by: ",
            uptime_detail: "Uptime: ",
            started_at: "Started at: ",
            data_as_of: "Data as of: ",
            address: "Address: ",
            procs: "Procs: ",
            name: "Name: ",
            actors: "Actors: ",
            actor_type: "Type: ",
            messages: "Messages: ",
            created: "Created: ",
            last_handler: "Last handler: ",
            children: "Children: ",
            status: "Status: ",
            processing_time: "Processing time: ",
            rss: "RSS: ",
            vm_size: "VM Size: ",
            queue_depth: "Queue depth: ",
            peak_depth: "Peak depth: ",
            last_busy: "Last busy: ",
            error_message: "Error: ",
            root_cause: "Root cause: ",
            failed_at: "Failed at: ",
            propagated: "Propagated: ",
            poisoned: "Poisoned: ",
            failed_actors: "Failed actors: ",
            yes: "yes",
            no: "no",
            diag_running: "Running\u{2026}",
            diag_live_updates: "live updates",
            diag_completed_at: "Completed at",
            diag_static_snapshot: "static snapshot",
            diag_checks_all: "All",
            diag_checks_passed: "checks passed",
            diag_admin_label: "Admin:",
            diag_mesh_label: "Mesh:",
            diag_status_healthy: "healthy",
            diag_status_failing: "failing",
            diag_status_na: "n/a",
            diag_note_admin_server: "admin HTTP server — lists connected hosts",
            diag_note_host_agent: "host agent — manages procs on this machine",
            diag_note_admin_service_proc: "admin service proc — hosts the admin actor layer",
            diag_note_local_client_proc: "local client proc — in-process actors (empty in pure Rust)",
            diag_note_introspection_handler: "handles GET /v1/\u{2026} HTTP requests",
            diag_note_actor_lifecycle_manager: "manages actor spawn and lifecycle",
            diag_note_root_client_bridge: "root client bridge — connects admin to the mesh",
            diag_note_comm_actor: "mesh comm actor — enables proc-to-proc messaging",
            diag_note_proc_agent: "proc agent — manages actor spawn and lifecycle on this proc",
            diag_note_user_proc: "user proc — your workload is alive",
            diag_note_user_actor: "user actor — reachable through full stack",
            pane_topology: "Topology",
            pane_details: "Details",
            pane_error: "Error",
            pane_root_details: "Root Details",
            pane_host_details: "Host Details",
            pane_proc_details: "Proc Details",
            pane_actor_details: "Actor Details",
            pane_flight_recorder: "Flight Recorder",
            pane_diagnostics: "Diagnostics",
            footer_help_text: "q: quit | j/k: navigate | g/G: top/bottom | Tab: expand/collapse | c: collapse all | s: system procs | h: stopped actors | d: diag | p: py-spy | C: config",
            footer_diag_running_help_text: "q: quit | Esc: cancel | j/k: scroll",
            footer_diag_completed_help_text: "q: quit | Esc: back to topology | j/k: scroll | r: rerun",
            footer_pyspy_help_text: "q: quit | Esc: back to topology | j/k: scroll | p: refresh",
            footer_config_help_text: "q: quit | Esc: back to topology | j/k: scroll | C: refresh",
        }
    }

    /// 简体中文 (Simplified Chinese) label set.
    pub(crate) fn zh() -> Self {
        Self {
            app_name: "网格管理",
            separator: " • ",
            selection_caret: "▸ ",
            refresh_icon: "⟳ ",
            no_selection: "未选择",
            uptime: "运行: ",
            system: "系统:",
            sys_on: "开",
            sys_off: "关",
            stopped: "已停止:",
            stopped_on: "开",
            stopped_off: "关",
            hosts: "主机: ",
            started_by: "启动者: ",
            uptime_detail: "运行时间: ",
            started_at: "启动时间: ",
            data_as_of: "数据时间: ",
            address: "地址: ",
            procs: "进程: ",
            name: "名称: ",
            actors: "执行器: ",
            actor_type: "类型: ",
            messages: "消息: ",
            created: "创建时间: ",
            last_handler: "最后处理: ",
            children: "子节点: ",
            status: "状态: ",
            processing_time: "处理时间: ",
            rss: "RSS: ",
            vm_size: "虚拟内存: ",
            queue_depth: "队列深度: ",
            peak_depth: "峰值深度: ",
            last_busy: "最近繁忙: ",
            error_message: "错误: ",
            root_cause: "根因: ",
            failed_at: "失败时间: ",
            propagated: "传播: ",
            poisoned: "中毒: ",
            failed_actors: "失败执行器: ",
            yes: "是",
            no: "否",
            diag_running: "运行中\u{2026}",
            diag_live_updates: "实时更新",
            diag_completed_at: "完成于",
            diag_static_snapshot: "静态快照",
            diag_checks_all: "所有",
            diag_checks_passed: "项检查通过",
            diag_admin_label: "管理:",
            diag_mesh_label: "网格:",
            diag_status_healthy: "健康",
            diag_status_failing: "失败",
            diag_status_na: "不适用",
            diag_note_admin_server: "管理HTTP服务器 — 列出已连接主机",
            diag_note_host_agent: "主机代理 — 管理此机器上的进程",
            diag_note_admin_service_proc: "管理服务进程 — 承载管理员Actor层",
            diag_note_local_client_proc: "本地客户端进程 — 进程内Actor（纯Rust中为空）",
            diag_note_introspection_handler: "处理 GET /v1/\u{2026} HTTP请求",
            diag_note_actor_lifecycle_manager: "管理Actor派生和生命周期",
            diag_note_root_client_bridge: "根客户端桥 — 连接管理员与用户网格",
            diag_note_comm_actor: "网格通信Actor — 实现进程间消息传递",
            diag_note_proc_agent: "进程代理 — 管理此进程上的Actor派生和生命周期",
            diag_note_user_proc: "用户进程 — 您的工作负载正在运行",
            diag_note_user_actor: "用户Actor — 可通过完整堆栈访问",
            pane_topology: "拓扑",
            pane_details: "详情",
            pane_error: "错误",
            pane_root_details: "根节点详情",
            pane_host_details: "主机详情",
            pane_proc_details: "进程详情",
            pane_actor_details: "执行器详情",
            pane_flight_recorder: "飞行记录器",
            pane_diagnostics: "诊断",
            footer_help_text: "q: 退出 | j/k: 导航 | g/G: 顶部/底部 | Tab: 展开/折叠 | c: 全部折叠 | s: 系统进程 | h: 已停止 | d: 诊断 | p: py-spy | C: 配置",
            footer_diag_running_help_text: "q: 退出 | Esc: 取消 | j/k: 滚动",
            footer_diag_completed_help_text: "q: 退出 | Esc: 返回拓扑 | j/k: 滚动 | r: 重新运行",
            footer_pyspy_help_text: "q: 退出 | Esc: 返回拓扑 | j/k: 滚动 | p: 刷新",
            footer_config_help_text: "q: 退出 | Esc: 返回拓扑 | j/k: 滚动 | C: 刷新",
        }
    }
}

/// Color scheme for the TUI.
///
/// Defines all colors and styles used throughout the interface.
/// Selectable via `--theme` (see [`ThemeName`]).
///
/// ## Semantic Roles
///
/// Each field maps to a semantic role, not a specific color. Themes
/// assign concrete colors to these roles while preserving the
/// following intent:
///
/// - **Topology**: host, proc, actor node types in the tree
/// - **Selection/focus**: currently highlighted node
/// - **System state**: config toggles, system/infrastructure actors
/// - **Timing/temporal**: refresh intervals, durations
/// - **Secondary**: URLs, labels, borders, muted text
/// - **Error/warning/success**: status indicators
/// - **Headers**: app name, info headings
///
/// ## Semantic Categories
///
/// - **UI chrome**: App name, borders, footer
/// - **Node types**: Distinct style per tree node kind
/// - **States**: Error, warning, success, stopped
/// - **Header stats**: Timing, topology counts, selection, config
/// - **Detail pane**: Labels, status indicators
#[derive(Clone, Copy)]
pub(crate) struct ColorScheme {
    // UI chrome
    pub(crate) app_name: Style,
    pub(crate) border: Style,
    pub(crate) _border_focused: Style,
    pub(crate) _footer_help: Style,

    // Node types (tree rendering) — one style per node kind.
    // Specific colors vary by theme; see default() and nord().
    pub(crate) node_root: Style,
    pub(crate) node_host: Style,
    pub(crate) node_proc: Style,
    pub(crate) node_actor: Style,
    pub(crate) node_failed: Style,
    pub(crate) node_system_actor: Style,
    pub(crate) node_user_actor: Style,

    // Semantic states
    pub(crate) error: Style,
    pub(crate) info: Style,

    // Header stat categories
    pub(crate) stat_timing: Style, // refresh intervals, durations (temporal)
    pub(crate) stat_selection: Style, // current selection (focus)
    pub(crate) stat_system: Style, // config toggles (state)
    pub(crate) stat_url: Style,    // connection info (secondary)
    pub(crate) stat_label: Style,  // stat labels/prefixes
    pub(crate) _stat_value: Style, // stat numeric values

    // Detail pane and misc rendering
    pub(crate) detail_label: Style, // label text in detail key-value lines
    pub(crate) detail_stopped: Style, // stopped/failed nodes in tree + detail
    pub(crate) detail_status_ok: Style, // actor status "Running"
    pub(crate) detail_status_warn: Style, // actor status non-Running (idle, etc.)
    pub(crate) detail_status_failed: Style, // actor status "failed:*"
    pub(crate) footer_help: Style,  // footer help bar text
    pub(crate) header_class_bracket: Style, // classification brackets in header
}

impl ColorScheme {
    /// Nord color scheme (https://www.nordtheme.com/).
    ///
    /// An arctic, north-bluish palette with muted, harmonious colors.
    pub(crate) fn nord() -> Self {
        // Polar Night (dark backgrounds)
        let polar3 = Color::Rgb(76, 86, 106); // #4C566A
        // Snow Storm (light text)
        let snow0 = Color::Rgb(216, 222, 233); // #D8DEE9
        let snow2 = Color::Rgb(236, 239, 244); // #ECEFF4
        // Frost (blues/cyans)
        let frost_teal = Color::Rgb(143, 188, 187); // #8FBCBB
        let frost_cyan = Color::Rgb(136, 192, 208); // #88C0D0
        let frost_blue = Color::Rgb(129, 161, 193); // #81A1C1
        let frost_dark = Color::Rgb(94, 129, 172); // #5E81AC
        // Aurora (accents)
        let aurora_red = Color::Rgb(191, 97, 106); // #BF616A
        let aurora_orange = Color::Rgb(208, 135, 112); // #D08770
        let aurora_yellow = Color::Rgb(235, 203, 139); // #EBCB8B
        let aurora_green = Color::Rgb(163, 190, 140); // #A3BE8C
        let aurora_purple = Color::Rgb(180, 142, 173); // #B48EAD

        Self {
            // UI chrome
            app_name: Style::default().fg(frost_cyan).add_modifier(Modifier::BOLD),
            border: Style::default().fg(polar3),
            _border_focused: Style::default().fg(frost_cyan),
            _footer_help: Style::default().fg(polar3),

            // Node types
            node_root: Style::default().fg(frost_teal).add_modifier(Modifier::BOLD),
            node_host: Style::default()
                .fg(aurora_green)
                .add_modifier(Modifier::BOLD),
            node_proc: Style::default()
                .fg(aurora_yellow)
                .add_modifier(Modifier::BOLD),
            node_actor: Style::default().fg(frost_blue),
            node_failed: Style::default().fg(aurora_red),
            node_system_actor: Style::default().fg(frost_dark),
            node_user_actor: Style::default().fg(aurora_green),

            // Semantic states
            error: Style::default().fg(aurora_red),
            info: Style::default().fg(frost_cyan),

            // Header stats
            stat_timing: Style::default().fg(aurora_yellow),
            stat_selection: Style::default().fg(aurora_purple),
            stat_system: Style::default().fg(frost_dark),
            stat_url: Style::default().fg(polar3),
            stat_label: Style::default().fg(snow0).add_modifier(Modifier::BOLD),
            _stat_value: Style::default().fg(snow2).add_modifier(Modifier::BOLD),

            // Detail pane and misc
            detail_label: Style::default().fg(snow0).add_modifier(Modifier::BOLD),
            detail_stopped: Style::default().fg(polar3),
            detail_status_ok: Style::default().fg(aurora_green),
            detail_status_warn: Style::default()
                .fg(aurora_orange)
                .add_modifier(Modifier::BOLD),
            detail_status_failed: Style::default().fg(aurora_red).add_modifier(Modifier::BOLD),
            footer_help: Style::default().fg(polar3),
            header_class_bracket: Style::default().fg(polar3),
        }
    }

    /// doom-nord-light color scheme.
    ///
    /// Desaturated Nord accents adapted for light backgrounds.
    /// Source: doom-nord-light-theme.el
    pub(crate) fn doom_nord_light() -> Self {
        // Base scale (light to dark)
        let base7 = Color::Rgb(96, 114, 140); // #60728C
        // Foreground
        let fg = Color::Rgb(59, 66, 82); // #3B4252
        let fg_alt = Color::Rgb(46, 52, 64); // #2E3440
        // Accents
        let red = Color::Rgb(153, 50, 75); // #99324B
        let orange = Color::Rgb(172, 68, 38); // #AC4426
        let green = Color::Rgb(79, 137, 76); // #4F894C
        let yellow = Color::Rgb(154, 117, 0); // #9A7500
        let blue = Color::Rgb(59, 110, 168); // #3B6EA8
        let dark_blue = Color::Rgb(82, 114, 175); // #5272AF
        let teal = Color::Rgb(41, 131, 141); // #29838D
        let cyan = Color::Rgb(57, 142, 172); // #398EAC
        let violet = Color::Rgb(132, 40, 121); // #842879

        Self {
            // UI chrome
            app_name: Style::default().fg(teal).add_modifier(Modifier::BOLD),
            border: Style::default().fg(base7),
            _border_focused: Style::default().fg(cyan),
            _footer_help: Style::default().fg(base7),

            // Node types
            node_root: Style::default().fg(teal).add_modifier(Modifier::BOLD),
            node_host: Style::default().fg(green).add_modifier(Modifier::BOLD),
            node_proc: Style::default().fg(yellow).add_modifier(Modifier::BOLD),
            node_actor: Style::default().fg(blue),
            node_failed: Style::default().fg(red),
            node_system_actor: Style::default().fg(orange),
            node_user_actor: Style::default().fg(green),

            // Semantic states
            error: Style::default().fg(red),
            info: Style::default().fg(cyan),

            // Header stats
            stat_timing: Style::default().fg(yellow),
            stat_selection: Style::default().fg(violet),
            stat_system: Style::default().fg(dark_blue),
            stat_url: Style::default().fg(base7),
            stat_label: Style::default().fg(fg).add_modifier(Modifier::BOLD),
            _stat_value: Style::default().fg(fg_alt).add_modifier(Modifier::BOLD),

            // Detail pane and misc
            detail_label: Style::default().fg(fg).add_modifier(Modifier::BOLD),
            detail_stopped: Style::default().fg(base7),
            detail_status_ok: Style::default().fg(green),
            detail_status_warn: Style::default().fg(orange).add_modifier(Modifier::BOLD),
            detail_status_failed: Style::default().fg(red).add_modifier(Modifier::BOLD),
            footer_help: Style::default().fg(base7),
            header_class_bracket: Style::default().fg(base7),
        }
    }

    /// Return the style for a given node type.
    pub(crate) fn node_style(&self, node_type: NodeType) -> Style {
        match node_type {
            NodeType::Root => self.node_root,
            NodeType::Host => self.node_host,
            NodeType::Proc => self.node_proc,
            NodeType::Actor => self.node_actor,
        }
    }
}

/// Complete visual presentation — colors + text.
///
/// Swap in a different `Theme` to localise or white-label the TUI.
pub(crate) struct Theme {
    pub(crate) scheme: ColorScheme,
    pub(crate) labels: Labels,
}

impl Theme {
    /// Build a theme for the given theme and language.
    pub(crate) fn new(theme_name: ThemeName, lang_name: LangName) -> Self {
        let scheme = match theme_name {
            ThemeName::Nord => ColorScheme::nord(),
            ThemeName::DoomNordLight => ColorScheme::doom_nord_light(),
        };
        let labels = match lang_name {
            LangName::En => Labels::en(),
            LangName::Zh => Labels::zh(),
        };
        Self { scheme, labels }
    }
}
