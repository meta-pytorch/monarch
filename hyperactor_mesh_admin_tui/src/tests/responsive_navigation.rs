/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;
use hyperactor_mesh::introspect::dto::NodePayloadDto;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::*;

#[derive(Clone)]
struct ServerState {
    responses: Arc<HashMap<String, NodePayloadDto>>,
    requests: mpsc::UnboundedSender<String>,
    request_history: Arc<Mutex<Vec<String>>>,
    holds: Arc<HashMap<String, watch::Receiver<bool>>>,
}

struct HeldAdminServer {
    base_url: String,
    requests: mpsc::UnboundedReceiver<String>,
    request_history: Arc<Mutex<Vec<String>>>,
    releases: HashMap<String, watch::Sender<bool>>,
    task: JoinHandle<()>,
}

impl HeldAdminServer {
    async fn spawn(
        responses: HashMap<String, NodePayloadDto>,
        held_references: impl IntoIterator<Item = NodeRef>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind an ephemeral port");
        let address = listener
            .local_addr()
            .expect("test server should have a local address");
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let request_history = Arc::new(Mutex::new(Vec::new()));
        let mut releases = HashMap::new();
        let holds = held_references
            .into_iter()
            .map(|reference| {
                let reference = reference.to_string();
                let (release, held) = watch::channel(false);
                releases.insert(reference.clone(), release);
                (reference, held)
            })
            .collect();
        let state = ServerState {
            responses: Arc::new(responses),
            requests: request_tx,
            request_history: Arc::clone(&request_history),
            holds: Arc::new(holds),
        };
        let router = Router::new()
            .route("/v1/{*reference}", get(held_node))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test admin server should run");
        });

        Self {
            base_url: format!("http://{address}"),
            requests: request_rx,
            request_history,
            releases,
            task,
        }
    }

    async fn next_request(&mut self) -> String {
        self.requests
            .recv()
            .await
            .expect("detail request should reach the test server")
    }

    async fn wait_for_request(&mut self, expected: &NodeRef) {
        let expected = expected.to_string();
        loop {
            if self.next_request().await == expected {
                return;
            }
        }
    }

    fn release(&self, reference: &NodeRef) {
        self.releases
            .get(&reference.to_string())
            .expect("reference should have a configured hold")
            .send(true)
            .expect("test server should still hold the response");
    }

    fn request_count(&self, reference: &NodeRef) -> usize {
        let expected = reference.to_string();
        self.request_history
            .lock()
            .expect("request history lock should not be poisoned")
            .iter()
            .filter(|actual| **actual == expected)
            .count()
    }

    fn total_request_count(&self) -> usize {
        self.request_history
            .lock()
            .expect("request history lock should not be poisoned")
            .len()
    }

    fn assert_no_additional_request(&mut self) {
        assert!(
            matches!(
                self.requests.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ),
            "rapid navigation should not issue detail requests for crossed rows"
        );
    }
}

impl Drop for HeldAdminServer {
    fn drop(&mut self) {
        self.releases.values().for_each(|release| {
            let _ = release.send(true);
        });
        self.task.abort();
    }
}

async fn held_node(
    Path(reference): Path<String>,
    State(state): State<ServerState>,
) -> Result<Json<NodePayloadDto>, StatusCode> {
    let response = state
        .responses
        .get(&reference)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    state
        .request_history
        .lock()
        .expect("request history lock should not be poisoned")
        .push(reference.clone());
    let _ = state.requests.send(reference.clone());
    if let Some(held) = state.holds.get(&reference) {
        let mut held = held.clone();
        let released = *held.borrow();
        if !released {
            let _ = held.changed().await;
        }
    }
    Ok(Json(response))
}

struct TestTopology {
    tree: TreeNode,
    responses: HashMap<String, NodePayloadDto>,
    target_proc: NodeRef,
    target_actor: NodeRef,
}

fn actor_ref_for(proc_name: &str, actor_name: &str) -> NodeRef {
    NodeRef::Actor(proc_addr(proc_name).actor_addr(actor_name))
}

fn large_topology() -> TestTopology {
    let target_proc_name = "host-0-proc-0";
    let target_proc = proc_ref(target_proc_name);
    let target_actor = actor_ref_for(target_proc_name, "detail-target");
    let mut responses = HashMap::new();
    let mut root_children = Vec::new();
    let mut tree_hosts = Vec::new();

    for host_index in 0..25 {
        let host_reference = host(&format!("host-{host_index}"));
        let mut proc_references = Vec::new();
        let mut tree_procs = Vec::new();

        for proc_index in 0..4 {
            let proc_name = format!("host-{host_index}-proc-{proc_index}");
            let proc_reference = proc_ref(&proc_name);
            let is_target = proc_reference == target_proc;
            let actor_children = if is_target {
                vec![target_actor.clone()]
            } else {
                Vec::new()
            };
            responses.insert(
                proc_reference.to_string(),
                NodePayloadDto::from(NodePayload {
                    identity: proc_reference.clone(),
                    properties: NodeProperties::Proc {
                        proc_name,
                        num_actors: actor_children.len(),
                        system_children: Vec::new(),
                        stopped_children: Vec::new(),
                        stopped_retention_cap: 0,
                        is_poisoned: false,
                        failed_actor_count: 0,
                        debug: Default::default(),
                    },
                    children: actor_children.clone(),
                    parent: Some(host_reference.clone()),
                    as_of: SystemTime::UNIX_EPOCH,
                }),
            );
            proc_references.push(proc_reference.clone());
            tree_procs.push(TreeNode {
                reference: proc_reference,
                label: format!("proc {proc_index}"),
                node_type: NodeType::Proc,
                expanded: is_target,
                fetched: true,
                has_children: is_target,
                stopped: false,
                failed: false,
                is_system: false,
                children: if is_target {
                    vec![TreeNode {
                        reference: target_actor.clone(),
                        label: "detail target".to_string(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: false,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: Vec::new(),
                    }]
                } else {
                    Vec::new()
                },
            });
        }

        responses.insert(
            host_reference.to_string(),
            NodePayloadDto::from(NodePayload {
                identity: host_reference.clone(),
                properties: NodeProperties::Host {
                    addr: format!("host-{host_index}"),
                    num_procs: proc_references.len(),
                    system_children: Vec::new(),
                    memory: Default::default(),
                },
                children: proc_references,
                parent: Some(NodeRef::Root),
                as_of: SystemTime::UNIX_EPOCH,
            }),
        );
        root_children.push(host_reference.clone());
        tree_hosts.push(TreeNode {
            reference: host_reference,
            label: format!("host {host_index}"),
            node_type: NodeType::Host,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: tree_procs,
        });
    }

    responses.insert(
        NodeRef::Root.to_string(),
        NodePayloadDto::from(NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Root {
                num_hosts: root_children.len(),
                started_at: SystemTime::UNIX_EPOCH,
                started_by: "responsive-navigation-test".to_string(),
                system_children: Vec::new(),
            },
            children: root_children,
            parent: None,
            as_of: SystemTime::UNIX_EPOCH,
        }),
    );
    responses.insert(
        target_actor.to_string(),
        NodePayloadDto::from(NodePayload {
            identity: target_actor.clone(),
            properties: NodeProperties::Actor {
                actor_status: "running".to_string(),
                actor_type: "ResponsiveNavigationActor".to_string(),
                instance_id: String::new(),
                messages_processed: 0,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                queue_depth: 0,
                flight_recorder: None,
                is_system: false,
                inbound_ordering: None,
                failure_info: None,
                execution: None,
            },
            children: Vec::new(),
            parent: Some(target_proc.clone()),
            as_of: SystemTime::UNIX_EPOCH,
        }),
    );

    TestTopology {
        tree: TreeNode {
            reference: NodeRef::Root,
            label: "Root".to_string(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: tree_hosts,
        },
        responses,
        target_proc,
        target_actor,
    }
}

fn detail_payload(reference: NodeRef) -> NodePayload {
    NodePayload {
        identity: reference,
        properties: NodeProperties::Error {
            code: "held_for_test".to_string(),
            message: "response is controlled by the test".to_string(),
        },
        children: Vec::new(),
        parent: None,
        as_of: SystemTime::now(),
    }
}

fn app_with_tree(base_url: String, tree: TreeNode, detail_debounce: std::time::Duration) -> App {
    let mut policy = test_policy();
    policy.detail_debounce = detail_debounce;
    let mut app = App::new(
        base_url,
        reqwest::Client::new(),
        ThemeName::Nord,
        LangName::En,
        policy,
    );
    app.set_tree(Some(tree));
    app.cursor.update_len(app.visible_rows().len());
    app
}

fn select_reference(app: &mut App, reference: &NodeRef) {
    let position = app
        .visible_rows()
        .as_slice()
        .iter()
        .position(|row| &row.node.reference == reference)
        .expect("reference should be visible in the test topology");
    app.cursor.set_pos(position);
}

#[tokio::test]
async fn navigation_does_not_wait_for_detail_fetch() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let held_reference = flatten_tree(&tree)[6].node.reference.clone();
    let mut server = HeldAdminServer::spawn(responses, [held_reference.clone()]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);

    let navigation = tokio::spawn(async move {
        for _ in 0..6 {
            let result = app.on_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
            app.apply_key_result(result).await;
        }
        app
    });

    server.wait_for_request(&held_reference).await;
    tokio::task::yield_now().await;

    assert!(
        navigation.is_finished(),
        "cursor navigation waited for the held detail response"
    );
    let app = navigation
        .await
        .expect("navigation task should finish without the held response");
    assert_eq!(app.cursor.pos(), 6, "all Down keys should be processed");
    let selected = app
        .selected_reference()
        .expect("the final cursor position should select a node");
    assert_eq!(&held_reference, selected);
    assert!(
        matches!(&app.detail, DetailState::Loading),
        "the final cold selection should render as loading"
    );
    server.release(&held_reference);
}

#[tokio::test(start_paused = true)]
async fn detail_request_waits_for_debounce() {
    let TestTopology {
        tree, target_actor, ..
    } = large_topology();
    let debounce = std::time::Duration::from_millis(100);
    let mut app = app_with_tree("::invalid-url::".to_string(), tree, debounce);
    select_reference(&mut app, &target_actor);

    app.schedule_selected_detail();
    tokio::task::yield_now().await;
    assert_eq!(app.detail_request_is_finished(), Some(false));

    tokio::time::advance(debounce - std::time::Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(app.detail_request_is_finished(), Some(false));

    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    let result = app.receive_pending_detail_for_test().await;
    assert!(matches!(result.state, FetchState::Error { .. }));
    app.apply_detail_result(result);
}

#[tokio::test(start_paused = true)]
async fn rapid_navigation_fetches_only_final_selection() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let final_reference = flatten_tree(&tree)[6].node.reference.clone();
    let mut server = HeldAdminServer::spawn(responses, [final_reference.clone()]).await;
    let debounce = std::time::Duration::from_millis(100);
    let mut app = app_with_tree(server.base_url.clone(), tree, debounce);

    for _ in 0..6 {
        let result = app.on_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        app.apply_key_result(result).await;
        tokio::task::yield_now().await;
    }

    assert_eq!(
        server.total_request_count(),
        0,
        "crossed rows should remain inside the debounce window"
    );
    tokio::time::advance(debounce - std::time::Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(server.total_request_count(), 0);

    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    assert_eq!(server.next_request().await, final_reference.to_string());
    assert_eq!(server.request_count(&final_reference), 1);
    server.assert_no_additional_request();
    server.release(&final_reference);
}

async fn wait_for_detail_request_to_finish(app: &App) {
    for _ in 0..100 {
        if app.detail_request_is_finished() == Some(true) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("detail request should finish after its held response is released");
}

async fn run_refresh(app: &mut App) {
    app.request_refresh();
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);
    app.start_pending_refresh_if_allowed();
}

#[tokio::test]
async fn refresh_does_not_cancel_pending_detail_fetch() {
    let TestTopology {
        tree,
        responses,
        target_actor,
        ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [target_actor.clone()]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    select_reference(&mut app, &target_actor);
    app.schedule_selected_detail();
    server.wait_for_request(&target_actor).await;
    let token = app.current_detail_token();

    for _ in 0..3 {
        run_refresh(&mut app).await;
        assert_eq!(app.pending_detail_reference(), Some(&target_actor));
        assert_eq!(app.current_detail_token(), token);
    }
    assert_eq!(
        server.request_count(&target_actor),
        1,
        "refresh should retain one detail request for the selected actor"
    );

    server.release(&target_actor);
    let result = app.receive_pending_detail_for_test().await;
    app.apply_detail_result(result);
    assert!(matches!(
        app.detail,
        DetailState::Ready {
            freshness: DetailFreshness::Fresh,
            ..
        }
    ));
    assert!(matches!(
        app.fetch_cache.get(&target_actor),
        Some(FetchState::Ready { generation, .. }) if *generation == app.refresh_gen
    ));

    app.schedule_selected_detail();
    assert!(app.pending_detail_reference().is_none());
    assert_eq!(server.request_count(&target_actor), 1);
}

#[tokio::test]
async fn refresh_preserves_completed_detail_result() {
    let TestTopology {
        tree,
        responses,
        target_actor,
        ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [target_actor.clone()]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    select_reference(&mut app, &target_actor);
    app.schedule_selected_detail();
    server.wait_for_request(&target_actor).await;
    let token = app.current_detail_token();
    server.release(&target_actor);
    wait_for_detail_request_to_finish(&app).await;

    run_refresh(&mut app).await;

    assert_eq!(app.pending_detail_reference(), Some(&target_actor));
    assert_eq!(app.current_detail_token(), token);
    assert_eq!(server.request_count(&target_actor), 1);
    let result = app.receive_pending_detail_for_test().await;
    app.apply_detail_result(result);
    assert!(matches!(
        app.detail,
        DetailState::Ready {
            freshness: DetailFreshness::Fresh,
            ..
        }
    ));
}

#[tokio::test]
async fn refresh_supersedes_redundant_detail_request() {
    let TestTopology {
        tree,
        responses,
        target_proc,
        ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [target_proc.clone()]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    select_reference(&mut app, &target_proc);
    app.schedule_selected_detail();
    server.wait_for_request(&target_proc).await;
    let token = app.current_detail_token();
    server.release(&target_proc);
    wait_for_detail_request_to_finish(&app).await;

    run_refresh(&mut app).await;

    assert!(app.pending_detail_reference().is_none());
    assert_ne!(app.current_detail_token(), token);
    assert!(matches!(
        &app.detail,
        DetailState::Ready {
            payload,
            freshness: DetailFreshness::Fresh,
        } if payload.identity == target_proc
    ));
    assert_eq!(server.request_count(&target_proc), 2);
}

#[test]
fn fresh_cached_detail_is_displayed_without_a_request() {
    let tree = large_topology().tree;
    let mut app = app_with_tree(
        "http://127.0.0.1:1".to_string(),
        tree,
        std::time::Duration::from_secs(60),
    );
    let reference = app
        .selected_reference()
        .expect("large tree should have a selection")
        .clone();
    app.fetch_cache.insert(
        reference.clone(),
        FetchState::Ready {
            stamp: app.stamps.next(),
            generation: app.refresh_gen,
            value: detail_payload(reference),
        },
    );

    app.schedule_selected_detail();

    assert!(app.pending_detail_reference().is_none());
    assert!(matches!(
        app.detail,
        DetailState::Ready {
            freshness: DetailFreshness::Fresh,
            ..
        }
    ));
}

#[tokio::test]
async fn stale_cached_detail_is_displayed_while_revalidating() {
    let tree = large_topology().tree;
    let mut app = app_with_tree(
        "http://127.0.0.1:1".to_string(),
        tree,
        std::time::Duration::from_secs(60),
    );
    let reference = app
        .selected_reference()
        .expect("large tree should have a selection")
        .clone();
    app.fetch_cache.insert(
        reference.clone(),
        FetchState::Ready {
            stamp: app.stamps.next(),
            generation: 0,
            value: detail_payload(reference.clone()),
        },
    );
    app.refresh_gen = 1;

    app.schedule_selected_detail();

    assert_eq!(app.pending_detail_reference(), Some(&reference));
    assert!(matches!(
        app.detail,
        DetailState::Ready {
            freshness: DetailFreshness::Revalidating,
            ..
        }
    ));
}

#[tokio::test]
async fn stale_detail_result_does_not_replace_current_selection() {
    let tree = large_topology().tree;
    let mut app = app_with_tree(
        "http://127.0.0.1:1".to_string(),
        tree,
        std::time::Duration::from_secs(60),
    );

    app.schedule_selected_detail();
    let stale_reference = app
        .selected_reference()
        .expect("large tree should have an initial selection")
        .clone();
    let stale_token = app.current_detail_token();

    assert!(app.cursor.move_down());
    app.schedule_selected_detail();
    let current_reference = app
        .selected_reference()
        .expect("second row should be selected")
        .clone();

    app.apply_detail_result(DetailFetchResult {
        reference: stale_reference.clone(),
        token: stale_token,
        state: FetchState::Ready {
            stamp: app.stamps.next(),
            generation: app.refresh_gen,
            value: detail_payload(stale_reference),
        },
    });

    assert_eq!(app.pending_detail_reference(), Some(&current_reference));
    assert!(matches!(app.detail, DetailState::Loading));
}

#[tokio::test]
async fn failed_revalidation_keeps_cached_detail_visible() {
    let tree = large_topology().tree;
    let mut app = app_with_tree(
        "http://127.0.0.1:1".to_string(),
        tree,
        std::time::Duration::from_secs(60),
    );
    let reference = app
        .selected_reference()
        .expect("large tree should have a selection")
        .clone();
    app.fetch_cache.insert(
        reference.clone(),
        FetchState::Ready {
            stamp: app.stamps.next(),
            generation: 0,
            value: detail_payload(reference.clone()),
        },
    );
    app.refresh_gen = 1;
    app.schedule_selected_detail();
    let token = app.current_detail_token();

    app.apply_detail_result(DetailFetchResult {
        reference,
        token,
        state: FetchState::Error {
            stamp: app.stamps.next(),
            msg: "injected failure".to_string(),
        },
    });

    assert!(matches!(
        app.detail,
        DetailState::Ready {
            freshness: DetailFreshness::Stale { ref message },
            ..
        } if message.contains("injected failure")
    ));
}

#[test]
fn detail_pane_renders_loading_state() {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    let mut app = app_with_tree(
        "http://127.0.0.1:1".to_string(),
        large_topology().tree,
        std::time::Duration::ZERO,
    );
    app.detail = DetailState::Loading;
    let backend = TestBackend::new(60, 10);
    let mut terminal = Terminal::new(backend).expect("test terminal should initialize");
    terminal
        .draw(|frame| crate::render::detail_pane::render_detail_pane(frame, frame.area(), &app))
        .expect("detail pane should render");
    let buffer = terminal.backend().buffer();
    let text = (0..buffer.area.height)
        .flat_map(|y| (0..buffer.area.width).map(move |x| buffer[(x, y)].symbol()))
        .collect::<String>();

    assert!(text.contains("Loading…"));
}

#[tokio::test]
async fn refresh_does_not_block_navigation() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    let initial_generation = app.refresh_gen;

    app.request_refresh();

    server.wait_for_request(&NodeRef::Root).await;
    let result = app.on_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    app.apply_key_result(result).await;

    assert_eq!(app.cursor.pos(), 1, "Down should move immediately");
    assert!(app.refresh_is_in_flight());
    assert_eq!(app.refresh_gen, initial_generation);
    let selected = app
        .selected_reference()
        .expect("the moved cursor should select a row")
        .clone();

    server.release(&NodeRef::Root);
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);

    assert_eq!(app.selected_reference(), Some(&selected));
}

#[tokio::test]
async fn refresh_keeps_previous_tree_visible() {
    let TestTopology {
        tree,
        mut responses,
        ..
    } = large_topology();
    let retained_host = host("host-0");
    responses.insert(
        NodeRef::Root.to_string(),
        NodePayloadDto::from(NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Root {
                num_hosts: 1,
                started_at: SystemTime::UNIX_EPOCH,
                started_by: "responsive-navigation-test".to_string(),
                system_children: Vec::new(),
            },
            children: vec![retained_host],
            parent: None,
            as_of: SystemTime::UNIX_EPOCH,
        }),
    );
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    let previous_rows = app.visible_rows().len();

    app.request_refresh();
    server.wait_for_request(&NodeRef::Root).await;

    assert_eq!(app.visible_rows().len(), previous_rows);

    server.release(&NodeRef::Root);
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);

    assert!(app.visible_rows().len() < previous_rows);
}

#[tokio::test]
async fn refresh_removes_proc_missing_from_host_introspection() {
    let TestTopology {
        tree,
        mut responses,
        target_proc,
        ..
    } = large_topology();
    let host_reference = host("host-0");
    let cached_target = NodePayload::try_from(
        responses
            .get(&target_proc.to_string())
            .expect("target proc response should exist")
            .clone(),
    )
    .expect("target proc response should be valid");
    let remaining_procs = (1..4)
        .map(|proc_index| proc_ref(&format!("host-0-proc-{proc_index}")))
        .collect::<Vec<_>>();
    responses.insert(
        host_reference.to_string(),
        NodePayloadDto::from(NodePayload {
            identity: host_reference.clone(),
            properties: NodeProperties::Host {
                addr: "host-0".to_string(),
                num_procs: remaining_procs.len(),
                system_children: Vec::new(),
                memory: Default::default(),
            },
            children: remaining_procs,
            parent: Some(NodeRef::Root),
            as_of: SystemTime::UNIX_EPOCH,
        }),
    );
    let server = HeldAdminServer::spawn(responses, []).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    select_reference(&mut app, &target_proc);
    app.fetch_cache.insert(
        target_proc.clone(),
        FetchState::Ready {
            stamp: app.stamps.next(),
            generation: app.refresh_gen,
            value: cached_target,
        },
    );

    run_refresh(&mut app).await;

    let host_node = app
        .tree()
        .and_then(|root| {
            root.children
                .iter()
                .find(|node| node.reference == host_reference)
        })
        .expect("host should remain in the refreshed tree");
    assert_eq!(host_node.children.len(), 3);
    assert!(
        host_node
            .children
            .iter()
            .all(|node| node.reference != target_proc),
        "a gracefully stopping proc should disappear when HostAgent stops advertising it",
    );
    assert_ne!(app.selected_reference(), Some(&target_proc));
    assert!(app.selected_reference().is_some());
    assert!(!app.fetch_cache.contains_key(&target_proc));
    assert_eq!(server.request_count(&target_proc), 0);
}

#[tokio::test]
async fn refresh_ticks_are_coalesced_while_one_is_in_flight() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);

    app.request_refresh();
    server.wait_for_request(&NodeRef::Root).await;
    for _ in 0..5 {
        app.request_refresh();
    }

    assert!(app.refresh_is_in_flight());
    assert!(app.refresh_is_pending());

    server.release(&NodeRef::Root);
    let first = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(first);
    app.start_pending_refresh_if_allowed();
    server.wait_for_request(&NodeRef::Root).await;

    let second = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(second);
    app.start_pending_refresh_if_allowed();

    assert_eq!(server.request_count(&NodeRef::Root), 2);
    assert!(!app.refresh_is_in_flight());
    assert!(!app.refresh_is_pending());
}

#[tokio::test]
async fn completed_refresh_preserves_selection_by_reference() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);

    app.request_refresh();
    server.wait_for_request(&NodeRef::Root).await;
    for _ in 0..7 {
        let result = app.on_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        app.apply_key_result(result).await;
    }
    let selected = app
        .selected_reference()
        .expect("navigation should leave a selected row")
        .clone();

    server.release(&NodeRef::Root);
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);

    assert_eq!(app.selected_reference(), Some(&selected));
}

#[tokio::test]
async fn refresh_completion_is_deferred_across_foreground_overlay() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let server = HeldAdminServer::spawn(responses, []).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    let generation = app.refresh_gen;

    app.request_refresh();
    let completion = app.receive_pending_refresh_for_test().await;
    app.set_job(ActiveJob::PySpy {
        rx: None,
        short: "worker[0]".to_string(),
        lines: vec![],
        completed_at: None,
    });
    app.apply_refresh_completion(completion);

    assert_eq!(app.refresh_gen, generation);
    assert!(app.refresh_is_pending());
    assert!(!app.refresh_is_in_flight());

    app.dismiss_job();
    app.start_pending_refresh_if_allowed();
    assert!(app.refresh_is_in_flight());
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);
    assert_eq!(app.refresh_gen, generation + 1);
}

#[tokio::test]
async fn startup_schedules_detail_for_initial_selection() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let selected = flatten_tree(&tree)[0].node.reference.clone();
    let mut server = HeldAdminServer::spawn(responses, [selected.clone()]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);

    app.seed_initial_detail();

    server.wait_for_request(&selected).await;
    assert_eq!(app.pending_detail_reference(), Some(&selected));
    server.release(&selected);
}

#[tokio::test]
async fn background_refresh_does_not_mark_visible_rows_stale() {
    let TestTopology {
        tree,
        responses,
        target_actor,
        ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    select_reference(&mut app, &target_actor);
    app.fetch_cache.insert(
        target_actor.clone(),
        FetchState::Ready {
            stamp: app.stamps.next(),
            generation: app.refresh_gen,
            value: detail_payload(target_actor.clone()),
        },
    );

    app.request_refresh();
    server.wait_for_request(&NodeRef::Root).await;
    app.schedule_selected_detail();

    assert!(app.pending_detail_reference().is_none());
    assert_eq!(server.request_count(&target_actor), 0);
    assert!(matches!(
        app.detail,
        DetailState::Ready {
            freshness: DetailFreshness::Fresh,
            ..
        }
    ));
    server.release(&NodeRef::Root);
}

#[tokio::test]
async fn discarded_snapshot_does_not_advance_generation() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let server = HeldAdminServer::spawn(responses, []).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    let generation = app.refresh_gen;

    app.request_refresh();
    let completion = app.receive_pending_refresh_for_test().await;
    app.set_job(ActiveJob::PySpy {
        rx: None,
        short: "worker[0]".to_string(),
        lines: vec![],
        completed_at: None,
    });
    app.apply_refresh_completion(completion);

    assert_eq!(app.refresh_gen, generation);
}

#[tokio::test]
async fn filter_change_supersedes_in_flight_refresh() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);

    app.request_refresh();
    server.wait_for_request(&NodeRef::Root).await;
    let first_revision = app
        .current_refresh_view_revision()
        .expect("the first refresh should record its view revision");

    let result = app.on_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    app.apply_key_result(result).await;
    server.wait_for_request(&NodeRef::Root).await;

    assert!(app.show_system);
    assert!(
        app.current_refresh_view_revision()
            .is_some_and(|revision| revision > first_revision)
    );
    assert_eq!(app.refresh_gen, 0);
    assert_eq!(server.request_count(&NodeRef::Root), 2);

    server.release(&NodeRef::Root);
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);
}

#[tokio::test]
async fn collapse_supersedes_in_flight_refresh() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let mut server = HeldAdminServer::spawn(responses, [NodeRef::Root]).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);
    assert!(
        app.tree()
            .and_then(|root| root.children.first())
            .is_some_and(|host| host.expanded)
    );

    app.request_refresh();
    server.wait_for_request(&NodeRef::Root).await;
    let result = app.on_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE));
    app.apply_key_result(result).await;
    server.wait_for_request(&NodeRef::Root).await;

    assert!(
        app.tree()
            .and_then(|root| root.children.first())
            .is_some_and(|host| !host.expanded)
    );

    server.release(&NodeRef::Root);
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);

    assert!(
        app.tree()
            .and_then(|root| root.children.first())
            .is_some_and(|host| !host.expanded)
    );
}

#[tokio::test]
async fn stale_refresh_completion_does_not_cancel_current_refresh() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let server = HeldAdminServer::spawn(responses, []).await;
    let mut app = app_with_tree(server.base_url.clone(), tree, std::time::Duration::ZERO);

    app.request_refresh();
    let stale_revision = app
        .current_refresh_view_revision()
        .expect("the first refresh should record its view revision");
    let stale_completion = app.receive_pending_refresh_for_test().await;

    let result = app.on_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE));
    app.apply_key_result(result).await;
    app.start_pending_refresh_if_allowed();
    assert!(
        app.current_refresh_view_revision()
            .is_some_and(|revision| revision > stale_revision)
    );

    app.apply_refresh_completion(stale_completion);

    assert!(app.refresh_is_in_flight());
}

#[tokio::test]
async fn failed_refresh_preserves_last_good_tree_and_allows_retry() {
    let TestTopology {
        tree, responses, ..
    } = large_topology();
    let server = HeldAdminServer::spawn(responses, []).await;
    let mut app = app_with_tree(
        "::invalid-url::".to_string(),
        tree,
        std::time::Duration::ZERO,
    );
    let expected_references: Vec<_> = app
        .visible_rows()
        .as_slice()
        .iter()
        .map(|row| row.node.reference.clone())
        .collect();
    let generation = app.refresh_gen;

    run_refresh(&mut app).await;

    assert_eq!(app.refresh_gen, generation);
    assert_eq!(
        app.visible_rows()
            .as_slice()
            .iter()
            .map(|row| row.node.reference.clone())
            .collect::<Vec<_>>(),
        expected_references
    );
    assert!(!app.refresh_is_in_flight());

    app.base_url = server.base_url.clone();
    run_refresh(&mut app).await;
    assert_eq!(app.refresh_gen, generation + 1);

    let generation = app.refresh_gen;
    app.install_panicking_refresh_for_test();
    let completion = app.receive_pending_refresh_for_test().await;
    app.apply_refresh_completion(completion);
    assert_eq!(app.refresh_gen, generation);
    assert!(!app.refresh_is_in_flight());

    run_refresh(&mut app).await;
    assert_eq!(app.refresh_gen, generation + 1);
}

#[tokio::test]
async fn in_flight_refresh_is_cancelled_when_owner_is_dropped() {
    let mut app = app_with_tree(
        "http://127.0.0.1:1".to_string(),
        large_topology().tree,
        std::time::Duration::ZERO,
    );
    let (dropped, cancelled) = tokio::sync::oneshot::channel();
    app.install_pending_refresh_for_test(dropped);

    drop(app);

    tokio::time::timeout(std::time::Duration::from_secs(1), cancelled)
        .await
        .expect("refresh cancellation should be bounded")
        .expect("refresh task should report that its future was dropped");
}
