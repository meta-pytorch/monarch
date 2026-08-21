//! End-to-end coverage for Python actor construction from an inherited
//! [`ActorEnvironment`].

use std::time::Duration;

use hyperactor::ActorEnvironment;
use hyperactor::Proc;
use hyperactor::actor::remote::Remote;
use hyperactor::id::Label;
use hyperactor::id::Uid;
use hyperactor_mesh::casting::CAST_POINT;
use monarch_hyperactor::actor::MethodSpecifier;
use monarch_hyperactor::actor::PythonActor;
use monarch_hyperactor::actor::PythonMessage;
use monarch_hyperactor::actor::PythonMessageKind;
use monarch_hyperactor::runtime::GilSite;
use monarch_hyperactor::runtime::monarch_with_gil;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use monarch_types::PickledPyObject;
use ndslice::extent;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use serde::Serialize;

#[derive(Serialize)]
struct PythonActorParamsWire {
    actor_type: PickledPyObject,
    init_message: Option<PythonMessage>,
    mesh_base_name: Option<String>,
}

#[tokio::test]
#[cfg_attr(not(fbcode_build), ignore)]
async fn gspawn_uid_python_actor_init_inherits_cast_point() {
    Python::initialize();

    let (pickled_type, init_message) = monarch_with_gil_blocking(GilSite::Test, |py| {
        py.run(
            c_str!(
                r#"
import monarch._rust_bindings
import monarch._src.actor.actor_mesh

_gspawn_uid_init_rank = None

class GspawnUidInitActor:
    async def handle(
        self, context, method, message, panic_flag, local_state, refs, response_port
    ):
        global _gspawn_uid_init_rank
        _gspawn_uid_init_rank = context.message_rank.rank
"#
            ),
            None,
            None,
        )?;

        let actor_type = py.import("__main__")?.getattr("GspawnUidInitActor")?;
        let init_message = PythonMessage::new_from_buf(
            PythonMessageKind::CallMethod {
                name: MethodSpecifier::Init {},
                response_port: None,
            },
            Vec::<u8>::new(),
        );
        Ok::<_, PyErr>((PickledPyObject::pickle(&actor_type)?, init_message))
    })
    .expect("create Python actor fixture");

    let expected_point = extent!(replicas = 4)
        .point_of_rank(3)
        .expect("rank should be inside the test extent");
    let mut environment = ActorEnvironment::default();
    environment
        .set(CAST_POINT, expected_point.clone())
        .expect("seed parent cast point");
    let proc = Proc::isolated();
    let parent = proc
        .actor_instance_in_environment::<PythonActor>("parent", environment)
        .expect("create parent instance");

    // `PythonActorParams` construction is crate-private. Its remote bincode
    // representation is positional, so this mirror also pins the wire shape
    // consumed by registered `PythonActor` construction.
    let params = PythonActorParamsWire {
        actor_type: pickled_type,
        init_message: Some(init_message),
        mesh_base_name: None,
    };
    let actor_type = Remote::global()
        .name_of::<PythonActor>()
        .expect("PythonActor should be registered for remote spawn");
    let child = parent
        .instance
        .gspawn_uid(
            actor_type,
            Uid::instance(Label::new("python_init_child").expect("valid child label")),
            bincode::serde::encode_to_vec(&params, bincode::config::legacy())
                .expect("serialize PythonActor parameters"),
        )
        .await
        .expect("spawn PythonActor child")
        .into_guard();

    let observed_rank = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let rank = monarch_with_gil(GilSite::Test, |py| {
                py.import("__main__")?
                    .getattr("_gspawn_uid_init_rank")?
                    .extract::<Option<usize>>()
            })
            .await
            .expect("read init rank");
            if let Some(rank) = rank {
                break rank;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "timed out waiting for PythonActor init: {error:?}; child status: {:?}",
            child.status().borrow().clone()
        )
    });

    assert_eq!(
        observed_rank,
        expected_point.rank(),
        "init should receive the parent's environmental cast point",
    );

    child.stop("test complete").expect("stop child actor");
    child.into_inner().await;
}
