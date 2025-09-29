/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This crate provides hyperactor's mesh abstractions.

#![feature(assert_matches)]
#![feature(exit_status_error)]
#![feature(impl_trait_in_bindings)]
#![feature(let_chains)]

pub mod actor_mesh;
pub mod alloc;
mod assign;
pub mod bootstrap;
pub mod comm;
pub mod connect;
pub mod logging;
pub mod mesh;
pub mod mesh_selection;
mod metrics;
pub mod proc_mesh;
pub mod reference;
pub mod resource;
mod router;
pub mod shared_cell;
pub mod shortuuid;
pub mod test_utils;
pub mod v1;

pub use actor_mesh::RootActorMesh;
pub use actor_mesh::SlicedActorMesh;
pub use bootstrap::Bootstrap;
pub use bootstrap::bootstrap;
pub use bootstrap::bootstrap_or_die;
pub use comm::CommActor;
pub use dashmap;
pub use hyperactor_mesh_macros::sel;
pub use mesh::Mesh;
pub use ndslice::extent;
pub use ndslice::sel_from_shape;
pub use ndslice::selection;
pub use ndslice::shape;
pub use proc_mesh::ProcMesh;
pub use proc_mesh::SlicedProcMesh;

#[cfg(test)]
mod tests {

    #[test]
    fn basic() {
        use ndslice::selection::dsl;
        use ndslice::selection::structurally_equal;

        let actual = sel!(*, 0:4, *);
        let expected = dsl::all(dsl::range(
            ndslice::shape::Range(0, Some(4), 1),
            dsl::all(dsl::true_()),
        ));
        assert!(structurally_equal(&actual, &expected));
    }

    #[cfg(FALSE)]
    #[test]
    fn shouldnt_compile() {
        let _ = sel!(foobar);
    }
    // error: sel! parse failed: unexpected token: Ident { sym: foobar, span: #0 bytes(605..611) }
    //   --> fbcode/monarch/hyperactor_mesh_macros/tests/basic.rs:19:13
    //    |
    // 19 |     let _ = sel!(foobar);
    //    |             ^^^^^^^^^^^^ in this macro invocation
    //   --> fbcode/monarch/hyperactor_mesh_macros/src/lib.rs:12:1
    //    |
    //    = note: in this expansion of `sel!`

    use hyperactor_mesh_macros::sel;
    use ndslice::assert_round_trip;
    use ndslice::assert_structurally_eq;
    use ndslice::selection::Selection;

    macro_rules! assert_round_trip_match {
        ($left:expr, $right:expr) => {{
            assert_structurally_eq!($left, $right);
            assert_round_trip!($left);
            assert_round_trip!($right);
        }};
    }

    #[test]
    fn token_parser() {
        use ndslice::selection::dsl::*;
        use ndslice::shape;

        assert_round_trip_match!(all(true_()), sel!(*));
        assert_round_trip_match!(range(3, true_()), sel!(3));
        assert_round_trip_match!(range(1..4, true_()), sel!(1:4));
        assert_round_trip_match!(all(range(1..4, true_())), sel!(*, 1:4));
        assert_round_trip_match!(range(shape::Range(0, None, 1), true_()), sel!(:));
        assert_round_trip_match!(any(true_()), sel!(?));
        assert_round_trip_match!(any(range(1..4, all(true_()))), sel!(?, 1:4, *));
        assert_round_trip_match!(union(range(0, true_()), range(1, true_())), sel!(0 | 1));
        assert_round_trip_match!(
            intersection(range(0..4, true_()), range(2..6, true_())),
            sel!(0:4 & 2:6)
        );
        assert_round_trip_match!(range(shape::Range(0, None, 1), true_()), sel!(:));
        assert_round_trip_match!(all(true_()), sel!(*));
        assert_round_trip_match!(any(true_()), sel!(?));
        assert_round_trip_match!(all(all(all(true_()))), sel!(*, *, *));
        assert_round_trip_match!(intersection(all(true_()), all(true_())), sel!(* & *));
        assert_round_trip_match!(
            all(all(union(
                range(0..2, true_()),
                range(shape::Range(6, None, 1), true_())
            ))),
            sel!(*, *, (:2|6:))
        );
        assert_round_trip_match!(
            all(all(range(shape::Range(1, None, 2), true_()))),
            sel!(*, *, 1::2)
        );
        assert_round_trip_match!(
            range(
                shape::Range(0, Some(1), 1),
                any(range(shape::Range(0, Some(4), 1), true_()))
            ),
            sel!(0, ?, :4)
        );
        assert_round_trip_match!(range(shape::Range(1, Some(4), 2), true_()), sel!(1:4:2));
        assert_round_trip_match!(range(shape::Range(0, None, 2), true_()), sel!(::2));
        assert_round_trip_match!(
            union(range(0..4, true_()), range(4..8, true_())),
            sel!(0:4 | 4:8)
        );
        assert_round_trip_match!(
            intersection(range(0..4, true_()), range(2..6, true_())),
            sel!(0:4 & 2:6)
        );
        assert_round_trip_match!(
            all(union(range(1..4, all(true_())), range(5..6, all(true_())))),
            sel!(*, (1:4 | 5:6), *)
        );
        assert_round_trip_match!(
            range(
                0,
                intersection(
                    range(1..4, range(7, true_())),
                    range(2..5, range(7, true_()))
                )
            ),
            sel!(0, (1:4 & 2:5), 7)
        );
        assert_round_trip_match!(
            all(all(union(
                union(range(0..2, true_()), range(4..6, true_())),
                range(shape::Range(6, None, 1), true_())
            ))),
            sel!(*, *, (:2 | 4:6 | 6:))
        );
        assert_round_trip_match!(intersection(all(true_()), all(true_())), sel!(* & *));
        assert_round_trip_match!(union(all(true_()), all(true_())), sel!(* | *));
        assert_round_trip_match!(
            intersection(
                range(0..2, true_()),
                union(range(1, true_()), range(2, true_()))
            ),
            sel!(0:2 & (1 | 2))
        );
        assert_round_trip_match!(
            all(all(intersection(
                range(1..2, true_()),
                range(2..3, true_())
            ))),
            sel!(*,*,(1:2&2:3))
        );
        assert_round_trip_match!(
            intersection(all(all(all(true_()))), all(all(all(true_())))),
            sel!((*,*,*) & (*,*,*))
        );
        assert_round_trip_match!(
            intersection(
                range(0, all(all(true_()))),
                range(0, union(range(1, all(true_())), range(3, all(true_()))))
            ),
            sel!((0, *, *) & (0, (1 | 3), *))
        );
        assert_round_trip_match!(
            intersection(
                range(0, all(all(true_()))),
                range(
                    0,
                    union(
                        range(1, range(2..5, true_())),
                        range(3, range(2..5, true_()))
                    )
                )
            ),
            sel!((0, *, *) & (0, (1 | 3), 2:5))
        );
        assert_round_trip_match!(all(true_()), sel!((*)));
        assert_round_trip_match!(range(1..4, range(2, true_())), sel!(((1:4), 2)));
        assert_round_trip_match!(sel!(1:4 & 5:6 | 7:8), sel!((1:4 & 5:6) | 7:8));
        assert_round_trip_match!(
            union(
                intersection(all(all(true_())), all(all(true_()))),
                all(all(true_()))
            ),
            sel!((*,*) & (*,*) | (*,*))
        );
        assert_round_trip_match!(all(true_()), sel!(*));
        assert_round_trip_match!(sel!(((1:4))), sel!(1:4));
        assert_round_trip_match!(sel!(*, (*)), sel!(*, *));
        assert_round_trip_match!(
            intersection(
                range(0, range(1..4, true_())),
                range(0, union(range(2, all(true_())), range(3, all(true_()))))
            ),
            sel!((0,1:4)&(0,(2|3),*))
        );

        //assert_round_trip_match!(true_(), sel!(foo)); // sel! macro: parse error: Parsing Error: Error { input: "foo", code: Tag }

        assert_round_trip_match!(
            sel!(0 & (0, (1|3), *)),
            intersection(
                range(0, true_()),
                range(0, union(range(1, all(true_())), range(3, all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!(0 & (0, (3|1), *)),
            intersection(
                range(0, true_()),
                range(0, union(range(3, all(true_())), range(1, all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((*, *, *) & (*, *, (2 | 4))),
            intersection(
                all(all(all(true_()))),
                all(all(union(range(2, true_()), range(4, true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((*, *, *) & (*, *, (4 | 2))),
            intersection(
                all(all(all(true_()))),
                all(all(union(range(4, true_()), range(2, true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((*, (1|2)) & (*, (2|1))),
            intersection(
                all(union(range(1, true_()), range(2, true_()))),
                all(union(range(2, true_()), range(1, true_())))
            )
        );
        assert_round_trip_match!(
            sel!((*, *, *) & *),
            intersection(all(all(all(true_()))), all(true_()))
        );
        assert_round_trip_match!(
            sel!(* & (*, *, *)),
            intersection(all(true_()), all(all(all(true_()))))
        );

        assert_round_trip_match!(
            sel!( (*, *, *) & ((*, *, *) & (*, *, *)) ),
            intersection(
                all(all(all(true_()))),
                intersection(all(all(all(true_()))), all(all(all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!((1, *, *) | (0 & (0, 3, *))),
            union(
                range(1, all(all(true_()))),
                intersection(range(0, true_()), range(0, range(3, all(true_()))))
            )
        );
        assert_round_trip_match!(
            sel!(((0, *)| (1, *)) & ((1, *) | (0, *))),
            intersection(
                union(range(0, all(true_())), range(1, all(true_()))),
                union(range(1, all(true_())), range(0, all(true_())))
            )
        );
        assert_round_trip_match!(sel!(*, 8:8), all(range(8..8, true_())));
        assert_round_trip_match!(
            sel!((*, 1) & (*, 8 : 8)),
            intersection(all(range(1..2, true_())), all(range(8..8, true_())))
        );
        assert_round_trip_match!(
            sel!((*, 8 : 8) | (*, 1)),
            union(all(range(8..8, true_())), all(range(1..2, true_())))
        );
        assert_round_trip_match!(
            sel!((*, 1) | (*, 2:8)),
            union(all(range(1..2, true_())), all(range(2..8, true_())))
        );
        assert_round_trip_match!(
            sel!((*, *, *) & (*, *, 2:8)),
            intersection(all(all(all(true_()))), all(all(range(2..8, true_()))))
        );
    }
}
