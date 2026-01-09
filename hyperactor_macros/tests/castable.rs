/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)]

use hyperactor::Bind;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor::message::Bind;
use hyperactor::message::Unbind;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(Bind, Unbind)]
struct MyUnitStruct;

#[derive(Bind, Unbind)]
struct EmptyNamedStruct {}

#[derive(Bind, Unbind)]
struct EmptyUnamedStruct();

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Named)]
struct MyReply(String);

#[derive(Clone, Debug, PartialEq, Bind, Unbind)]
struct MyNamedStruct {
    field0: u64,
    field1: MyReply,
    #[binding(include)]
    field2: PortRef<MyReply>,
    field3: bool,
    #[binding(include)]
    field4: hyperactor::PortRef<u64>,
}

#[derive(Clone, Debug, PartialEq, Bind, Unbind)]
struct MyUnamedStruct(
    u64,
    MyReply,
    #[binding(include)] hyperactor::PortRef<MyReply>,
    bool,
    #[binding(include)] PortRef<u64>,
);

#[derive(Clone, Debug, PartialEq, Bind, Unbind)]
struct MyGenericStruct<'a, A: Bind + Unbind, B>(#[binding(include)] A, &'a B, A);

#[derive(Clone, Debug, PartialEq, Bind, Unbind)]
enum MyEnum {
    Unit,
    EmptyStruct {},
    EmptyTuple(),
    NoopTuple(u64, bool),
    NoopStruct {
        field0: u64,
        field1: bool,
    },
    Tuple(
        u64,
        MyReply,
        #[binding(include)] PortRef<MyReply>,
        bool,
        #[binding(include)] hyperactor::PortRef<u64>,
    ),
    Struct {
        field0: u64,
        field1: MyReply,
        #[binding(include)]
        field2: PortRef<MyReply>,
        field3: bool,
        #[binding(include)]
        field4: hyperactor::PortRef<u64>,
    },
}

#[derive(Clone, Debug, PartialEq, Bind, Unbind)]
enum MyGenericEnum<'a, A: Bind + Unbind, B> {
    Unit,
    EmptyStruct {},
    EmptyTuple(),
    Tuple(#[binding(include)] A, &'a B, A),
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use hyperactor::id;
    use hyperactor::message::Bind;
    use hyperactor::message::Bindings;
    use hyperactor::message::Unbind;
    use hyperactor::message::Unbound;

    use super::*;

    fn verify<T: Bind + Unbind + Clone + PartialEq + Debug>(my_type: T, bindings: Bindings) {
        let unbound = Unbound::try_from_message(my_type.clone()).unwrap();

        assert_eq!(unbound, Unbound::new(my_type.clone(), bindings));

        let bind = unbound.bind().unwrap();
        assert_eq!(bind, my_type);
    }

    #[test]
    fn test_named_struct() {
        let port_id2 = id!(world[0].comm[0][2]);
        let port_id4 = id!(world[1].worker[0][4]);
        let port2 = PortRef::attest(port_id2.clone());
        let port4 = PortRef::attest(port_id4.clone());
        let my_struct = MyNamedStruct {
            field0: 11,
            field1: MyReply("hello".to_string()),
            field2: port2.clone(),
            field3: true,
            field4: port4.clone(),
        };
        let mut bindings = Bindings::default();
        port2.unbind(&mut bindings).unwrap();
        port4.unbind(&mut bindings).unwrap();
        verify(my_struct, bindings);
    }

    #[test]
    fn test_unnamed_struct() {
        let port_id2 = id!(world[0].comm[0][2]);
        let port_id4 = id!(world[1].worker[0][4]);
        let port2 = PortRef::attest(port_id2.clone());
        let port4 = PortRef::attest(port_id4.clone());
        let my_struct = MyUnamedStruct(
            11,
            MyReply("hello".to_string()),
            port2.clone(),
            true,
            port4.clone(),
        );
        let mut bindings = Bindings::default();
        port2.unbind(&mut bindings).unwrap();
        port4.unbind(&mut bindings).unwrap();
        verify(my_struct, bindings);
    }

    #[test]
    fn test_named_enum() {
        let port_id2 = id!(world[0].comm[0][2]);
        let port_id4 = id!(world[1].worker[0][4]);
        let port2 = PortRef::attest(port_id2.clone());
        let port4 = PortRef::attest(port_id4.clone());
        let my_enum = MyEnum::Struct {
            field0: 11,
            field1: MyReply("hello".to_string()),
            field2: port2.clone(),
            field3: true,
            field4: port4.clone(),
        };
        let mut bindings = Bindings::default();
        port2.unbind(&mut bindings).unwrap();
        port4.unbind(&mut bindings).unwrap();
        verify(my_enum, bindings);
    }

    #[test]
    fn test_unnamed_enum() {
        let port_id2 = id!(world[0].comm[0][2]);
        let port_id4 = id!(world[1].worker[0][4]);
        let port2 = PortRef::attest(port_id2.clone());
        let port4 = PortRef::attest(port_id4.clone());
        let my_enum = MyEnum::Tuple(
            11,
            MyReply("hello".to_string()),
            port2.clone(),
            true,
            port4.clone(),
        );
        let mut bindings = Bindings::default();
        port2.unbind(&mut bindings).unwrap();
        port4.unbind(&mut bindings).unwrap();
        verify(my_enum, bindings);
    }

    #[test]
    fn test_unit_enum() {
        let my_enum = MyEnum::Unit;
        let bindings = Bindings::default();
        verify(my_enum, bindings);
    }

    #[test]
    fn test_my_generic_struct() {
        let port_id2 = id!(world[0].comm[0][2]);
        let port_id4 = id!(world[1].worker[0][4]);
        let port2: PortRef<()> = PortRef::attest(port_id2.clone());
        let port4: PortRef<()> = PortRef::attest(port_id4.clone());
        let my_struct = MyGenericStruct(port2.clone(), &11, port4.clone());
        let mut bindings = Bindings::default();
        port2.unbind(&mut bindings).unwrap();
        verify(my_struct, bindings);
    }

    #[test]
    fn test_my_generic_enum() {
        let port_id2 = id!(world[0].comm[0][2]);
        let port_id4 = id!(world[1].worker[0][4]);
        let port2: PortRef<()> = PortRef::attest(port_id2.clone());
        let port4: PortRef<()> = PortRef::attest(port_id4.clone());
        let my_enum = MyGenericEnum::Tuple(port2.clone(), &11, port4.clone());
        let mut bindings = Bindings::default();
        port2.unbind(&mut bindings).unwrap();
        verify(my_enum, bindings);
    }
}
