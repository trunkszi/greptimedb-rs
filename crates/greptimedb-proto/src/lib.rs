//! Generated GreptimeDB and Arrow Flight protocol bindings.
//!
//! `prost-build` emits the message types and `grpc-build` renders the
//! synchronous service traits. Module nesting follows the protobuf package
//! hierarchy so that the `super::` paths in the generated files resolve.

// Bindings are machine-generated; hand-written style lints do not apply.
#[allow(clippy::all, clippy::pedantic, warnings)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/greptime.v1.rs"));
    include!(concat!(env!("OUT_DIR"), "/greptime.v1.database_grpc.rs"));
    include!(concat!(env!("OUT_DIR"), "/greptime.v1.health_grpc.rs"));
    include!(concat!(env!("OUT_DIR"), "/greptime.v1.prom_grpc.rs"));

    pub mod meta {
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.meta.rs"));
        include!(concat!(
            env!("OUT_DIR"),
            "/greptime.v1.meta.cluster_grpc.rs"
        ));
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.meta.config_grpc.rs"));
        include!(concat!(
            env!("OUT_DIR"),
            "/greptime.v1.meta.heartbeat_grpc.rs"
        ));
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.meta.lock_grpc.rs"));
        include!(concat!(
            env!("OUT_DIR"),
            "/greptime.v1.meta.procedure_grpc.rs"
        ));
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.meta.store_grpc.rs"));
    }

    pub mod region {
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.region.rs"));
        include!(concat!(
            env!("OUT_DIR"),
            "/greptime.v1.region.server_grpc.rs"
        ));
    }

    pub mod flow {
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.flow.rs"));
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.flow.server_grpc.rs"));
    }

    pub mod frontend {
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.frontend.rs"));
        include!(concat!(
            env!("OUT_DIR"),
            "/greptime.v1.frontend.server_grpc.rs"
        ));
    }

    pub mod index {
        include!(concat!(env!("OUT_DIR"), "/greptime.v1.index.rs"));
    }

    impl From<value::ValueData> for Value {
        fn from(value: value::ValueData) -> Self {
            Self {
                value_data: Some(value),
            }
        }
    }
}

#[allow(clippy::all, clippy::pedantic, warnings)]
pub mod flight {
    include!(concat!(env!("OUT_DIR"), "/arrow.flight.protocol.rs"));
    include!(concat!(env!("OUT_DIR"), "/Flight_grpc.rs"));
}

#[allow(clippy::all, clippy::pedantic, warnings)]
pub mod substrait_extension {
    include!(concat!(env!("OUT_DIR"), "/substrait_extension.rs"));
}

#[allow(clippy::all, clippy::pedantic, warnings)]
pub mod prometheus {
    pub mod remote {
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }
}

#[allow(clippy::all, clippy::pedantic, warnings)]
pub mod io {
    pub mod prometheus {
        pub mod write {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/io.prometheus.write.v2.rs"));
            }
        }
    }
}
