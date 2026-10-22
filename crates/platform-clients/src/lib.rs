//! Clients for the platform's services, shared by the collector, cicd-sim,
//! the agents and the MCP server.

pub mod chatops;
pub mod flink;
pub mod kafka;
pub mod ops_db;
pub mod sql_gateway;
pub mod stack_trace;
pub mod tickets;
