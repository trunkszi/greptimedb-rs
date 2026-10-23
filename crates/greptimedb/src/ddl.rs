//! Synchronous schema, view, and flow operations.

use std::collections::HashMap;

use greptimedb_proto::v1::{
    AlterDatabaseExpr, AlterTableExpr, CommentOnExpr, CreateDatabaseExpr, CreateFlowExpr,
    CreateTableExpr, CreateViewExpr, DdlRequest, DropFlowExpr, DropTableExpr, DropViewExpr,
    TruncateTableExpr, ddl_request, greptime_request,
};

use crate::{Client, Result};

impl Client {
    /// Creates a schema in the client's catalog with the supplied database options.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn create_database(
        &mut self,
        schema_name: impl Into<String>,
        create_if_not_exists: bool,
        options: HashMap<String, String>,
    ) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::CreateDatabase(CreateDatabaseExpr {
            catalog_name: self.options().catalog.clone(),
            schema_name: schema_name.into(),
            create_if_not_exists,
            options,
        }))
    }

    /// Alters a database using the expression's catalog, schema, and operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn alter_database(&mut self, expression: AlterDatabaseExpr) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::AlterDatabase(expression))
    }

    /// Creates a table using the expression's complete schema and storage options.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn create_table(&mut self, expression: CreateTableExpr) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::CreateTable(expression))
    }

    /// Alters a table using the expression's catalog, schema, and operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn alter_table(&mut self, expression: AlterTableExpr) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::AlterTable(expression))
    }

    /// Drops a table in the client's catalog and schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn drop_table(
        &mut self,
        table_name: impl Into<String>,
        drop_if_exists: bool,
    ) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::DropTable(DropTableExpr {
            catalog_name: self.options().catalog.clone(),
            schema_name: self.options().schema.clone(),
            table_name: table_name.into(),
            table_id: None,
            drop_if_exists,
        }))
    }

    /// Truncates the entire table in the client's catalog and schema.
    ///
    /// Use [`Self::execute_ddl`] with a `TruncateTableExpr` to select time ranges.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn truncate_table(&mut self, table_name: impl Into<String>) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::TruncateTable(TruncateTableExpr {
            catalog_name: self.options().catalog.clone(),
            schema_name: self.options().schema.clone(),
            table_name: table_name.into(),
            table_id: None,
            time_ranges: None,
        }))
    }

    /// Creates a view using the expression's plan, dependencies, and definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn create_view(&mut self, expression: CreateViewExpr) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::CreateView(expression))
    }

    /// Drops a view in the client's catalog and schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn drop_view(&mut self, view_name: impl Into<String>, drop_if_exists: bool) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::DropView(DropViewExpr {
            catalog_name: self.options().catalog.clone(),
            schema_name: self.options().schema.clone(),
            view_name: view_name.into(),
            view_id: None,
            drop_if_exists,
        }))
    }

    /// Creates a flow using the expression's sources, sink, and SQL.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn create_flow(&mut self, expression: CreateFlowExpr) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::CreateFlow(expression))
    }

    /// Drops a flow in the client's catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn drop_flow(&mut self, flow_name: impl Into<String>, drop_if_exists: bool) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::DropFlow(DropFlowExpr {
            catalog_name: self.options().catalog.clone(),
            flow_name: flow_name.into(),
            flow_id: None,
            drop_if_exists,
        }))
    }

    /// Sets a table, column, or flow comment using the supplied expression.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or GreptimeDB rejects the expression.
    pub fn comment_on(&mut self, expression: CommentOnExpr) -> Result<u32> {
        self.execute_ddl(ddl_request::Expr::CommentOn(expression))
    }

    /// Executes a DDL expression and returns GreptimeDB's affected row count.
    ///
    /// Expression fields are sent unchanged. The client supplies the request
    /// header, authentication, and metadata according to its configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failures, gRPC errors, GreptimeDB status
    /// errors, or a response without an affected row count.
    pub fn execute_ddl(&mut self, expression: ddl_request::Expr) -> Result<u32> {
        self.handle_request(
            greptime_request::Request::Ddl(DdlRequest {
                expr: Some(expression),
            }),
            None,
        )
    }
}
