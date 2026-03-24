//! DataFusion integration — [`QvdTableProvider`] for SQL queries on QVD files.
//!
//! Requires feature `datafusion_support`.
//!
//! # Example
//!
//! ```no_run
//! use datafusion::prelude::*;
//! use qvd::datafusion::QvdTableProvider;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//! let provider = QvdTableProvider::try_new("sales.qvd")?;
//! ctx.register_table("sales", Arc::new(provider))?;
//!
//! let df = ctx.sql("SELECT Region, SUM(Amount) FROM sales GROUP BY Region").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::error::QvdResult;
use crate::parquet::qvd_to_record_batch;
use crate::reader;

/// A DataFusion [`TableProvider`] backed by a QVD file.
///
/// The file is read into an Arrow RecordBatch on construction and cached.
pub struct QvdTableProvider {
    path: PathBuf,
    schema: SchemaRef,
    batch: RecordBatch,
}

impl fmt::Debug for QvdTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QvdTableProvider")
            .field("path", &self.path)
            .field("schema", &self.schema)
            .field("rows", &self.batch.num_rows())
            .finish()
    }
}

impl QvdTableProvider {
    /// Create a new provider by reading and converting a QVD file.
    pub fn try_new(path: impl AsRef<Path>) -> QvdResult<Self> {
        let path = path.as_ref().to_path_buf();
        let path_str = path.to_str()
            .ok_or_else(|| crate::error::QvdError::Format("Path contains invalid UTF-8".into()))?;
        let table = reader::read_qvd_file(path_str)?;
        let batch = qvd_to_record_batch(&table)?;
        let schema = batch.schema();
        Ok(Self { path, schema, batch })
    }

    /// Create a provider from an already-loaded QvdTable.
    pub fn from_table(table: &reader::QvdTable, path: impl AsRef<Path>) -> QvdResult<Self> {
        let path = path.as_ref().to_path_buf();
        let batch = qvd_to_record_batch(table)?;
        let schema = batch.schema();
        Ok(Self { path, schema, batch })
    }

    /// Return a reference to the cached RecordBatch.
    pub fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }
}

#[async_trait]
impl TableProvider for QvdTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let source = MemorySourceConfig::try_new(
            &[vec![self.batch.clone()]],
            self.schema.clone(),
            projection.cloned(),
        )?;

        Ok(DataSourceExec::from_data_source(source))
    }
}

/// Register a QVD file as a table in a DataFusion
/// [`SessionContext`](datafusion::prelude::SessionContext).
///
/// ```no_run
/// use datafusion::prelude::*;
/// use qvd::datafusion::register_qvd;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// register_qvd(&ctx, "sales", "sales.qvd")?;
///
/// let df = ctx.sql("SELECT * FROM sales LIMIT 10").await?;
/// df.show().await?;
/// # Ok(())
/// # }
/// ```
pub fn register_qvd(
    ctx: &datafusion::prelude::SessionContext,
    table_name: &str,
    qvd_path: impl AsRef<Path>,
) -> QvdResult<()> {
    let provider = QvdTableProvider::try_new(qvd_path)?;
    ctx.register_table(table_name, Arc::new(provider))
        .map_err(crate::error::QvdError::DataFusion)?;
    Ok(())
}
