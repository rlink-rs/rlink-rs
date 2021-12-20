use std::sync::Arc;

pub trait ScalarFunction: Send + Sync {
    fn open(&mut self);
    fn return_type(&self);
    fn eval(&mut self);
    fn close(&mut self);
}

/// Logical representation of a UDF.
#[derive(Clone)]
pub struct ScalarUDF {
    /// name
    pub name: String,
    // /// signature
    // pub signature: Signature,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    pub fun: Arc<dyn ScalarFunction>,
}

pub trait AggregateFunction: Send + Sync {
    fn open(&mut self);
    fn return_type(&self);
    fn eval(&mut self);
    fn close(&mut self);
}

/// Logical representation of a user-defined aggregate function (UDAF)
/// A UDAF is different from a UDF in that it is stateful across batches.
#[derive(Clone)]
pub struct AggregateUDF {
    /// name
    pub name: String,
    // /// signature
    // pub signature: Signature,
    /// actual implementation
    pub accumulator: Arc<dyn AggregateFunction>,
}
