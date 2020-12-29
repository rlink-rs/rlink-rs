//! Exports metrics over HTTP.
//!
//! This exporter can utilize observers that are able to be converted to a textual representation
//! via [`Drain<String>`].  It will respond to any requests, regardless of the method or path.
//!
//! Awaiting on `async_run` will drive an HTTP server listening on the configured address.
// #![deny(missing_docs)]

use std::{net::SocketAddr, sync::Arc};

use hyper::server::conn::AddrIncoming;
use hyper::{
    service::{make_service_fn, service_fn},
    {Body, Error, Response, Server},
};
use metrics_core::{Builder, Drain, Observe, Observer};

use crate::metrics::worker_proxy;

/// Exports metrics over HTTP.
pub struct HttpExporter<C, B> {
    controller: C,
    builder: B,
    // address: SocketAddr,
}

impl<C, B> HttpExporter<C, B>
where
    C: Observe + Send + Sync + 'static,
    B: Builder + Send + Sync + 'static,
    B::Output: Drain<String> + Observer,
{
    /// Creates a new [`HttpExporter`] that listens on the given `address`.
    ///
    /// Observers expose their output by being converted into strings.
    pub fn new(controller: C, builder: B) -> Self {
        HttpExporter {
            controller,
            builder,
            // address,
        }
    }

    // /// Starts an HTTP server on the `address` the exporter was originally configured with,
    // /// responding to any request with the output of the configured observer.
    // pub async fn async_run(self) -> hyper::error::Result<()> {
    //     let builder = Arc::new(self.builder);
    //     let controller = Arc::new(self.controller);
    //
    //     let make_svc = make_service_fn(move |_| {
    //         let builder = builder.clone();
    //         let controller = controller.clone();
    //
    //         async move {
    //             Ok::<_, Error>(service_fn(move |_| {
    //                 let builder = builder.clone();
    //                 let controller = controller.clone();
    //
    //                 async move {
    //                     let mut observer = builder.build();
    //                     controller.observe(&mut observer);
    //                     let output = observer.drain();
    //                     Ok::<_, Error>(Response::new(Body::from(output)))
    //                 }
    //             }))
    //         }
    //     });
    //
    //     Server::bind(&self.address).serve(make_svc).await
    // }

    pub fn try_bind(
        &self,
        address: &SocketAddr,
    ) -> hyper::Result<hyper::server::Builder<AddrIncoming>> {
        Server::try_bind(address)
    }

    pub async fn async_run1(
        self,
        addr_incoming: hyper::server::Builder<AddrIncoming>,
        with_proxy: bool,
    ) -> hyper::error::Result<()> {
        let builder = Arc::new(self.builder);
        let controller = Arc::new(self.controller);

        let make_svc = make_service_fn(move |_| {
            let builder = builder.clone();
            let controller = controller.clone();

            async move {
                Ok::<_, Error>(service_fn(move |_| {
                    let builder = builder.clone();
                    let controller = controller.clone();

                    async move {
                        crate::metrics::global_metrics::compute();

                        let proxy_metrics = if with_proxy {
                            let n = worker_proxy::collect_worker_metrics().await;
                            n
                        } else {
                            "".to_string()
                        };

                        let mut observer = builder.build();
                        controller.observe(&mut observer);
                        let mut output = observer.drain();

                        if with_proxy {
                            output.push_str("\n");
                            output.push_str(proxy_metrics.as_str());
                        }

                        Ok::<_, Error>(Response::new(Body::from(output)))
                    }
                }))
            }
        });

        addr_incoming.serve(make_svc).await
    }
}
