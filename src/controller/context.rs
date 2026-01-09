use kube::Client;

/// Shared context for the controller
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
}

impl Context {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}
