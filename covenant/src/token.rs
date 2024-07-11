use std::fmt::{Debug, Formatter};

use async_channel::{bounded as channel, Receiver, Sender};

pub(crate) struct TokenDistributor {
    tx: Sender<usize>,
    rx: Receiver<usize>,
}

impl TokenDistributor {
    pub(crate) async fn new(limit: usize) -> Self {
        let (tx, rx) = channel(limit);
        for i in 0..limit {
            let _ = tx.send(i).await;
        }
        Self { rx, tx }
    }

    pub(crate) async fn acquire(&self) -> Token {
        let id = self.rx.recv().await.unwrap(); // If rx still exists then tx does too
        let tx = self.tx.clone();
        Token { id, tx }
    }
}

pub(crate) struct Token {
    id: usize,
    tx: Sender<usize>,
}

impl Token {
    pub fn id(&self) -> usize {
        self.id
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        let _ = self.tx.send_blocking(self.id);
    }
}

impl Debug for TokenDistributor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenDistributor")
            .field("limit", &self.rx.capacity())
            .field("available_ids", &self.rx.len())
            .finish_non_exhaustive()
    }
}

impl Debug for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token").field("id", &self.id).finish_non_exhaustive()
    }
}
