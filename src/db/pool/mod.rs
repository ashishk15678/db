// Connection Pool Module
// Provides connection limiting and management using semaphores

use std::sync::Arc;
use tokio::sync::Semaphore;
use std::time::Duration;

use crate::config::PoolConfig;

/// Connection pool using semaphore for limiting concurrent connections
pub struct ConnectionPool {
    /// Semaphore to limit concurrent connections
    semaphore: Arc<Semaphore>,
    /// Pool configuration
    config: PoolConfig,
}

/// Guard that releases the semaphore permit when dropped
pub struct ConnectionGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ConnectionPool {
    /// Create a new connection pool with the given configuration
    pub fn new(config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections as usize));
        Self { semaphore, config }
    }

    /// Create a connection pool with default configuration
    pub fn default() -> Self {
        Self::new(PoolConfig::default())
    }

    /// Acquire a connection from the pool
    /// Returns a ConnectionGuard that releases the connection when dropped
    pub async fn acquire(&self) -> Result<ConnectionGuard, String> {
        let timeout = Duration::from_millis(self.config.connection_timeout_ms);
        
        match tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(ConnectionGuard { _permit: permit }),
            Ok(Err(_)) => Err("Connection pool closed".to_string()),
            Err(_) => Err(format!(
                "Connection pool timeout after {}ms", 
                self.config.connection_timeout_ms
            )),
        }
    }

    /// Get the number of available connections
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the maximum number of connections
    pub fn max_connections(&self) -> u32 {
        self.config.max_connections
    }

    /// Get the current pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }
}

// Global connection pool instance
lazy_static::lazy_static! {
    pub static ref POOL: ConnectionPool = {
        match crate::config::get_config() {
            Ok(config) => ConnectionPool::new(config.pool),
            Err(_) => ConnectionPool::default(),
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let config = PoolConfig::default();
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.max_connections(), 100);
    }

    #[test]
    fn test_pool_available() {
        let config = PoolConfig {
            min_connections: 5,
            max_connections: 10,
            connection_timeout_ms: 1000,
            idle_timeout_ms: 5000,
        };
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.available(), 10);
    }

    #[tokio::test]
    async fn test_pool_acquire() {
        let config = PoolConfig {
            min_connections: 1,
            max_connections: 5,
            connection_timeout_ms: 1000,
            idle_timeout_ms: 5000,
        };
        let pool = ConnectionPool::new(config);
        
        let guard = pool.acquire().await;
        assert!(guard.is_ok());
        assert_eq!(pool.available(), 4);
        
        // When guard is dropped, connection is released
        drop(guard);
        assert_eq!(pool.available(), 5);
    }

    #[tokio::test]
    async fn test_pool_multiple_acquire() {
        let config = PoolConfig {
            min_connections: 1,
            max_connections: 3,
            connection_timeout_ms: 1000,
            idle_timeout_ms: 5000,
        };
        let pool = ConnectionPool::new(config);
        
        let g1 = pool.acquire().await.unwrap();
        let g2 = pool.acquire().await.unwrap();
        let g3 = pool.acquire().await.unwrap();
        
        assert_eq!(pool.available(), 0);
        
        drop(g1);
        assert_eq!(pool.available(), 1);
        
        drop(g2);
        drop(g3);
        assert_eq!(pool.available(), 3);
    }
}
