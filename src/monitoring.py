"""
Monitoring and metrics collection for NL2SQL application
"""
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from config import logger, redis_client
from collections import defaultdict, deque

class QueryMetrics:
    """Track query performance and usage metrics"""
    
    def __init__(self):
        self.query_times = deque(maxlen=1000)  # Keep last 1000 query times
        self.error_counts = defaultdict(int)
        self.query_patterns = defaultdict(int)
        
    def record_query_time(self, query_time: float, query_type: str = "general"):
        """Record query execution time"""
        self.query_times.append({
            'time': query_time,
            'type': query_type,
            'timestamp': datetime.now()
        })
        
        # Log slow queries
        if query_time > 5.0:  # 5 seconds threshold
            logger.warning(f"Slow query detected: {query_time:.2f}s for {query_type}")
    
    def record_error(self, error_type: str):
        """Record error occurrence"""
        self.error_counts[error_type] += 1
        logger.info(f"Error recorded: {error_type} (total: {self.error_counts[error_type]})")
    
    def get_performance_stats(self) -> Dict:
        """Get performance statistics"""
        if not self.query_times:
            return {"status": "no_data"}
        
        times = [q['time'] for q in self.query_times]
        return {
            "avg_query_time": sum(times) / len(times),
            "max_query_time": max(times),
            "min_query_time": min(times),
            "total_queries": len(times),
            "slow_queries": len([t for t in times if t > 5.0]),
            "error_summary": dict(self.error_counts)
        }

# Global metrics instance
metrics = QueryMetrics()

def track_query_performance(func):
    """Decorator to track query performance"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            metrics.record_query_time(execution_time, func.__name__)
            return result
        except Exception as e:
            metrics.record_error(type(e).__name__)
            raise
    return wrapper

def get_system_health() -> Dict:
    """Get overall system health status"""
    health_status = {
        "timestamp": datetime.now().isoformat(),
        "database": "unknown",
        "redis": "unknown",
        "performance": metrics.get_performance_stats()
    }
    
    # Test database connection
    try:
        from db_utils import safe_db_run
        result = safe_db_run(f"SELECT 1 as health_check")
        health_status["database"] = "healthy" if result and not str(result).startswith("Error:") else "error"
    except Exception:
        health_status["database"] = "error"
    
    # Test Redis connection
    if redis_client:
        try:
            redis_client.ping()
            health_status["redis"] = "healthy"
        except Exception:
            health_status["redis"] = "error"
    else:
        health_status["redis"] = "disabled"
    
    return health_status