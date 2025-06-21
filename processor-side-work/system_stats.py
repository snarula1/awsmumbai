import psutil
import logging

def get_system_stats():
    """
    Get system statistics in a safe way that won't crash if access is denied
    Returns a dictionary with memory and CPU usage information
    """
    stats = {}
    
    try:
        # Get memory usage
        memory = psutil.virtual_memory()
        stats['memory_percent'] = memory.percent
        stats['memory_used_mb'] = memory.used / (1024 * 1024)
        stats['memory_available_mb'] = memory.available / (1024 * 1024)
        
        # Get CPU usage
        stats['cpu_percent'] = psutil.cpu_percent(interval=0.1)
        
        # Get disk usage for the current directory
        disk = psutil.disk_usage('/')
        stats['disk_percent'] = disk.percent
        stats['disk_free_gb'] = disk.free / (1024 * 1024 * 1024)
        
        return stats
    except Exception as e:
        # If any error occurs, return a minimal set of stats
        logging.warning(f"Error getting system stats: {str(e)}")
        return {'error': str(e)}

def format_system_stats():
    """
    Get and format system statistics as a string
    Returns a formatted string with system stats
    """
    try:
        stats = get_system_stats()
        
        if 'error' in stats:
            return f"System stats unavailable: {stats['error']}"
        
        return (f"System stats: "
                f"CPU: {stats.get('cpu_percent', 'N/A')}%, "
                f"Memory: {stats.get('memory_percent', 'N/A')}% "
                f"({stats.get('memory_used_mb', 'N/A'):.0f}MB used), "
                f"Disk: {stats.get('disk_percent', 'N/A')}% full "
                f"({stats.get('disk_free_gb', 'N/A'):.1f}GB free)")
    except Exception as e:
        return f"System stats unavailable: {str(e)}"