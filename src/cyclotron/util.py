try:
    import psutil
except ImportError:
    psutil = None


_PROCESS = None


def _get_psutil_rss():
    global _PROCESS
    if _PROCESS is None:
        _PROCESS = psutil.Process()
    return _PROCESS.memory_info().rss


def _get_proc_rss():
    result = {'peak': 0, 'rss': 0}
    with open('/proc/self/status') as status:
        for line in status:
            parts = line.split()
            key = parts[0][2:-1].lower()
            if key in result:
                result[key] = int(parts[1])
    return result['rss']


def get_memory_usage():
    if psutil is not None:
        return _get_psutil_rss()
    return _get_proc_rss()
