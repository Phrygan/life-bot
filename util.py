from datetime import datetime

import os

def log_event(message, module: str=None):
    """
    Log Event

    Args:
        message (any): Message to log.
        module (str, optional): Name of module where message came from. Defaults to None.
    """
    print(f"[{str(module) + ' | ' if module is not None else ''}{datetime.now().strftime('%H:%M:%S')}] {message}")