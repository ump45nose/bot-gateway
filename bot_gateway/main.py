import argparse
import fcntl
import logging
from pathlib import Path
import signal
import threading

from .config import load
from .engine import Gateway
from .server import server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    cfg = load(args.config)
    root = Path(cfg['state_dir'])
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (root / 'gateway.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        gateway = Gateway(cfg)
        listener = server(gateway)
        def stop(*_):
            gateway.stop.set()
            with gateway.changed:
                gateway.changed.notify_all()
            threading.Thread(target=listener.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        gateway.start()
        try:
            listener.serve_forever(poll_interval=.5)
        finally:
            gateway.stop.set()
            listener.server_close()
            for thread in gateway.threads:
                thread.join(timeout=35)


if __name__ == '__main__':
    main()
