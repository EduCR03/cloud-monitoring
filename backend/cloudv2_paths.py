import os


DEFAULT_WEB_DIR = "frontend"
LEGACY_WEB_DIR = "web"
OLDER_LEGACY_WEB_DIR = "dashboards"
LEGACY_WEB_DIRS = (LEGACY_WEB_DIR, OLDER_LEGACY_WEB_DIR)
DATA_SUBDIR = "data"
WEB_DIR_ENV = "CLOUDV2_WEB_DIR"
DATA_DIR_ENV = "CLOUDV2_DATA_DIR"


def _normalize_path(path):
    return os.path.normpath(str(path or "").strip())


def resolve_web_dir():
    env_value = os.environ.get(WEB_DIR_ENV)
    if env_value:
        return _normalize_path(env_value)

    if os.path.isdir(DEFAULT_WEB_DIR):
        return DEFAULT_WEB_DIR

    for legacy_dir in LEGACY_WEB_DIRS:
        if os.path.isdir(legacy_dir):
            return legacy_dir

    return DEFAULT_WEB_DIR


def resolve_data_dir(web_dir=None):
    env_value = os.environ.get(DATA_DIR_ENV)
    if env_value:
        return _normalize_path(env_value)
    base_dir = _normalize_path(web_dir or resolve_web_dir())
    return os.path.join(base_dir, DATA_SUBDIR)
