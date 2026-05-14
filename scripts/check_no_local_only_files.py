import subprocess
import sys
from pathlib import PurePosixPath


FORBIDDEN_PREFIXES = (
    ".local-dev/",
)

FORBIDDEN_EXACT = set()


def _is_forbidden_tracked_path(path_text: str) -> bool:
    normalized = str(PurePosixPath(path_text.replace("\\", "/")))
    if normalized in FORBIDDEN_EXACT:
        return True
    if any(normalized.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
        return True
    if normalized.startswith("frontend/data/") and normalized != "frontend/data/.gitkeep":
        return True
    return False


def main() -> int:
    result = subprocess.run(
        ["git", "ls-files"],
        check=True,
        capture_output=True,
        text=True,
    )
    tracked_files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    forbidden = [path for path in tracked_files if _is_forbidden_tracked_path(path)]

    if not forbidden:
        print("OK: nenhum arquivo local-only/mockado esta versionado.")
        return 0

    print("ERRO: arquivos locais/mockados nao podem ser versionados:")
    for path in forbidden:
        print(f" - {path}")
    print("")
    print("Mantenha mock/dados locais apenas em '.local-dev/' ou fora do Git.")
    print("Arquivos dentro de 'frontend/data/' tambem nao devem ser versionados, exceto 'frontend/data/.gitkeep'.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
