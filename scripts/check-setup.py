#!/usr/bin/env python3
"""Check setup safety and Make ordering without starting Docker or using real data."""
from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile

source = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="zchat-setup-") as directory:
    root = Path(directory)
    (root / "scripts").mkdir()
    for name in ["Makefile", ".env.example", "scripts/init-env.sh"]:
        shutil.copyfile(source / name, root / name)
    tools = root / "tools"
    tools.mkdir()

    def executable(name, text):
        path = tools / name
        path.write_text("#!/usr/bin/env bash\nset -eu\n" + text)
        path.chmod(0o755)

    # A Docker-only setup must not invoke either host Go or the column utility.
    for name in ["go", "column"]:
        executable(name, 'touch "$CHECK_ROOT/forbidden-tool"\nexit 91\n')
    executable("docker", '''
printf '%s\n' "$*" >> "$CHECK_ROOT/docker-args"
case "${2:-}" in
  config) echo config >> "$CHECK_ROOT/events" ;;
  up)
    echo up-start >> "$CHECK_ROOT/events"
    sleep 0.1
    echo up-end >> "$CHECK_ROOT/events" ;;
  logs) echo logs >> "$CHECK_ROOT/events"; exit "${CHECK_LOG_EXIT:-0}" ;;
esac
''')
    executable("goose", 'printf "%s\\n" "$*" > "$CHECK_ROOT/goose-args"\n')
    env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"], CHECK_ROOT=str(root))
    env.pop("MAKEFLAGS", None)

    def make(*args, success=True):
        result = subprocess.run(["make", "--no-print-directory", *args], cwd=root,
                                env=env, capture_output=True, text=True)
        assert (result.returncode == 0) == success, "Unexpected Make result: " + " ".join(args)
        return result

    assert "help" in make().stdout and "goose_reset" in make("help").stdout
    for target in ["up", "build", "db", "redis-up", "redis-stop", "goose_status", "goose_down", "goose_reset"]:
        make(target, success=False)
    assert not (root / "docker-args").exists() and not (root / "goose-args").exists(), "Missing config reached a real operation"
    print("PASS: help needs no config/Go/column; missing config blocks runtime and DB maintenance")

    result = make("init")
    path = root / ".env"
    original = path.read_bytes()
    values = dict(line.split("=", 1) for line in original.decode().splitlines()
                  if line and not line.startswith("#") and "=" in line)
    secrets = [values["DB_PASS"], values["JWT_SECRET"]]
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in secrets), "Invalid generated secret"
    assert secrets[0] != secrets[1], "Secrets must be independent"
    assert path.stat().st_mode & 0o777 == 0o600, "Config permissions must be owner-only"
    assert not any(value in result.stdout + result.stderr for value in secrets), "Secret leaked to output"
    make("init")
    assert path.read_bytes() == original, "Existing config changed"
    path.unlink()
    path.symlink_to("user-config")
    make("init")
    assert path.is_symlink() and not (root / "user-config").exists(), "Existing symlink changed"
    path.unlink()
    print("PASS: init creates private independent secrets; existing config/symlink preserved")

    processes = [subprocess.Popen(["bash", "scripts/init-env.sh"], cwd=root, env=env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for _ in range(4)]
    for process in processes:
        process.communicate(timeout=20)
        assert process.returncode == 0, "Concurrent config initialization failed"
    assert path.is_file() and not list(root.glob(".env.init.*")), "Concurrent init left incomplete files"
    print("PASS: concurrent initialization leaves one complete config and no temporary files")

    make("-j4", "run")
    assert (root / "events").read_text().splitlines() == ["config", "up-start", "up-end", "logs"], "Start/log ordering failed"
    assert "compose up -d --build --wait" in (root / "docker-args").read_text(), "Startup must build and wait"
    make("stop")
    make("down")
    args = (root / "docker-args").read_text().splitlines()
    assert args[-2:] == ["compose --profile redis stop", "compose --profile redis down"], "Stop/down must include optional Redis and preserve volumes"
    env["CHECK_LOG_EXIT"] = "41"
    make("logs", success=False)
    env.pop("CHECK_LOG_EXIT")
    print("PASS: parallel make run waits for startup; stop/down preserve volumes")

    result = make("goose_status", "DB_HOST_PORT=6543")
    assert "host=127.0.0.1 port=6543" in (root / "goose-args").read_text(), "Host DB port mapping failed"
    current = dict(line.split("=", 1) for line in path.read_text().splitlines()
                   if line and not line.startswith("#") and "=" in line)
    assert current["DB_PASS"] not in result.stdout + result.stderr, "DB password echoed"
    assert not (root / "forbidden-tool").exists(), "Docker workflow unexpectedly invoked Go/column"
    print("PASS: host DB configuration works and maintenance does not echo credentials")
