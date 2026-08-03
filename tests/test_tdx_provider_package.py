from __future__ import annotations

import ast
import json
import importlib
import os
import pytest
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

from axdata_core.cli import main
from axdata_core.plugin_config import disable_provider, enable_provider
from axdata_core.plugins import ProviderManifest
from axdata_core.provider_catalog import build_builtin_provider_registry
from axdata_core.source_errors import SourceUnavailableError
from axdata_core.source_request import request_interface
from axdata_core.tdx_plugin_required import TDX_PLUGIN_REQUIRED_MESSAGE

from tests.test_tdx_source_request_adapter import (
    EchoTqlexClient,
    FakeTdxClient,
    _expected_default_tdx_stats_cache_root,
    _minimal_stat2_line,
    _minimal_stat_line,
    _stats_zip_bytes,
)
from tests.tdx_plugin_helpers import (
    TDX_BASE_COLLECTOR_DATASET_IDS,
    TDX_COLLECTOR_PLUGIN_ID,
    TDX_COLLECTOR_RUNNER_ENTRY,
    TDX_LEGACY_COLLECTOR_NAMES,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
TDX_PACKAGE_ROOT = REPO_ROOT / "packages" / "axdata-source-tdx"
TDX_PROVIDER_ID = "axdata.source.tdx_external"
BUILTIN_TDX_PROVIDER_ID = "axdata.source.tdx"
TDX_INTERFACE_NAME = "stock_codes_tdx"


def _core_pythonpath() -> str:
    return str(REPO_ROOT / "libs" / "axdata_core")


def _tdx_provider_pythonpath() -> str:
    return os.pathsep.join([str(TDX_PACKAGE_ROOT / "src"), _core_pythonpath()])


def _core_without_site_subprocess(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-S", "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _core_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_tdx_provider_package_root_import_is_lightweight(monkeypatch, tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)

    monkeypatch.syspath_prepend(str(install_root))
    _clear_tdx_provider_modules()

    package = importlib.import_module("axdata_source_tdx")

    assert "axdata_source_tdx.provider" not in sys.modules
    assert package.provider.provider_id == TDX_PROVIDER_ID
    assert "axdata_source_tdx.provider" in sys.modules
    assert "axdata_source_tdx.adapter" not in sys.modules


def test_core_tdx_fallback_runtime_packages_are_removed() -> None:
    removed_paths = [
        REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "_tdx_wire_fallback",
        REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "_tdx_server_config_fallback.py",
        REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "_tdx_f10_specs_fallback.py",
        REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "adapters" / "tdx" / "_request_adapter_fallback.py",
    ]

    assert all(not path.exists() for path in removed_paths)


def test_core_tdx_wire_requires_provider_when_missing() -> None:
    code = (
        "from axdata_core.source_errors import SourceUnavailableError\n"
        "from axdata_core.tdx_plugin_required import TDX_PLUGIN_REQUIRED_MESSAGE\n"
        "try:\n"
        "    from axdata_core._tdx_wire.client import TdxClient\n"
        "    print('unexpected=' + repr(TdxClient))\n"
        "except SourceUnavailableError as exc:\n"
        "    print('error=' + str(exc))\n"
        "    print('matches=' + str(str(exc) == TDX_PLUGIN_REQUIRED_MESSAGE))\n"
    )
    result = _core_without_site_subprocess(code)

    assert f"error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout
    assert "matches=True" in result.stdout


def test_core_tdx_catalog_requires_provider_when_missing() -> None:
    code = (
        "from axdata_core.source_errors import SourceUnavailableError\n"
        "try:\n"
        "    import axdata_core.sources.tdx.catalog\n"
        "except SourceUnavailableError as exc:\n"
        "    print('error=' + str(exc))\n"
    )
    result = _core_without_site_subprocess(code)

    assert f"error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout


def test_tdx_provider_catalog_import_does_not_load_builtin_projection() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.catalog\n"
        "print('catalog=' + str('axdata_source_tdx.catalog' in sys.modules))\n"
        "print('builtin=' + str('axdata_core.builtin_providers' in sys.modules))\n"
        "print('downloader_registry=' + str('axdata_core.downloader_registry' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "catalog=True" in result.stdout
    assert "builtin=False" in result.stdout
    assert "downloader_registry=False" in result.stdout
    assert "request=False" in result.stdout


def test_tdx_provider_import_does_not_load_builtin_projection() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.provider\n"
        "print('provider=' + str('axdata_source_tdx.provider' in sys.modules))\n"
        "print('builtin=' + str('axdata_core.builtin_providers' in sys.modules))\n"
        "print('downloader_registry=' + str('axdata_core.downloader_registry' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "provider=True" in result.stdout
    assert "builtin=False" in result.stdout
    assert "downloader_registry=False" in result.stdout
    assert "request=False" in result.stdout


def test_tdx_provider_catalog_projection_does_not_load_downloader_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx.provider import provider\n"
        "interfaces = provider.interfaces()\n"
        "profiles = provider.downloader_profiles()\n"
        "collectors = provider.collectors()\n"
        "print('interfaces=' + str(len(interfaces)))\n"
        "print('profiles=' + str(len(profiles)))\n"
        "print('collectors=' + str(len(collectors)))\n"
        "tracked = [\n"
        "    'axdata_core.sources.tdx.catalog',\n"
        "    'axdata_core.tdx_f10_catalog',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.downloaders',\n"
        "    'axdata_core.downloader_registry',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "]\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "interfaces=90" in result.stdout
    assert "profiles=10" in result.stdout
    assert "collectors=0" in result.stdout
    assert "axdata_core.sources.tdx.catalog" not in result.stdout
    assert "axdata_core.tdx_f10_catalog" not in result.stdout
    assert "axdata_core.tdx_f10_specs" not in result.stdout
    assert "axdata_core.downloaders" not in result.stdout
    assert "axdata_core.downloader_registry" not in result.stdout
    assert "axdata_core.source_request" not in result.stdout
    assert "axdata_core.adapters.tdx.request" not in result.stdout
    assert "axdata_core.adapters.tdx.downloader" not in result.stdout


def test_core_tdx_source_catalog_import_requires_tdx_provider() -> None:
    code = (
        "from axdata_core.source_errors import SourceUnavailableError\n"
        "try:\n"
        "    import axdata_core.sources.tdx.catalog\n"
        "except SourceUnavailableError as exc:\n"
        "    print('error=' + str(exc))\n"
    )
    result = _core_without_site_subprocess(code)

    assert f"error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout


def test_tdx_f10_catalog_projection_matches_runtime_specs() -> None:
    from axdata_core.tdx_f10_catalog import F10_CATALOG_SPECS
    from axdata_core.tdx_f10_specs import F10_INTERFACE_SPECS

    assert tuple(F10_CATALOG_SPECS) == tuple(F10_INTERFACE_SPECS)
    for interface_name, catalog_spec in F10_CATALOG_SPECS.items():
        runtime_spec = F10_INTERFACE_SPECS[interface_name]
        assert catalog_spec.name == runtime_spec.name
        assert catalog_spec.display_name_zh == runtime_spec.display_name_zh
        assert catalog_spec.category == runtime_spec.category
        assert catalog_spec.summary_zh == runtime_spec.summary_zh
        assert catalog_spec.evaluation == runtime_spec.evaluation
        assert [
            (parameter.name, parameter.dtype, parameter.required, parameter.description_zh, parameter.default)
            for parameter in catalog_spec.params
        ] == [
            (parameter.name, parameter.dtype, parameter.required, parameter.description_zh, parameter.default)
            for parameter in runtime_spec.params
        ]
        assert [
            (field.name, field.dtype, field.description_zh, field.example)
            for field in catalog_spec.fields
        ] == [
            (field.name, field.dtype, field.description_zh, field.example)
            for field in runtime_spec.fields
        ]


def test_tdx_provider_f10_declarations_import_without_core_runtime() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.tdx_f10_names',\n"
        "    'axdata_source_tdx.tdx_f10_models',\n"
        "    'axdata_source_tdx.tdx_f10_catalog',\n"
        "    'axdata_source_tdx.tdx_f10_specs',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "from axdata_source_tdx.tdx_f10_catalog import F10_CATALOG_SPECS\n"
        "from axdata_source_tdx.tdx_f10_specs import F10_INTERFACE_SPECS\n"
        "tracked = [\n"
        "    'axdata_core.tdx_f10_names',\n"
        "    'axdata_core.tdx_f10_models',\n"
        "    'axdata_core.tdx_f10_catalog',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.source_request',\n"
        "]\n"
        "print('catalog_count=' + str(len(F10_CATALOG_SPECS)))\n"
        "print('spec_count=' + str(len(F10_INTERFACE_SPECS)))\n"
        "print('catalog_model_module=' + next(iter(F10_CATALOG_SPECS.values())).__class__.__module__)\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "catalog_count=36" in result.stdout
    assert "spec_count=36" in result.stdout
    assert "catalog_model_module=axdata_source_tdx.tdx_f10_models" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_metadata_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.metadata as metadata\n"
        "print('provider_id=' + metadata.PROVIDER_ID)\n"
        "print('provider=' + str('axdata_source_tdx.provider' in sys.modules))\n"
        "print('catalog=' + str('axdata_source_tdx.catalog' in sys.modules))\n"
        "print('adapter=' + str('axdata_source_tdx.adapter' in sys.modules))\n"
        "print('plugins=' + str('axdata_core.plugins' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert f"provider_id={TDX_PROVIDER_ID}" in result.stdout
    assert "provider=False" in result.stdout
    assert "catalog=False" in result.stdout
    assert "adapter=False" in result.stdout
    assert "plugins=False" in result.stdout
    assert "request=False" in result.stdout


def test_core_tdx_package_root_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "from axdata_core.source_errors import SourceUnavailableError\n"
        "import axdata_core.adapters.tdx as tdx\n"
        "print('request_before=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tqlex_before=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_specs_before=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "try:\n"
        "    _ = tdx.instrument_id_to_tdx_code\n"
        "except SourceUnavailableError as exc:\n"
        "    print('codes_error=' + str(exc))\n"
        "_ = tdx.create_tdx_client\n"
        "print('client_factory_after=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "try:\n"
        "    _ = tdx.TdxRequestAdapter\n"
        "except SourceUnavailableError as exc:\n"
        "    print('request_error=' + str(exc))\n"
        "print('request_after=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tqlex_after=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_specs_after=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "request_before=False" in result.stdout
    assert "tqlex_before=False" in result.stdout
    assert "f10_specs_before=False" in result.stdout
    assert f"codes_error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout
    assert "client_factory_after=True" in result.stdout
    assert f"request_error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout
    assert "request_after=True" in result.stdout
    assert "tqlex_after=False" in result.stdout
    assert "f10_specs_after=False" in result.stdout


def test_tdx_client_factory_import_does_not_load_host_config() -> None:
    code = (
        "import sys\n"
        "from axdata_core.source_errors import SourceUnavailableError\n"
        "import axdata_core.adapters.tdx as tdx\n"
        "_ = tdx.create_tdx_client\n"
        "try:\n"
        "    tdx.create_tdx_client(hosts=['demo:7709'], pool_size=1)\n"
        "except SourceUnavailableError as exc:\n"
        "    print('error=' + str(exc))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "client_factory=True" in result.stdout
    assert f"error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout
    assert "host_config=False" in result.stdout
    assert "server_config=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_tdx_provider_client_factory_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.client_factory\n"
        "print('provider_client_factory=' + str('axdata_source_tdx.client_factory' in sys.modules))\n"
        "print('provider_host_config=' + str('axdata_source_tdx.host_config' in sys.modules))\n"
        "print('core_client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('core_host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "provider_client_factory=True" in result.stdout
    assert "provider_host_config=False" in result.stdout
    assert "core_client_factory=False" in result.stdout
    assert "core_host_config=False" in result.stdout
    assert "server_config=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_tdx_provider_wire_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.wire\n"
        "print('provider_wire=' + str('axdata_source_tdx.wire' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('tdx_wire_hosts=' + str('axdata_core._tdx_wire.hosts' in sys.modules))\n"
        "print('tdx_wire_socket=' + str('axdata_core._tdx_wire.transport.socket' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "provider_wire=True" in result.stdout
    assert "tdx_wire_client=False" in result.stdout
    assert "tdx_wire_hosts=False" in result.stdout
    assert "tdx_wire_socket=False" in result.stdout


def test_tdx_provider_wire_root_import_is_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx._tdx_wire as wire\n"
        "print('provider_wire=' + str('axdata_source_tdx._tdx_wire' in sys.modules))\n"
        "print('provider_client=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('provider_protocol=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('provider_models=' + str('axdata_source_tdx._tdx_wire.models' in sys.modules))\n"
        "client_cls = wire.TdxClient\n"
        "print('client_module=' + client_cls.__module__)\n"
        "print('provider_client_after=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "provider_wire=True" in result.stdout
    assert "provider_client=False" in result.stdout
    assert "provider_protocol=False" in result.stdout
    assert "provider_models=False" in result.stdout
    assert "client_module=axdata_source_tdx._tdx_wire.client" in result.stdout
    assert "provider_client_after=True" in result.stdout


def test_tdx_provider_wire_model_exports_are_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx._tdx_wire.models as models\n"
        "print('models_package=' + str('axdata_source_tdx._tdx_wire.models' in sys.modules))\n"
        "print('security_before=' + str('axdata_source_tdx._tdx_wire.models.security' in sys.modules))\n"
        "print('quote_before=' + str('axdata_source_tdx._tdx_wire.models.quote' in sys.modules))\n"
        "print('intraday_before=' + str('axdata_source_tdx._tdx_wire.models.intraday' in sys.modules))\n"
        "from axdata_source_tdx._tdx_wire.models import SecurityCode\n"
        "print('security_module=' + SecurityCode.__module__)\n"
        "print('security_after=' + str('axdata_source_tdx._tdx_wire.models.security' in sys.modules))\n"
        "print('quote_after=' + str('axdata_source_tdx._tdx_wire.models.quote' in sys.modules))\n"
        "print('intraday_after=' + str('axdata_source_tdx._tdx_wire.models.intraday' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
        "print('dir_has_security=' + str('SecurityCode' in dir(models)))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "models_package=True" in result.stdout
    assert "security_before=False" in result.stdout
    assert "quote_before=False" in result.stdout
    assert "intraday_before=False" in result.stdout
    assert "security_module=axdata_source_tdx._tdx_wire.models.security" in result.stdout
    assert "security_after=True" in result.stdout
    assert "quote_after=False" in result.stdout
    assert "intraday_after=False" in result.stdout
    assert "core_wire=False" in result.stdout
    assert "dir_has_security=True" in result.stdout


def _import_from_modules(path: Path) -> set[str]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    return {
        "." * node.level + (node.module or "")
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom)
    }


def test_tdx_provider_wire_api_base_execute_uses_lightweight_command_codes() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.api.base import ApiBase\n"
        "class Transport:\n"
        "    def execute(self, command, payload):\n"
        "        print('executed=' + hex(command) + ':' + repr(payload))\n"
        "        return command, payload\n"
        "result = ApiBase(Transport())._execute('legacy_quotes', securities=[('sz', '000001')])\n"
        "print('result=' + repr(result))\n"
        "print('command_codes=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('protocol_commands=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('protocol_registry=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('protocol_constants=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('transport_package=' + str('axdata_source_tdx._tdx_wire.transport' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "executed=0x53e:{'securities': [('sz', '000001')]}" in result.stdout
    assert "result=(1342, {'securities': [('sz', '000001')]})" in result.stdout
    assert "command_codes=True" in result.stdout
    assert "protocol_commands=False" in result.stdout
    assert "protocol_registry=False" in result.stdout
    assert "protocol_constants=False" in result.stdout
    assert "transport_package=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_connection_defaults_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _connection_defaults as defaults\n"
        "print('defaults=' + ','.join(str(value) for value in (defaults.DEFAULT_PROBE_TIMEOUT, defaults.DEFAULT_PROBE_WORKERS, defaults.DEFAULT_HEARTBEAT_INTERVAL)))\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('hosts_loaded=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('transport_loaded=' + str('axdata_source_tdx._tdx_wire.transport' in sys.modules))\n"
        "print('socket_loaded=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('pool_loaded=' + str('axdata_source_tdx._tdx_wire.transport.pool' in sys.modules))\n"
        "print('protocol_loaded=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "defaults=1.2,32,None" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "hosts_loaded=False" in result.stdout
    assert "transport_loaded=False" in result.stdout
    assert "socket_loaded=False" in result.stdout
    assert "pool_loaded=False" in result.stdout
    assert "protocol_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_codes_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _command_codes as codes\n"
        "print('values=' + ','.join((hex(codes.TYPE_HEARTBEAT), hex(codes.TYPE_HANDSHAKE), hex(codes.COMMAND_CODES['legacy_quotes']), hex(codes.COMMAND_CODES['recent_historical_intraday']))))\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('transport_loaded=' + str('axdata_source_tdx._tdx_wire.transport' in sys.modules))\n"
        "print('socket_loaded=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('protocol_loaded=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('commands_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "values=0x4,0xd,0x53e,0xfeb" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "transport_loaded=False" in result.stdout
    assert "socket_loaded=False" in result.stdout
    assert "protocol_loaded=False" in result.stdout
    assert "commands_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_defaults_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _command_defaults as defaults\n"
        "print('defaults=' + ','.join((str(defaults.DEFAULT_AUCTION_MODE), str(defaults.DEFAULT_AUCTION_COUNT), defaults.DEFAULT_TODAY_INTRADAY_RESERVED_TAIL.hex(), hex(defaults.RECENT_HISTORICAL_INTRADAY_DATE_BASE), str(defaults.FILE_PATH_FIELD_SIZE))))\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('api_loaded=' + str('axdata_source_tdx._tdx_wire.api' in sys.modules))\n"
        "print('models_loaded=' + str('axdata_source_tdx._tdx_wire.models' in sys.modules))\n"
        "print('protocol_loaded=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('commands_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('auction_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.auction' in sys.modules))\n"
        "print('intraday_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.intraday' in sys.modules))\n"
        "print('resources_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.resources' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "defaults=3,500,00000093,0xfed62304,300" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "api_loaded=False" in result.stdout
    assert "models_loaded=False" in result.stdout
    assert "protocol_loaded=False" in result.stdout
    assert "commands_loaded=False" in result.stdout
    assert "auction_loaded=False" in result.stdout
    assert "intraday_loaded=False" in result.stdout
    assert "resources_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_request_defaults_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _request_defaults as defaults\n"
        "print('defaults=' + ','.join(str(value) for value in (defaults.DEFAULT_CODE_PAGE_SIZE, defaults.DEFAULT_QUOTE_BATCH_SIZE, defaults.DEFAULT_FILE_CHUNK_SIZE)))\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('api_loaded=' + str('axdata_source_tdx._tdx_wire.api' in sys.modules))\n"
        "print('models_loaded=' + str('axdata_source_tdx._tdx_wire.models' in sys.modules))\n"
        "print('protocol_loaded=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('commands_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('resources_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.resources' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "defaults=1600,80,30000" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "api_loaded=False" in result.stdout
    assert "models_loaded=False" in result.stdout
    assert "protocol_loaded=False" in result.stdout
    assert "constants_loaded=False" in result.stdout
    assert "commands_loaded=False" in result.stdout
    assert "resources_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_layouts_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _command_layouts as layouts\n"
        "print('record_sizes=' + ','.join(str(value) for value in (layouts.CAPITAL_CHANGE_RECORD_SIZE, layouts.FINANCE_INFO_RECORD_SIZE, layouts.FINANCE_INFO_BODY_SIZE, layouts.PRICE_LIMIT_RECORD_SIZE, layouts.CODE_RECORD_SIZE)))\n"
        "print('subchart=' + str(layouts.INTRADAY_SUBCHART_VOLUME_COMPARISON) + ':' + layouts.INTRADAY_SUBCHART_SELECTOR_NAMES[layouts.INTRADAY_SUBCHART_VOLUME_COMPARISON])\n"
        "print('side=' + layouts.SIDE_MAP[1])\n"
        "print('security_prefix=' + layouts.SSE_A_SHARE_PREFIXES[0])\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('api_loaded=' + str('axdata_source_tdx._tdx_wire.api' in sys.modules))\n"
        "print('models_loaded=' + str('axdata_source_tdx._tdx_wire.models' in sys.modules))\n"
        "print('protocol_loaded=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('commands_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('security_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.security' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "record_sizes=29,143,136,13,37" in result.stdout
    assert "subchart=11:volume_comparison" in result.stdout
    assert "side=sell" in result.stdout
    assert "security_prefix=sh600" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "api_loaded=False" in result.stdout
    assert "models_loaded=False" in result.stdout
    assert "protocol_loaded=False" in result.stdout
    assert "commands_loaded=False" in result.stdout
    assert "security_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_security_classification_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire._security_classification import classify_board, classify_security\n"
        "category, reason = classify_security('sh600000')\n"
        "board, board_reason = classify_board('sh600000', category)\n"
        "print('classification=' + category + ':' + board)\n"
        "print('reasons=' + str(bool(reason)) + ':' + str(bool(board_reason)))\n"
        "print('layout_loaded=' + str('axdata_source_tdx._tdx_wire._command_layouts' in sys.modules))\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('api_loaded=' + str('axdata_source_tdx._tdx_wire.api' in sys.modules))\n"
        "print('models_loaded=' + str('axdata_source_tdx._tdx_wire.models' in sys.modules))\n"
        "print('protocol_loaded=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('commands_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('security_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.security' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "classification=a_share:sse_main_board" in result.stdout
    assert "reasons=True:True" in result.stdout
    assert "layout_loaded=True" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "api_loaded=False" in result.stdout
    assert "models_loaded=False" in result.stdout
    assert "protocol_loaded=False" in result.stdout
    assert "commands_loaded=False" in result.stdout
    assert "security_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_transport_exports_are_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx._tdx_wire.transport as transport\n"
        "print('transport_package=' + str('axdata_source_tdx._tdx_wire.transport' in sys.modules))\n"
        "print('base_before=' + str('axdata_source_tdx._tdx_wire.transport.base' in sys.modules))\n"
        "print('socket_before=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('pool_before=' + str('axdata_source_tdx._tdx_wire.transport.pool' in sys.modules))\n"
        "from axdata_source_tdx._tdx_wire.transport import Transport\n"
        "print('transport_module=' + Transport.__module__)\n"
        "print('base_after_transport=' + str('axdata_source_tdx._tdx_wire.transport.base' in sys.modules))\n"
        "print('socket_after_transport=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "from axdata_source_tdx._tdx_wire.transport import SocketTransport\n"
        "print('socket_module=' + SocketTransport.__module__)\n"
        "print('socket_after_socket=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('pool_after_socket=' + str('axdata_source_tdx._tdx_wire.transport.pool' in sys.modules))\n"
        "print('dir_has_socket=' + str('SocketTransport' in dir(transport)))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "transport_package=True" in result.stdout
    assert "base_before=False" in result.stdout
    assert "socket_before=False" in result.stdout
    assert "pool_before=False" in result.stdout
    assert "transport_module=axdata_source_tdx._tdx_wire.transport.base" in result.stdout
    assert "base_after_transport=True" in result.stdout
    assert "socket_after_transport=False" in result.stdout
    assert "socket_module=axdata_source_tdx._tdx_wire.transport.socket" in result.stdout
    assert "socket_after_socket=True" in result.stdout
    assert "pool_after_socket=False" in result.stdout
    assert "dir_has_socket=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_pool_import_defers_socket_and_hosts() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport.pool import PooledSocketTransport\n"
        "print('pool_module=' + PooledSocketTransport.__module__)\n"
        "print('socket_module=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('hosts_module=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('protocol_commands=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "pool_module=axdata_source_tdx._tdx_wire.transport.pool" in result.stdout
    assert "socket_module=False" in result.stdout
    assert "hosts_module=False" in result.stdout
    assert "protocol_commands=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_pool_import_defers_stdlib_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport import pool\n"
        "print('threading_after_import=' + str('threading' in sys.modules))\n"
        "print('itertools_attr_after_import=' + str('itertools' in pool.__dict__))\n"
        "print('socket_transport_after_import=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "transport = pool.PooledSocketTransport(hosts=['127.0.0.1:7709'], pool_size=2)\n"
        "print('ping=' + transport.request('ping'))\n"
        "print('threading_after_init=' + str('threading' in sys.modules))\n"
        "print('itertools_after_init=' + str('itertools' in sys.modules))\n"
        "print('itertools_attr_after_init=' + str('itertools' in pool.__dict__))\n"
        "print('socket_transport_after_init=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('threading_attr=' + pool.threading.__name__)\n"
        "print('itertools_attr=' + pool.itertools.__name__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "threading_after_import=False" in result.stdout
    assert "itertools_attr_after_import=False" in result.stdout
    assert "socket_transport_after_import=False" in result.stdout
    assert "ping=pong" in result.stdout
    assert "threading_after_init=True" in result.stdout
    assert "itertools_after_init=True" in result.stdout
    assert "itertools_attr_after_init=True" in result.stdout
    assert "socket_transport_after_init=False" in result.stdout
    assert "threading_attr=threading" in result.stdout
    assert "itertools_attr=itertools" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_socket_import_defers_session_codes_hosts_and_command_codec() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport import socket as socket_transport\n"
        "SocketTransport = socket_transport.SocketTransport\n"
        "print('socket_module=' + SocketTransport.__module__)\n"
        "socket_instance = SocketTransport(hosts=['127.0.0.1:7709'])\n"
        "print('ping=' + socket_instance.request('ping'))\n"
        "print('exceptions=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('command_codes=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('hosts_module=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('protocol_commands=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('commands_codec=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('command_quotes=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('protocol_frame=' + str('axdata_source_tdx._tdx_wire.protocol.frame' in sys.modules))\n"
        "print('protocol_constants=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
        "print('all_has_handshake=' + str('TYPE_HANDSHAKE' in socket_transport.__all__))\n"
        "print('all_has_heartbeat=' + str('TYPE_HEARTBEAT' in socket_transport.__all__))\n"
        "print('all_has_protocol_error=' + str('ProtocolError' in socket_transport.__all__))\n"
        "print('protocol_error_module=' + socket_transport.ProtocolError.__module__)\n"
        "print('exceptions_after_export=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('handshake=' + hex(socket_transport.TYPE_HANDSHAKE))\n"
        "print('heartbeat=' + hex(socket_transport.TYPE_HEARTBEAT))\n"
        "print('command_codes_after_export=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('protocol_commands_after_export=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "socket_module=axdata_source_tdx._tdx_wire.transport.socket" in result.stdout
    assert "ping=pong" in result.stdout
    assert "exceptions=False" in result.stdout
    assert "command_codes=False" in result.stdout
    assert "hosts_module=False" in result.stdout
    assert "protocol_commands=False" in result.stdout
    assert "commands_codec=False" in result.stdout
    assert "command_quotes=False" in result.stdout
    assert "protocol_frame=False" in result.stdout
    assert "protocol_constants=False" in result.stdout
    assert "core_wire=False" in result.stdout
    assert "all_has_handshake=True" in result.stdout
    assert "all_has_heartbeat=True" in result.stdout
    assert "all_has_protocol_error=True" in result.stdout
    assert "protocol_error_module=axdata_source_tdx._tdx_wire.exceptions" in result.stdout
    assert "exceptions_after_export=True" in result.stdout
    assert "handshake=0xd" in result.stdout
    assert "heartbeat=0x4" in result.stdout
    assert "command_codes_after_export=True" in result.stdout
    assert "protocol_commands_after_export=False" in result.stdout


def test_tdx_provider_wire_socket_import_defers_stdlib_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport import socket as socket_transport\n"
        "print('socket_after_import=' + str('socket' in sys.modules))\n"
        "print('threading_after_import=' + str('threading' in sys.modules))\n"
        "print('queue_after_import=' + str('queue' in sys.modules))\n"
        "transport = socket_transport.SocketTransport(hosts=['127.0.0.1:7709'])\n"
        "print('ping=' + transport.request('ping'))\n"
        "print('socket_after_init=' + str('socket' in sys.modules))\n"
        "print('threading_after_init=' + str('threading' in sys.modules))\n"
        "print('queue_after_init=' + str('queue' in sys.modules))\n"
        "print('socket_attr=' + socket_transport.socket.__name__)\n"
        "print('socket_after_attr=' + str('socket' in sys.modules))\n"
        "print('queue_attr=' + socket_transport.Queue.__module__)\n"
        "print('threading_attr=' + socket_transport.threading.__name__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "socket_after_import=False" in result.stdout
    assert "threading_after_import=False" in result.stdout
    assert "queue_after_import=False" in result.stdout
    assert "ping=pong" in result.stdout
    assert "socket_after_init=False" in result.stdout
    assert "threading_after_init=True" in result.stdout
    assert "queue_after_init=True" in result.stdout
    assert "socket_attr=socket" in result.stdout
    assert "socket_after_attr=True" in result.stdout
    assert "queue_attr=queue" in result.stdout
    assert "threading_attr=threading" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_socket_frame_exports_are_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport import socket as socket_transport\n"
        "print('frame_before=' + str('axdata_source_tdx._tdx_wire.protocol.frame' in sys.modules))\n"
        "print('constants_before=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "ResponseFrame = socket_transport.ResponseFrame\n"
        "print('response_frame_module=' + ResponseFrame.__module__)\n"
        "print('frame_after=' + str('axdata_source_tdx._tdx_wire.protocol.frame' in sys.modules))\n"
        "print('constants_after=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "frame_before=False" in result.stdout
    assert "constants_before=False" in result.stdout
    assert "response_frame_module=axdata_source_tdx._tdx_wire.protocol.frame" in result.stdout
    assert "frame_after=True" in result.stdout
    assert "constants_after=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_frame_import_defers_protocol_constants() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import RequestFrame, decode_response\n"
        "frame = RequestFrame(msg_id=5, msg_type=0x053e, data=b'abc')\n"
        "response = decode_response(bytes.fromhex('b1cb74000105000000003e0500000000'))\n"
        "print('frame_module=' + RequestFrame.__module__)\n"
        "print('frame_bytes=' + frame.to_bytes().hex())\n"
        "print('decode_module=' + decode_response.__module__)\n"
        "print('response_type=' + type(response).__module__ + '.' + type(response).__name__)\n"
        "print('constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('commands_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('frame_constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol._frame_constants' in sys.modules))\n"
        "print('exceptions_loaded=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "frame_module=axdata_source_tdx._tdx_wire.protocol.frame" in result.stdout
    assert "frame_bytes=0c0500000001050005003e05616263" in result.stdout
    assert "decode_module=axdata_source_tdx._tdx_wire.protocol.frame" in result.stdout
    assert "response_type=axdata_source_tdx._tdx_wire.protocol.frame.ResponseFrame" in result.stdout
    assert "constants_loaded=False" in result.stdout
    assert "commands_loaded=False" in result.stdout
    assert "frame_constants_loaded=True" in result.stdout
    assert "exceptions_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_frame_errors_load_exceptions_on_demand() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol import frame\n"
        "print('exceptions_before=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "try:\n"
        "    frame.decode_response(b'bad')\n"
        "except Exception as exc:\n"
        "    print('exc_type=' + type(exc).__module__ + '.' + type(exc).__name__)\n"
        "print('exceptions_after_error=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "ProtocolError = frame.ProtocolError\n"
        "print('compat_error=' + ProtocolError.__module__ + '.' + ProtocolError.__name__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_before=False" in result.stdout
    assert "exc_type=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_error=True" in result.stdout
    assert "compat_error=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout


def test_tdx_provider_wire_explicit_hosts_avoid_default_host_resource() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport.pool import PooledSocketTransport\n"
        "from axdata_source_tdx._tdx_wire.transport.socket import SocketTransport\n"
        "socket_transport = SocketTransport(hosts=[' 127.0.0.1:7709 ', '127.0.0.1:7709', 'bad'])\n"
        "pool_transport = PooledSocketTransport(hosts=[' 127.0.0.2:7709 ', 'bad'], pool_size=1)\n"
        "print('socket_hosts=' + ','.join(socket_transport._hosts))\n"
        "print('pool_hosts=' + ','.join(pool_transport.hosts))\n"
        "print('host_utils=' + str('axdata_source_tdx._tdx_wire._host_utils' in sys.modules))\n"
        "print('host_resource=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('hosts_module=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('protocol_commands=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('commands_codec=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "socket_hosts=127.0.0.1:7709" in result.stdout
    assert "pool_hosts=127.0.0.2:7709" in result.stdout
    assert "host_utils=True" in result.stdout
    assert "host_resource=False" in result.stdout
    assert "hosts_module=False" in result.stdout
    assert "protocol_commands=False" in result.stdout
    assert "commands_codec=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_exports_are_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx._tdx_wire.protocol.commands as commands\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('registry_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('lookup_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.lookup' in sys.modules))\n"
        "print('codec_before=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('quotes_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import command_code\n"
        "print('command_code_module=' + command_code.__module__)\n"
        "print('command_code_value=' + hex(command_code('legacy_quotes')))\n"
        "print('registry_after_command=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('lookup_after_command=' + str('axdata_source_tdx._tdx_wire.protocol.commands.lookup' in sys.modules))\n"
        "print('command_codes_after_command=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('codec_after_command=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import build_command_frame\n"
        "print('build_module=' + build_command_frame.__module__)\n"
        "print('codec_after_build=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('quotes_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('session_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.session' in sys.modules))\n"
        "print('constants_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "codec_module = sys.modules[build_command_frame.__module__]\n"
        "print('codec_builders_cached=' + str('BUILDERS' in codec_module.__dict__))\n"
        "print('codec_parsers_cached=' + str('PARSERS' in codec_module.__dict__))\n"
        "print('codec_builders_type=' + type(codec_module.BUILDERS).__name__)\n"
        "print('codec_parsers_type=' + type(codec_module.PARSERS).__name__)\n"
        "print('dir_has_build=' + str('build_command_frame' in dir(commands)))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "commands_package=True" in result.stdout
    assert "registry_before=False" in result.stdout
    assert "lookup_before=False" in result.stdout
    assert "codec_before=False" in result.stdout
    assert "quotes_before=False" in result.stdout
    assert "command_code_module=axdata_source_tdx._tdx_wire._command_lookup" in result.stdout
    assert "command_code_value=0x53e" in result.stdout
    assert "registry_after_command=False" in result.stdout
    assert "lookup_after_command=False" in result.stdout
    assert "command_codes_after_command=True" in result.stdout
    assert "codec_after_command=False" in result.stdout
    assert "build_module=axdata_source_tdx._tdx_wire._command_codec" in result.stdout
    assert "codec_after_build=True" in result.stdout
    assert "quotes_after_build=False" in result.stdout
    assert "session_after_build=False" in result.stdout
    assert "constants_after_build=False" in result.stdout
    assert "codec_builders_cached=False" in result.stdout
    assert "codec_parsers_cached=False" in result.stdout
    assert "codec_builders_type=dict" in result.stdout
    assert "codec_parsers_type=dict" in result.stdout
    assert "dir_has_build=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_codec_uses_lightweight_dispatch_facts() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import build_command_frame, parse_command_response\n"
        "print('build_module=' + build_command_frame.__module__)\n"
        "print('parse_module=' + parse_command_response.__module__)\n"
        "print('command_codes=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('command_dispatch=' + str('axdata_source_tdx._tdx_wire._command_dispatch' in sys.modules))\n"
        "print('root_codec=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('legacy_codec=' + str('axdata_source_tdx._tdx_wire.protocol.commands.codec' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('quotes_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('session_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.session' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
        "codec_module = sys.modules[build_command_frame.__module__]\n"
        "print('codec_builders_cached=' + str('BUILDERS' in codec_module.__dict__))\n"
        "print('codec_parsers_cached=' + str('PARSERS' in codec_module.__dict__))\n"
        "builders = codec_module.BUILDERS\n"
        "parsers = codec_module.PARSERS\n"
        "print('builder_count=' + str(len(builders)))\n"
        "print('parser_count=' + str(len(parsers)))\n"
        "print('command_codes_after_tables=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('command_dispatch_after_tables=' + str('axdata_source_tdx._tdx_wire._command_dispatch' in sys.modules))\n"
        "codes_module = sys.modules['axdata_source_tdx._tdx_wire._command_codes']\n"
        "dispatch_module = sys.modules['axdata_source_tdx._tdx_wire._command_dispatch']\n"
        "print('command_codes_export_cached_after_tables=' + str('COMMAND_CODES' in codes_module.__dict__))\n"
        "print('builder_targets_export_cached_after_tables=' + str('BUILDER_TARGETS' in dispatch_module.__dict__))\n"
        "print('parser_targets_export_cached_after_tables=' + str('PARSER_TARGETS' in dispatch_module.__dict__))\n"
        "print('explicit_legacy_quotes=' + hex(codes_module.COMMAND_CODES['legacy_quotes']))\n"
        "print('explicit_builder_targets=' + str(len(dispatch_module.BUILDER_TARGETS)))\n"
        "print('explicit_parser_targets=' + str(len(dispatch_module.PARSER_TARGETS)))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "build_module=axdata_source_tdx._tdx_wire._command_codec" in result.stdout
    assert "parse_module=axdata_source_tdx._tdx_wire._command_codec" in result.stdout
    assert "command_codes=False" in result.stdout
    assert "command_dispatch=False" in result.stdout
    assert "root_codec=True" in result.stdout
    assert "legacy_codec=False" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "constants_loaded=False" in result.stdout
    assert "quotes_loaded=False" in result.stdout
    assert "session_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout
    assert "codec_builders_cached=False" in result.stdout
    assert "codec_parsers_cached=False" in result.stdout
    assert "builder_count=20" in result.stdout
    assert "parser_count=20" in result.stdout
    assert "command_codes_after_tables=True" in result.stdout
    assert "command_dispatch_after_tables=True" in result.stdout
    assert "command_codes_export_cached_after_tables=False" in result.stdout
    assert "builder_targets_export_cached_after_tables=False" in result.stdout
    assert "parser_targets_export_cached_after_tables=False" in result.stdout
    assert "explicit_legacy_quotes=0x53e" in result.stdout
    assert "explicit_builder_targets=20" in result.stdout
    assert "explicit_parser_targets=20" in result.stdout


def test_tdx_provider_wire_command_codec_compat_path_routes_to_root_codec() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands.codec import build_command_frame, parse_command_response\n"
        "print('build_module=' + build_command_frame.__module__)\n"
        "print('parse_module=' + parse_command_response.__module__)\n"
        "print('root_codec=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('legacy_codec=' + str('axdata_source_tdx._tdx_wire.protocol.commands.codec' in sys.modules))\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
        "codec_module = sys.modules[build_command_frame.__module__]\n"
        "print('codec_builders_cached=' + str('BUILDERS' in codec_module.__dict__))\n"
        "print('codec_parsers_cached=' + str('PARSERS' in codec_module.__dict__))\n"
        "print('builder_count=' + str(len(codec_module.BUILDERS)))\n"
        "print('parser_count=' + str(len(codec_module.PARSERS)))\n"
        "codes_module = sys.modules['axdata_source_tdx._tdx_wire._command_codes']\n"
        "dispatch_module = sys.modules['axdata_source_tdx._tdx_wire._command_dispatch']\n"
        "print('provider_command_codes_export_cached_after_tables=' + str('COMMAND_CODES' in codes_module.__dict__))\n"
        "print('provider_builder_targets_export_cached_after_tables=' + str('BUILDER_TARGETS' in dispatch_module.__dict__))\n"
        "print('provider_parser_targets_export_cached_after_tables=' + str('PARSER_TARGETS' in dispatch_module.__dict__))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "build_module=axdata_source_tdx._tdx_wire._command_codec" in result.stdout
    assert "parse_module=axdata_source_tdx._tdx_wire._command_codec" in result.stdout
    assert "root_codec=True" in result.stdout
    assert "legacy_codec=True" in result.stdout
    assert "commands_package=True" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout
    assert "codec_builders_cached=False" in result.stdout
    assert "codec_parsers_cached=False" in result.stdout
    assert "builder_count=20" in result.stdout
    assert "parser_count=20" in result.stdout
    assert "provider_command_codes_export_cached_after_tables=False" in result.stdout
    assert "provider_builder_targets_export_cached_after_tables=False" in result.stdout
    assert "provider_parser_targets_export_cached_after_tables=False" in result.stdout


def test_tdx_provider_wire_codec_loads_only_used_command_module() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import build_command_frame, parse_command_response\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "TYPE_LEGACY_QUOTES = 0x053e\n"
        "print('constants_before=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('quotes_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('klines_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('session_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.session' in sys.modules))\n"
        "print('security_before=' + str('axdata_source_tdx._tdx_wire.protocol.commands.security' in sys.modules))\n"
        "frame = build_command_frame(TYPE_LEGACY_QUOTES, {'securities': [('sz', '000001')]}, 5)\n"
        "print('frame_type=' + frame.__class__.__name__)\n"
        "print('quotes_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('klines_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('session_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.session' in sys.modules))\n"
        "codec_module = sys.modules[build_command_frame.__module__]\n"
        "print('builders_export_cached_after_build=' + str('BUILDERS' in codec_module.__dict__))\n"
        "print('parsers_export_cached_after_build=' + str('PARSERS' in codec_module.__dict__))\n"
        "print('builder_cache_size_after_build=' + str(len(codec_module._BUILDER_CACHE)))\n"
        "print('parser_cache_size_after_build=' + str(len(codec_module._PARSER_CACHE)))\n"
        "response = ResponseFrame(control=1, msg_id=5, msg_type=TYPE_LEGACY_QUOTES, zip_length=0, length=0, data=b'', raw=b'')\n"
        "try:\n"
        "    parse_command_response(TYPE_LEGACY_QUOTES, response, {'securities': [('sz', '000001')]})\n"
        "except Exception as exc:\n"
        "    print('parse_exc=' + exc.__class__.__name__)\n"
        "print('quotes_after_parse=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('klines_after_parse=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('security_after_parse=' + str('axdata_source_tdx._tdx_wire.protocol.commands.security' in sys.modules))\n"
        "print('constants_after_parse=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('builders_export_cached_after_parse=' + str('BUILDERS' in codec_module.__dict__))\n"
        "print('parsers_export_cached_after_parse=' + str('PARSERS' in codec_module.__dict__))\n"
        "print('builder_cache_size_after_parse=' + str(len(codec_module._BUILDER_CACHE)))\n"
        "print('parser_cache_size_after_parse=' + str(len(codec_module._PARSER_CACHE)))\n"
        "codes_module = sys.modules['axdata_source_tdx._tdx_wire._command_codes']\n"
        "dispatch_module = sys.modules['axdata_source_tdx._tdx_wire._command_dispatch']\n"
        "print('command_codes_export_cached_after_parse=' + str('COMMAND_CODES' in codes_module.__dict__))\n"
        "print('builder_targets_export_cached_after_parse=' + str('BUILDER_TARGETS' in dispatch_module.__dict__))\n"
        "print('parser_targets_export_cached_after_parse=' + str('PARSER_TARGETS' in dispatch_module.__dict__))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "constants_before=False" in result.stdout
    assert "quotes_before=False" in result.stdout
    assert "klines_before=False" in result.stdout
    assert "session_before=False" in result.stdout
    assert "security_before=False" in result.stdout
    assert "frame_type=RequestFrame" in result.stdout
    assert "quotes_after_build=True" in result.stdout
    assert "klines_after_build=False" in result.stdout
    assert "session_after_build=False" in result.stdout
    assert "builders_export_cached_after_build=False" in result.stdout
    assert "parsers_export_cached_after_build=False" in result.stdout
    assert "builder_cache_size_after_build=1" in result.stdout
    assert "parser_cache_size_after_build=0" in result.stdout
    assert "parse_exc=ProtocolError" in result.stdout
    assert "quotes_after_parse=True" in result.stdout
    assert "klines_after_parse=False" in result.stdout
    assert "security_after_parse=False" in result.stdout
    assert "constants_after_parse=False" in result.stdout
    assert "builders_export_cached_after_parse=False" in result.stdout
    assert "parsers_export_cached_after_parse=False" in result.stdout
    assert "builder_cache_size_after_parse=1" in result.stdout
    assert "parser_cache_size_after_parse=1" in result.stdout
    assert "command_codes_export_cached_after_parse=False" in result.stdout
    assert "builder_targets_export_cached_after_parse=False" in result.stdout
    assert "parser_targets_export_cached_after_parse=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_modules_defer_protocol_constants() -> None:
    code = (
        "import importlib, sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "modules = [\n"
        "    'auction', 'corporate', 'finance', 'intraday', 'klines', 'price_limits',\n"
        "    'quotes', 'resources', 'security', 'session', 'subchart', 'trades',\n"
        "]\n"
        "for module in modules:\n"
        "    importlib.import_module(f'axdata_source_tdx._tdx_wire.protocol.commands.{module}')\n"
        "print('constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('command_codes_loaded=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('frame_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.frame' in sys.modules))\n"
        "print('frame_constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol._frame_constants' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "constants_loaded=False" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "command_codes_loaded=True" in result.stdout
    assert "frame_loaded=True" in result.stdout
    assert "frame_constants_loaded=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_small_command_modules_defer_exceptions_until_errors() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import price_limits, resources, session\n"
        "def response(msg_type, data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=msg_type, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "resource_frame = resources.build_file_content_frame({'path': 'T0002/hq_cache.dat'}, 7)\n"
        "heartbeat_frame = session.build_heartbeat_frame({}, 8)\n"
        "price_frame = price_limits.build_price_limits_frame({'start': 1}, 9)\n"
        "print('resource_frame=' + resource_frame.to_bytes().hex())\n"
        "print('heartbeat_frame=' + heartbeat_frame.to_bytes().hex())\n"
        "print('price_frame=' + price_frame.to_bytes().hex())\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "for name, func, response in [\n"
        "    ('resource', resources.parse_file_content_payload, response(resources.TYPE_FILE_CONTENT, b'\\x02\\x00\\x00\\x00a')),\n"
        "    ('heartbeat', session.parse_heartbeat_payload, response(session.TYPE_HEARTBEAT, b'bad')),\n"
        "    ('price', price_limits.parse_price_limits_payload, response(price_limits.TYPE_PRICE_LIMITS, b'\\x01\\x00')),\n"
        "]:\n"
        "    try:\n"
        "        func(response)\n"
        "    except Exception as exc:\n"
        "        print(name + '_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_errors=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('resource_compat=' + resources.ProtocolError.__module__ + '.' + resources.ProtocolError.__name__)\n"
        "print('session_compat=' + session.ProtocolError.__module__ + '.' + session.ProtocolError.__name__)\n"
        "print('price_compat=' + price_limits.ProtocolError.__module__ + '.' + price_limits.ProtocolError.__name__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "resource_frame=0c070000000136013601b906000000003075000054303030322f68715f63616368652e646174" in result.stdout
    assert "heartbeat_frame=0c0800000001020002000400" in result.stdout
    assert "price_frame=0c09000000011000100052040100000000000000000000000000" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "resource_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "heartbeat_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "price_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_errors=True" in result.stdout
    assert "resource_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "session_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "price_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_quote_price_commands_defer_struct_runtime_imports() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import price_limits, quotes\n"
        "def flags(label):\n"
        "    print(label + '=' + str('struct' in quotes.__dict__) + '/' + str('struct' in price_limits.__dict__))\n"
        "print('binary_before=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "flags('initial')\n"
        "legacy_frame = quotes.build_legacy_quotes_frame({'securities': [('sz', '000001')]}, 7)\n"
        "print('legacy_frame=' + legacy_frame.to_bytes().hex())\n"
        "flags('after_legacy_build')\n"
        "print('binary_after_legacy_build=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "response = ResponseFrame(control=1, msg_id=7, msg_type=quotes.TYPE_LEGACY_QUOTES, zip_length=4, length=4, data=b'\\x00\\x00\\x00\\x00', raw=b'')\n"
        "print('legacy_count=' + str(len(quotes.parse_legacy_quotes_payload(response, {'securities': [('sz', '000001')]}))))\n"
        "flags('after_legacy_parse')\n"
        "print('binary_after_legacy_parse=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "category_frame = quotes.build_category_quotes_frame({'category': 6, 'sort': 0, 'start': 0, 'count': 1}, 8)\n"
        "print('category_frame=' + category_frame.to_bytes().hex())\n"
        "flags('after_category_build')\n"
        "print('binary_after_category_build=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "price_frame = price_limits.build_price_limits_frame({'start': 1}, 9)\n"
        "print('price_frame=' + price_frame.to_bytes().hex())\n"
        "flags('after_price_build')\n"
        "print('quote_binary_compat=' + quotes.little_u16.__module__)\n"
        "print('quote_struct_compat=' + quotes.struct.__name__)\n"
        "print('price_struct_compat=' + price_limits.struct.__name__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "binary_before=False" in result.stdout
    assert "initial=False/False" in result.stdout
    assert "legacy_frame=" in result.stdout
    assert "after_legacy_build=False/False" in result.stdout
    assert "binary_after_legacy_build=False" in result.stdout
    assert "legacy_count=0" in result.stdout
    assert "after_legacy_parse=False/False" in result.stdout
    assert "binary_after_legacy_parse=True" in result.stdout
    assert "category_frame=" in result.stdout
    assert "after_category_build=True/False" in result.stdout
    assert "binary_after_category_build=True" in result.stdout
    assert "price_frame=" in result.stdout
    assert "after_price_build=True/True" in result.stdout
    assert "quote_binary_compat=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "quote_struct_compat=struct" in result.stdout
    assert "price_struct_compat=struct" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_quote_kline_commands_defer_exceptions_until_errors() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import klines, quotes\n"
        "def response(msg_type, data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=msg_type, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('time_utils_after_import=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "kline_frame = klines.build_klines_frame({'code': '000001.SZ', 'period': '5s', 'start': 0, 'count': 1}, 21)\n"
        "quote_frame = quotes.build_legacy_quotes_frame({'securities': [('sz', '000001')]}, 22)\n"
        "print('kline_frame=' + kline_frame.to_bytes().hex())\n"
        "print('quote_frame=' + quote_frame.to_bytes().hex())\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('time_utils_after_build=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "for name, func, item, payload in [\n"
        "    ('kline', klines.parse_klines_payload, response(klines.TYPE_KLINES, b'\\x01'), {'code': 'sz000001'}),\n"
        "    ('quote', quotes.parse_legacy_quotes_payload, response(quotes.TYPE_LEGACY_QUOTES, b''), {'securities': [('sz', '000001')]}),\n"
        "]:\n"
        "    try:\n"
        "        func(item, payload)\n"
        "    except Exception as exc:\n"
        "        print(name + '_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_errors=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('kline_compat=' + klines.ProtocolError.__module__ + '.' + klines.ProtocolError.__name__)\n"
        "print('quote_compat=' + quotes.ProtocolError.__module__ + '.' + quotes.ProtocolError.__name__)\n"
        "print('kline_timezone_module=' + klines.SHANGHAI_TZ.__class__.__module__)\n"
        "print('time_utils_after_compat=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "time_utils_after_import=False" in result.stdout
    assert "kline_frame=0c15000000012c002c002d0500003030303030310d00050000000100000000000000000000000000000000000000000000000000" in result.stdout
    assert "quote_frame=0c1600000001130013003e050500000000000000010000303030303031" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "time_utils_after_build=False" in result.stdout
    assert "kline_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "quote_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_errors=True" in result.stdout
    assert "kline_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "quote_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "kline_timezone_module=datetime" in result.stdout
    assert "time_utils_after_compat=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_corporate_security_commands_defer_exceptions_until_errors() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import corporate, security\n"
        "def response(msg_type, data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=msg_type, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "corporate_frame = corporate.build_capital_changes_frame({'code': '000001.SZ'}, 10)\n"
        "count_frame = security.build_security_count_frame({'market': 'sz', 'client_date': '2026-06-27'}, 11)\n"
        "list_frame = security.build_security_list_frame({'market': 'sh', 'start': 1, 'limit': 2}, 12)\n"
        "print('corporate_frame=' + corporate_frame.to_bytes().hex())\n"
        "print('count_frame=' + count_frame.to_bytes().hex())\n"
        "print('list_frame=' + list_frame.to_bytes().hex())\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "for name, func, item in [\n"
        "    ('corporate', corporate.parse_capital_changes_payload, response(corporate.TYPE_CAPITAL_CHANGES, b'bad')),\n"
        "    ('security_count', security.parse_security_count_payload, response(security.TYPE_SECURITY_COUNT, b'')),\n"
        "    ('security_list', security.parse_security_list_payload, response(security.TYPE_SECURITY_LIST, b'\\x01\\x00')),\n"
        "]:\n"
        "    try:\n"
        "        func(item)\n"
        "    except Exception as exc:\n"
        "        print(name + '_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_errors=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('corporate_compat=' + corporate.ProtocolError.__module__ + '.' + corporate.ProtocolError.__name__)\n"
        "print('security_compat=' + security.ProtocolError.__module__ + '.' + security.ProtocolError.__name__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "corporate_frame=0c0a000000010b000b000f00010000303030303031" in result.stdout
    assert "count_frame=0c0b00000001080008004e04000013273501" in result.stdout
    assert "list_frame=0c0c00000001100010004d040100010000000200000000000000" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "corporate_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "security_count_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "security_list_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_errors=True" in result.stdout
    assert "corporate_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "security_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_finance_auction_commands_defer_klines_and_exceptions() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import auction, finance\n"
        "def response(msg_type, data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=msg_type, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('klines_after_import=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "auction_frame = auction.build_auction_process_frame({'code': 'sz000001'}, 13)\n"
        "finance_frame = finance.build_finance_info_frame({'code': ['000001.SZ', '600000.SH']}, 14)\n"
        "print('auction_frame=' + auction_frame.to_bytes().hex())\n"
        "print('finance_frame=' + finance_frame.to_bytes().hex())\n"
        "print('klines_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "for name, func, item in [\n"
        "    ('auction', auction.parse_auction_process_payload, response(auction.TYPE_AUCTION_PROCESS, b'bad')),\n"
        "    ('finance', finance.parse_finance_info_payload, response(finance.TYPE_FINANCE_INFO, b'bad')),\n"
        "]:\n"
        "    try:\n"
        "        func(item)\n"
        "    except Exception as exc:\n"
        "        print(name + '_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_errors=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('auction_compat=' + auction.ProtocolError.__module__ + '.' + auction.ProtocolError.__name__)\n"
        "print('finance_compat=' + finance.ProtocolError.__module__ + '.' + finance.ProtocolError.__name__)\n"
        "print('split_code_module=' + auction.split_code.__module__ + ',' + finance.split_code.__module__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "klines_after_import=False" in result.stdout
    assert "auction_frame=0c0d000000011e001e006a05000030303030303100000000030000000000000000000000f4010000" in result.stdout
    assert "finance_frame=0c0e0000000112001200100002000030303030303101363030303030" in result.stdout
    assert "klines_after_build=False" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "auction_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "finance_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_errors=True" in result.stdout
    assert "auction_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "finance_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert (
        "split_code_module=axdata_source_tdx._tdx_wire._code_utils,"
        "axdata_source_tdx._tdx_wire._code_utils"
    ) in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_subchart_command_defers_klines_and_exceptions() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import subchart\n"
        "def response(data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=subchart.TYPE_INTRADAY_SUBCHART, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('klines_after_import=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "frame = subchart.build_intraday_subchart_frame({'code': 'sz000001', 'selector': 'volume_comparison'}, 15)\n"
        "print('subchart_frame=' + frame.to_bytes().hex())\n"
        "print('klines_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "try:\n"
        "    subchart.parse_intraday_subchart_payload(response(b'bad'))\n"
        "except Exception as exc:\n"
        "    print('subchart_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_error=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('subchart_compat=' + subchart.ProtocolError.__module__ + '.' + subchart.ProtocolError.__name__)\n"
        "print('split_code_module=' + subchart.split_code.__module__)\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "klines_after_import=False" in result.stdout
    assert "subchart_frame=0c0f000000011e001e001b050000303030303031000000000000000000000000000000000000000b" in result.stdout
    assert "klines_after_build=False" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "subchart_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_error=True" in result.stdout
    assert "subchart_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "split_code_module=axdata_source_tdx._tdx_wire._code_utils" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_trades_command_defers_klines_and_exceptions() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import trades\n"
        "def response(msg_type, data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=msg_type, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('klines_after_import=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('time_utils_after_import=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "today_frame = trades.build_today_trades_frame({'code': 'sz000001'}, 16)\n"
        "historical_frame = trades.build_historical_trades_frame({'code': 'sz000001', 'trade_date': '2026-05-19'}, 17)\n"
        "print('today_frame=' + today_frame.to_bytes().hex())\n"
        "print('historical_frame=' + historical_frame.to_bytes().hex())\n"
        "print('klines_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('time_utils_after_build=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "try:\n"
        "    trades.parse_today_trades_payload(response(trades.TYPE_TODAY_TRADES, b'bad'))\n"
        "except Exception as exc:\n"
        "    print('trades_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_error=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('trades_compat=' + trades.ProtocolError.__module__ + '.' + trades.ProtocolError.__name__)\n"
        "print('split_code_module=' + trades.split_code.__module__)\n"
        "print('timezone_module=' + trades.SHANGHAI_TZ.__class__.__module__)\n"
        "print('time_utils_after_compat=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "klines_after_import=False" in result.stdout
    assert "time_utils_after_import=False" in result.stdout
    assert "today_frame=0c10000000010e000e00c50f000030303030303100007300" in result.stdout
    assert "historical_frame=0c110000000112001200c60fa7263501000030303030303100008403" in result.stdout
    assert "klines_after_build=False" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "time_utils_after_build=False" in result.stdout
    assert "trades_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_error=True" in result.stdout
    assert "trades_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "split_code_module=axdata_source_tdx._tdx_wire._code_utils" in result.stdout
    assert "timezone_module=datetime" in result.stdout
    assert "time_utils_after_compat=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_intraday_command_defers_klines_and_exceptions() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol.frame import ResponseFrame\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import intraday\n"
        "def response(msg_type, data):\n"
        "    return ResponseFrame(control=1, msg_id=1, msg_type=msg_type, zip_length=len(data), length=len(data), data=data, raw=data)\n"
        "print('exceptions_after_import=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('klines_after_import=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('time_utils_after_import=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "today_frame = intraday.build_today_intraday_frame({'code': 'sz000001'}, 18)\n"
        "historical_frame = intraday.build_historical_intraday_frame({'code': 'sz000001', 'trade_date': '2026-05-19'}, 19)\n"
        "recent_frame = intraday.build_recent_historical_intraday_frame({'code': 'sz000001', 'trade_date': '2026-05-19'}, 20)\n"
        "print('today_frame=' + today_frame.to_bytes().hex())\n"
        "print('historical_frame=' + historical_frame.to_bytes().hex())\n"
        "print('recent_frame=' + recent_frame.to_bytes().hex())\n"
        "print('klines_after_build=' + str('axdata_source_tdx._tdx_wire.protocol.commands.klines' in sys.modules))\n"
        "print('exceptions_after_build=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('time_utils_after_build=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "try:\n"
        "    intraday.parse_today_intraday_payload(response(intraday.TYPE_TODAY_INTRADAY, b'bad'))\n"
        "except Exception as exc:\n"
        "    print('intraday_exc=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_error=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('intraday_compat=' + intraday.ProtocolError.__module__ + '.' + intraday.ProtocolError.__name__)\n"
        "print('split_code_module=' + intraday.split_code.__module__)\n"
        "print('timezone_module=' + intraday.SHANGHAI_TZ.__class__.__module__)\n"
        "print('time_utils_after_compat=' + str('axdata_source_tdx._tdx_wire._time_utils' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_after_import=False" in result.stdout
    assert "klines_after_import=False" in result.stdout
    assert "time_utils_after_import=False" in result.stdout
    assert "today_frame=0c12000000010e000e003705000030303030303100000093" in result.stdout
    assert "historical_frame=0c13000000010d000d00b40fa726350100303030303031" in result.stdout
    assert "recent_frame=0c14000000010d000d00eb0f59d9cafe00303030303031" in result.stdout
    assert "klines_after_build=False" in result.stdout
    assert "exceptions_after_build=False" in result.stdout
    assert "time_utils_after_build=False" in result.stdout
    assert "intraday_exc=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_error=True" in result.stdout
    assert "intraday_compat=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "split_code_module=axdata_source_tdx._tdx_wire._code_utils" in result.stdout
    assert "timezone_module=datetime" in result.stdout
    assert "time_utils_after_compat=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_modules_share_command_defaults() -> None:
    from axdata_source_tdx._tdx_wire import _command_defaults
    from axdata_source_tdx._tdx_wire.protocol.commands import auction, intraday, resources

    assert auction.DEFAULT_AUCTION_MODE is _command_defaults.DEFAULT_AUCTION_MODE
    assert auction.DEFAULT_AUCTION_COUNT is _command_defaults.DEFAULT_AUCTION_COUNT
    assert intraday.DEFAULT_TODAY_INTRADAY_RESERVED_TAIL is _command_defaults.DEFAULT_TODAY_INTRADAY_RESERVED_TAIL
    assert (
        intraday.RECENT_HISTORICAL_INTRADAY_DATE_BASE
        is _command_defaults.RECENT_HISTORICAL_INTRADAY_DATE_BASE
    )
    assert resources.FILE_PATH_FIELD_SIZE is _command_defaults.FILE_PATH_FIELD_SIZE


def test_tdx_provider_wire_request_defaults_are_shared_by_api_and_constants() -> None:
    from axdata_source_tdx._tdx_wire import _request_defaults
    from axdata_source_tdx._tdx_wire.api import codes, quotes, resources
    from axdata_source_tdx._tdx_wire.protocol import constants
    from axdata_source_tdx._tdx_wire.protocol.commands import resources as resource_command

    assert codes.DEFAULT_CODE_PAGE_SIZE is _request_defaults.DEFAULT_CODE_PAGE_SIZE
    assert quotes.DEFAULT_QUOTE_BATCH_SIZE is _request_defaults.DEFAULT_QUOTE_BATCH_SIZE
    assert resources.DEFAULT_FILE_CHUNK_SIZE is _request_defaults.DEFAULT_FILE_CHUNK_SIZE
    assert constants.DEFAULT_CODE_PAGE_SIZE is _request_defaults.DEFAULT_CODE_PAGE_SIZE
    assert constants.DEFAULT_QUOTE_BATCH_SIZE is _request_defaults.DEFAULT_QUOTE_BATCH_SIZE
    assert resource_command.DEFAULT_FILE_CHUNK_SIZE is _request_defaults.DEFAULT_FILE_CHUNK_SIZE


def test_tdx_provider_wire_command_modules_share_command_layouts() -> None:
    from axdata_source_tdx._tdx_wire import _command_layouts
    from axdata_source_tdx._tdx_wire.protocol.commands import (
        auction,
        corporate,
        finance,
        price_limits,
        security,
        subchart,
        trades,
    )

    assert auction.AUCTION_RECORD_SIZE is _command_layouts.AUCTION_RECORD_SIZE
    assert corporate.CAPITAL_CHANGE_RECORD_SIZE is _command_layouts.CAPITAL_CHANGE_RECORD_SIZE
    assert corporate.CAPITAL_CHANGE_CATEGORY_NAMES is _command_layouts.CAPITAL_CHANGE_CATEGORY_NAMES
    assert finance.FINANCE_INFO_RECORD_SIZE is _command_layouts.FINANCE_INFO_RECORD_SIZE
    assert finance.FINANCE_INFO_BODY_SIZE is _command_layouts.FINANCE_INFO_BODY_SIZE
    assert price_limits.PRICE_LIMIT_RECORD_SIZE is _command_layouts.PRICE_LIMIT_RECORD_SIZE
    assert security.CODE_RECORD_SIZE is _command_layouts.CODE_RECORD_SIZE
    assert security.INDEX_PREFIXES is _command_layouts.INDEX_PREFIXES
    assert security.ETF_PREFIXES is _command_layouts.ETF_PREFIXES
    assert security.FUND_PREFIXES is _command_layouts.FUND_PREFIXES
    assert security.SSE_A_SHARE_PREFIXES is _command_layouts.SSE_A_SHARE_PREFIXES
    assert security.SZSE_A_SHARE_PREFIXES is _command_layouts.SZSE_A_SHARE_PREFIXES
    assert (
        subchart.INTRADAY_SUBCHART_BUY_SELL_STRENGTH
        is _command_layouts.INTRADAY_SUBCHART_BUY_SELL_STRENGTH
    )
    assert (
        subchart.INTRADAY_SUBCHART_VOLUME_COMPARISON
        is _command_layouts.INTRADAY_SUBCHART_VOLUME_COMPARISON
    )
    assert subchart.INTRADAY_SUBCHART_SELECTOR_NAMES is _command_layouts.INTRADAY_SUBCHART_SELECTOR_NAMES
    assert trades.SIDE_MAP is _command_layouts.SIDE_MAP


def test_tdx_provider_wire_market_lookup_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _market\n"
        "print('market=' + _market.normalize_market('SZA'))\n"
        "print('market_id=' + str(_market.market_to_id('BSE')))\n"
        "print('market_module=' + _market.normalize_market.__module__)\n"
        "print('protocol_unit=' + str('axdata_source_tdx._tdx_wire.protocol.unit' in sys.modules))\n"
        "print('protocol_package=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('protocol_commands=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('exceptions=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "market=sz" in result.stdout
    assert "market_id=2" in result.stdout
    assert "market_module=axdata_source_tdx._tdx_wire._market" in result.stdout
    assert "protocol_unit=False" in result.stdout
    assert "protocol_package=False" in result.stdout
    assert "protocol_commands=False" in result.stdout
    assert "exceptions=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_market_errors_load_exceptions_on_demand() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _market\n"
        "print('exceptions_before=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "try:\n"
        "    _market.normalize_market('zz')\n"
        "except Exception as exc:\n"
        "    print('exc_type=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_error=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "ProtocolError = _market.ProtocolError\n"
        "print('compat_error=' + ProtocolError.__module__ + '.' + ProtocolError.__name__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_before=False" in result.stdout
    assert "exc_type=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_error=True" in result.stdout
    assert "compat_error=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout


def test_tdx_provider_wire_binary_lookup_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _binary\n"
        "print('u16=' + str(_binary.little_u16(b'\\x34\\x12')))\n"
        "print('binary_module=' + _binary.little_u16.__module__)\n"
        "print('protocol_unit=' + str('axdata_source_tdx._tdx_wire.protocol.unit' in sys.modules))\n"
        "print('protocol_package=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('protocol_commands=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('exceptions=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "u16=4660" in result.stdout
    assert "binary_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "protocol_unit=False" in result.stdout
    assert "protocol_package=False" in result.stdout
    assert "protocol_commands=False" in result.stdout
    assert "exceptions=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_binary_defers_stdlib_runtime_imports() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _binary\n"
        "def flags(label):\n"
        "    names = ('struct', 'math', 'date', 'datetime')\n"
        "    print(label + '=' + ','.join(name + ':' + str(name in _binary.__dict__) for name in names))\n"
        "flags('initial')\n"
        "print('u16=' + str(_binary.little_u16(b'\\x34\\x12')))\n"
        "flags('after_u16')\n"
        "print('f32=' + str(_binary.little_f32(b'\\x00\\x00\\x80?')))\n"
        "flags('after_f32')\n"
        "print('compact=' + str(round(_binary.decode_compact_float(0x41000000), 6)))\n"
        "flags('after_compact')\n"
        "print('ymd_text=' + str(_binary.yyyymmdd('2026-06-27')))\n"
        "flags('after_ymd_text')\n"
        "print('date_value=' + str(_binary.date_from_yyyymmdd(20260627)))\n"
        "flags('after_date')\n"
        "print('struct_compat=' + _binary.struct.__name__)\n"
        "print('math_compat=' + _binary.math.__name__)\n"
        "print('date_compat=' + _binary.date.__name__)\n"
        "print('datetime_compat=' + _binary.datetime.__name__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "initial=struct:False,math:False,date:False,datetime:False" in result.stdout
    assert "u16=4660" in result.stdout
    assert "after_u16=struct:False,math:False,date:False,datetime:False" in result.stdout
    assert "f32=1.0" in result.stdout
    assert "after_f32=struct:True,math:False,date:False,datetime:False" in result.stdout
    assert "after_compact=struct:True,math:True,date:False,datetime:False" in result.stdout
    assert "ymd_text=20260627" in result.stdout
    assert "after_ymd_text=struct:True,math:True,date:False,datetime:False" in result.stdout
    assert "date_value=2026-06-27" in result.stdout
    assert "after_date=struct:True,math:True,date:True,datetime:True" in result.stdout
    assert "struct_compat=struct" in result.stdout
    assert "math_compat=math" in result.stdout
    assert "date_compat=date" in result.stdout
    assert "datetime_compat=datetime" in result.stdout


def test_tdx_provider_wire_binary_errors_load_exceptions_on_demand() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _binary\n"
        "print('exceptions_before=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "try:\n"
        "    _binary.yyyymmdd('bad-date')\n"
        "except Exception as exc:\n"
        "    print('exc_type=' + exc.__class__.__module__ + '.' + exc.__class__.__name__)\n"
        "print('exceptions_after_error=' + str('axdata_source_tdx._tdx_wire.exceptions' in sys.modules))\n"
        "ProtocolError = _binary.ProtocolError\n"
        "print('compat_error=' + ProtocolError.__module__ + '.' + ProtocolError.__name__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exceptions_before=False" in result.stdout
    assert "exc_type=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout
    assert "exceptions_after_error=True" in result.stdout
    assert "compat_error=axdata_source_tdx._tdx_wire.exceptions.ProtocolError" in result.stdout


def test_tdx_provider_protocol_unit_import_defers_root_helpers() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "unit = importlib.import_module('axdata_source_tdx._tdx_wire.protocol.unit')\n"
        "print('binary_before=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "print('market_before=' + str('axdata_source_tdx._tdx_wire._market' in sys.modules))\n"
        "print('u16=' + str(unit.little_u16(b'\\x34\\x12')))\n"
        "print('binary_after_u16=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "print('market_after_u16=' + str('axdata_source_tdx._tdx_wire._market' in sys.modules))\n"
        "print('market=' + unit.normalize_market('SHA'))\n"
        "print('market_module=' + unit.normalize_market.__module__)\n"
        "print('market_after_market=' + str('axdata_source_tdx._tdx_wire._market' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "binary_before=False" in result.stdout
    assert "market_before=False" in result.stdout
    assert "u16=4660" in result.stdout
    assert "binary_after_u16=True" in result.stdout
    assert "market_after_u16=False" in result.stdout
    assert "market=sh" in result.stdout
    assert "market_module=axdata_source_tdx._tdx_wire._market" in result.stdout
    assert "market_after_market=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_modules_defer_root_binary_helpers() -> None:
    code = (
        "import importlib, sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "modules = {}\n"
        "for module in ['quotes', 'klines', 'intraday', 'trades', 'security', 'session']:\n"
        "    modules[module] = importlib.import_module(f'axdata_source_tdx._tdx_wire.protocol.commands.{module}')\n"
        "print('binary_loaded=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "print('quotes_u16_module=' + modules['quotes'].little_u16.__module__)\n"
        "print('klines_u16_module=' + modules['klines'].little_u16.__module__)\n"
        "print('intraday_u16_module=' + modules['intraday'].little_u16.__module__)\n"
        "print('trades_u16_module=' + modules['trades'].little_u16.__module__)\n"
        "print('security_u16_module=' + modules['security'].little_u16.__module__)\n"
        "print('session_u32_module=' + modules['session'].little_u32.__module__)\n"
        "print('binary_after_compat=' + str('axdata_source_tdx._tdx_wire._binary' in sys.modules))\n"
        "print('unit_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.unit' in sys.modules))\n"
        "print('constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.constants' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "binary_loaded=False" in result.stdout
    assert "quotes_u16_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "klines_u16_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "intraday_u16_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "trades_u16_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "security_u16_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "session_u32_module=axdata_source_tdx._tdx_wire._binary" in result.stdout
    assert "binary_after_compat=True" in result.stdout
    assert "unit_loaded=False" in result.stdout
    assert "constants_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_client_market_normalization_uses_lightweight_helper() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.client import _normalize_market\n"
        "print('market=' + _normalize_market('SHA'))\n"
        "print('market_helper=' + str('axdata_source_tdx._tdx_wire._market' in sys.modules))\n"
        "print('protocol_unit=' + str('axdata_source_tdx._tdx_wire.protocol.unit' in sys.modules))\n"
        "print('protocol_package=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "market=sh" in result.stdout
    assert "market_helper=True" in result.stdout
    assert "protocol_unit=False" in result.stdout
    assert "protocol_package=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_registry_matches_protocol_constants() -> None:
    from axdata_source_tdx._tdx_wire.protocol import constants
    from axdata_source_tdx._tdx_wire.protocol.commands.registry import COMMANDS

    for name, spec in COMMANDS.items():
        assert spec.name == name
        assert spec.code == getattr(constants, f"TYPE_{name.upper()}")


def test_tdx_provider_wire_command_registry_uses_shared_command_codes() -> None:
    from axdata_source_tdx._tdx_wire import _command_codes, _command_registry
    from axdata_source_tdx._tdx_wire.protocol.commands.registry import COMMANDS

    assert COMMANDS == _command_registry.COMMANDS
    assert tuple(COMMANDS) == tuple(name for name, _ in _command_codes.COMMAND_CODE_ITEMS)
    for name, spec in COMMANDS.items():
        assert spec.code == _command_codes.command_code(name)


def test_tdx_provider_wire_command_registry_uses_shared_metadata() -> None:
    from axdata_source_tdx._tdx_wire import _command_metadata, _command_registry
    from axdata_source_tdx._tdx_wire.protocol.commands.registry import COMMANDS, CommandSpec, required_commands

    assert tuple(COMMANDS) == tuple(name for name, *_ in _command_metadata.COMMAND_METADATA_ITEMS)
    assert CommandSpec is _command_registry.CommandSpec
    assert required_commands is _command_registry.required_commands
    for name, module, method, required_for_1_0, document in _command_metadata.COMMAND_METADATA_ITEMS:
        spec = COMMANDS[name]
        assert (spec.module, spec.method, spec.required_for_1_0, spec.document) == (
            module,
            method,
            required_for_1_0,
            document,
        )


def test_tdx_provider_wire_command_registry_root_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _command_registry as registry\n"
        "print('has_commands_before=' + str('COMMANDS' in registry.__dict__))\n"
        "print('metadata_before=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('command_codes_before=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('command_code=' + hex(registry.command_code('legacy_quotes')))\n"
        "print('has_commands_after_lookup=' + str('COMMANDS' in registry.__dict__))\n"
        "print('metadata_after_lookup=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('command_codes_after_lookup=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('root_lookup_after_lookup=' + str('axdata_source_tdx._tdx_wire._command_lookup' in sys.modules))\n"
        "codes_module = sys.modules.get('axdata_source_tdx._tdx_wire._command_codes')\n"
        "print('command_codes_export_cached_after_lookup=' + str(codes_module is not None and 'COMMAND_CODES' in codes_module.__dict__))\n"
        "required_before_commands = registry.required_commands()\n"
        "print('required_before_commands=' + ','.join(spec.name for spec in required_before_commands))\n"
        "print('has_commands_after_required=' + str('COMMANDS' in registry.__dict__))\n"
        "print('root_lookup_after_required=' + str('axdata_source_tdx._tdx_wire._command_lookup' in sys.modules))\n"
        "print('command_count=' + str(len(registry.COMMANDS)))\n"
        "codes_module = sys.modules['axdata_source_tdx._tdx_wire._command_codes']\n"
        "print('command_codes_export_cached_after_commands=' + str('COMMAND_CODES' in codes_module.__dict__))\n"
        "print('explicit_legacy_quotes=' + hex(codes_module.COMMAND_CODES['legacy_quotes']))\n"
        "print('command_codes_export_cached_after_explicit=' + str('COMMAND_CODES' in codes_module.__dict__))\n"
        "print('required=' + ','.join(spec.name for spec in registry.required_commands()))\n"
        "print('spec_module=' + registry.CommandSpec.__module__)\n"
        "print('registry_root=' + str('axdata_source_tdx._tdx_wire._command_registry' in sys.modules))\n"
        "print('legacy_registry=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('protocol_package=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('codec_loaded=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('metadata_loaded=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('command_codes_loaded=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "has_commands_before=False" in result.stdout
    assert "metadata_before=False" in result.stdout
    assert "command_codes_before=False" in result.stdout
    assert "command_code=0x53e" in result.stdout
    assert "has_commands_after_lookup=False" in result.stdout
    assert "metadata_after_lookup=False" in result.stdout
    assert "command_codes_after_lookup=True" in result.stdout
    assert "root_lookup_after_lookup=False" in result.stdout
    assert "command_codes_export_cached_after_lookup=False" in result.stdout
    assert "required_before_commands=heartbeat,handshake,security_list,security_count" in result.stdout
    assert "has_commands_after_required=False" in result.stdout
    assert "root_lookup_after_required=False" in result.stdout
    assert "command_count=20" in result.stdout
    assert "command_codes_export_cached_after_commands=False" in result.stdout
    assert "explicit_legacy_quotes=0x53e" in result.stdout
    assert "command_codes_export_cached_after_explicit=True" in result.stdout
    assert "required=heartbeat,handshake,security_list,security_count" in result.stdout
    assert "spec_module=axdata_source_tdx._tdx_wire._command_registry" in result.stdout
    assert "registry_root=True" in result.stdout
    assert "legacy_registry=False" in result.stdout
    assert "commands_package=False" in result.stdout
    assert "protocol_package=False" in result.stdout
    assert "codec_loaded=False" in result.stdout
    assert "metadata_loaded=True" in result.stdout
    assert "command_codes_loaded=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_registry_compat_import_is_lazy() -> None:
    code = (
        "import importlib, sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "registry = importlib.import_module('axdata_source_tdx._tdx_wire.protocol.commands.registry')\n"
        "print('legacy_registry=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('root_registry_before=' + str('axdata_source_tdx._tdx_wire._command_registry' in sys.modules))\n"
        "print('command_codes_before=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('metadata_before=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('codec_before=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('cached_command_code_before=' + str('command_code' in registry.__dict__))\n"
        "command_code = registry.command_code\n"
        "print('command_code_module=' + command_code.__module__)\n"
        "print('root_registry_after_attr=' + str('axdata_source_tdx._tdx_wire._command_registry' in sys.modules))\n"
        "print('command_codes_after_attr=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('metadata_after_attr=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('cached_command_code_after=' + str('command_code' in registry.__dict__))\n"
        "print('command_code=' + hex(command_code('legacy_quotes')))\n"
        "print('command_codes_after_call=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('metadata_after_call=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('command_count=' + str(len(registry.COMMANDS)))\n"
        "print('metadata_after_commands=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('codec_after_commands=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "legacy_registry=True" in result.stdout
    assert "root_registry_before=False" in result.stdout
    assert "command_codes_before=False" in result.stdout
    assert "metadata_before=False" in result.stdout
    assert "codec_before=False" in result.stdout
    assert "cached_command_code_before=False" in result.stdout
    assert "command_code_module=axdata_source_tdx._tdx_wire._command_registry" in result.stdout
    assert "root_registry_after_attr=True" in result.stdout
    assert "command_codes_after_attr=False" in result.stdout
    assert "metadata_after_attr=False" in result.stdout
    assert "cached_command_code_after=True" in result.stdout
    assert "command_code=0x53e" in result.stdout
    assert "command_codes_after_call=True" in result.stdout
    assert "metadata_after_call=False" in result.stdout
    assert "command_count=20" in result.stdout
    assert "metadata_after_commands=True" in result.stdout
    assert "codec_after_commands=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_aggregates_use_root_registry() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol import COMMANDS as protocol_commands\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import COMMANDS as commands_commands\n"
        "print('same=' + str(protocol_commands is commands_commands))\n"
        "print('command_count=' + str(len(protocol_commands)))\n"
        "print('registry_root=' + str('axdata_source_tdx._tdx_wire._command_registry' in sys.modules))\n"
        "print('legacy_registry=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('codec_loaded=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "codes_module = sys.modules.get('axdata_source_tdx._tdx_wire._command_codes')\n"
        "print('command_codes_export_cached=' + str(codes_module is not None and 'COMMAND_CODES' in codes_module.__dict__))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "same=True" in result.stdout
    assert "command_count=20" in result.stdout
    assert "registry_root=True" in result.stdout
    assert "legacy_registry=False" in result.stdout
    assert "commands_package=True" in result.stdout
    assert "codec_loaded=False" in result.stdout
    assert "command_codes_export_cached=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_code_lookup_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx._tdx_wire._command_lookup as lookup\n"
        "command_code = lookup.command_code\n"
        "print('lookup_module=' + command_code.__module__)\n"
        "print('command_codes_before_call=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('command_code=' + hex(command_code('legacy_quotes')))\n"
        "print('command_codes_after_call=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('protocol_package=' + str('axdata_source_tdx._tdx_wire.protocol' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('metadata_loaded=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('codec_loaded=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "command_code=0x53e" in result.stdout
    assert "lookup_module=axdata_source_tdx._tdx_wire._command_lookup" in result.stdout
    assert "command_codes_before_call=False" in result.stdout
    assert "command_codes_after_call=True" in result.stdout
    assert "commands_package=False" in result.stdout
    assert "protocol_package=False" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "metadata_loaded=False" in result.stdout
    assert "codec_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_lookup_compat_path_still_works() -> None:
    code = (
        "import importlib, sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "lookup = importlib.import_module('axdata_source_tdx._tdx_wire.protocol.commands.lookup')\n"
        "print('lookup_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.lookup' in sys.modules))\n"
        "print('root_lookup_before_attr=' + str('axdata_source_tdx._tdx_wire._command_lookup' in sys.modules))\n"
        "print('command_codes_before_attr=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('cached_command_code_before=' + str('command_code' in lookup.__dict__))\n"
        "command_code = lookup.command_code\n"
        "print('lookup_module=' + command_code.__module__)\n"
        "print('root_lookup_after_attr=' + str('axdata_source_tdx._tdx_wire._command_lookup' in sys.modules))\n"
        "print('cached_command_code_after=' + str('command_code' in lookup.__dict__))\n"
        "print('command_codes_before_call=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('command_code=' + hex(command_code('legacy_quotes')))\n"
        "print('command_codes_after_call=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('metadata_loaded=' + str('axdata_source_tdx._tdx_wire._command_metadata' in sys.modules))\n"
        "print('codec_loaded=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "command_code=0x53e" in result.stdout
    assert "lookup_module=axdata_source_tdx._tdx_wire._command_lookup" in result.stdout
    assert "lookup_loaded=True" in result.stdout
    assert "root_lookup_before_attr=False" in result.stdout
    assert "command_codes_before_attr=False" in result.stdout
    assert "cached_command_code_before=False" in result.stdout
    assert "root_lookup_after_attr=True" in result.stdout
    assert "cached_command_code_after=True" in result.stdout
    assert "command_codes_before_call=False" in result.stdout
    assert "command_codes_after_call=True" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "metadata_loaded=False" in result.stdout
    assert "codec_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_lookup_command_codes_export_is_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx._tdx_wire._command_lookup as lookup\n"
        "print('command_codes_before_export=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "from axdata_source_tdx._tdx_wire._command_lookup import COMMAND_CODES\n"
        "print('legacy_quotes=' + hex(COMMAND_CODES['legacy_quotes']))\n"
        "print('same_object=' + str(COMMAND_CODES is lookup.COMMAND_CODES))\n"
        "print('command_codes_after_export=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('codec_loaded=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "command_codes_before_export=False" in result.stdout
    assert "legacy_quotes=0x53e" in result.stdout
    assert "same_object=True" in result.stdout
    assert "command_codes_after_export=True" in result.stdout
    assert "commands_package=False" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "codec_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_command_dispatch_matches_registry() -> None:
    from axdata_source_tdx._tdx_wire import _command_dispatch
    from axdata_source_tdx._tdx_wire.protocol.commands.registry import COMMANDS

    assert tuple(name for name, _ in _command_dispatch.BUILDER_TARGET_ITEMS) == tuple(COMMANDS)
    assert tuple(name for name, _ in _command_dispatch.PARSER_TARGET_ITEMS) == tuple(COMMANDS)
    for name in COMMANDS:
        assert name in _command_dispatch.BUILDER_TARGETS
        assert name in _command_dispatch.PARSER_TARGETS


def test_tdx_provider_wire_protocol_constants_keep_legacy_values() -> None:
    from axdata_source_tdx._tdx_wire.protocol import constants

    assert constants.PREFIX == 0x0C
    assert constants.PREFIX_RESP == b"\xB1\xCB\x74\x00"
    assert constants.CONTROL_DEFAULT == 0x01
    assert constants.TYPE_HANDSHAKE == 0x000D
    assert constants.TYPE_HEARTBEAT == 0x0004
    assert constants.TYPE_LEGACY_QUOTES == 0x053E
    assert constants.TYPE_FILE_CONTENT == 0x06B9
    assert constants.TYPE_RECENT_HISTORICAL_INTRADAY == 0x0FEB
    assert constants.DEFAULT_CODE_PAGE_SIZE == 1600
    assert constants.DEFAULT_QUOTE_BATCH_SIZE == 80


def test_tdx_provider_wire_protocol_constants_share_frame_constants() -> None:
    from axdata_source_tdx._tdx_wire.protocol import _frame_constants, constants

    assert constants.PREFIX is _frame_constants.PREFIX
    assert constants.PREFIX_RESP is _frame_constants.PREFIX_RESP
    assert constants.CONTROL_DEFAULT is _frame_constants.CONTROL_DEFAULT


def test_tdx_provider_wire_socket_uses_shared_session_command_codes() -> None:
    from axdata_source_tdx._tdx_wire import _command_codes
    from axdata_source_tdx._tdx_wire.transport import socket

    assert socket.TYPE_HEARTBEAT is _command_codes.TYPE_HEARTBEAT
    assert socket.TYPE_HANDSHAKE is _command_codes.TYPE_HANDSHAKE


def test_tdx_provider_wire_protocol_constants_use_lightweight_command_codes() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.protocol import constants\n"
        "print('legacy_quotes=' + hex(constants.TYPE_LEGACY_QUOTES))\n"
        "print('command_codes_loaded=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('registry_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.registry' in sys.modules))\n"
        "print('commands_package=' + str('axdata_source_tdx._tdx_wire.protocol.commands' in sys.modules))\n"
        "print('codec_loaded=' + str('axdata_source_tdx._tdx_wire._command_codec' in sys.modules))\n"
        "print('quotes_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.quotes' in sys.modules))\n"
        "print('session_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.commands.session' in sys.modules))\n"
        "print('frame_loaded=' + str('axdata_source_tdx._tdx_wire.protocol.frame' in sys.modules))\n"
        "print('frame_constants_loaded=' + str('axdata_source_tdx._tdx_wire.protocol._frame_constants' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "legacy_quotes=0x53e" in result.stdout
    assert "command_codes_loaded=True" in result.stdout
    assert "registry_loaded=False" in result.stdout
    assert "commands_package=False" in result.stdout
    assert "codec_loaded=False" in result.stdout
    assert "quotes_loaded=False" in result.stdout
    assert "session_loaded=False" in result.stdout
    assert "frame_loaded=False" in result.stdout
    assert "frame_constants_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_protocol_constants_import_defers_fact_modules() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "constants = importlib.import_module('axdata_source_tdx._tdx_wire.protocol.constants')\n"
        "print('codes_before=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('defaults_before=' + str('axdata_source_tdx._tdx_wire._request_defaults' in sys.modules))\n"
        "print('frame_constants_before=' + str('axdata_source_tdx._tdx_wire.protocol._frame_constants' in sys.modules))\n"
        "print('prefix=' + hex(constants.PREFIX))\n"
        "print('frame_constants_after_prefix=' + str('axdata_source_tdx._tdx_wire.protocol._frame_constants' in sys.modules))\n"
        "print('codes_after_prefix=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('quote_batch=' + str(constants.DEFAULT_QUOTE_BATCH_SIZE))\n"
        "print('defaults_after_default=' + str('axdata_source_tdx._tdx_wire._request_defaults' in sys.modules))\n"
        "print('codes_after_default=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('legacy_quotes=' + hex(constants.TYPE_LEGACY_QUOTES))\n"
        "print('codes_after_type=' + str('axdata_source_tdx._tdx_wire._command_codes' in sys.modules))\n"
        "print('command_codes_cached_before=' + str('COMMAND_CODES' in constants.__dict__))\n"
        "print('explicit_legacy=' + hex(constants.COMMAND_CODES['legacy_quotes']))\n"
        "print('command_codes_cached_after=' + str('COMMAND_CODES' in constants.__dict__))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "codes_before=False" in result.stdout
    assert "defaults_before=False" in result.stdout
    assert "frame_constants_before=False" in result.stdout
    assert "prefix=0xc" in result.stdout
    assert "frame_constants_after_prefix=True" in result.stdout
    assert "codes_after_prefix=False" in result.stdout
    assert "quote_batch=80" in result.stdout
    assert "defaults_after_default=True" in result.stdout
    assert "codes_after_default=False" in result.stdout
    assert "legacy_quotes=0x53e" in result.stdout
    assert "codes_after_type=True" in result.stdout
    assert "command_codes_cached_before=False" in result.stdout
    assert "explicit_legacy=0x53e" in result.stdout
    assert "command_codes_cached_after=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_hosts_are_provider_owned() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import wire\n"
        "default_hosts = wire.default_tdx_hosts()\n"
        "fallback_hosts = wire.fallback_tdx_hosts()\n"
        "print('default_count=' + str(len(default_hosts)))\n"
        "print('fallback_count=' + str(len(fallback_hosts)))\n"
        "print('first_default=' + default_hosts[0])\n"
        "print('first_fallback=' + fallback_hosts[0])\n"
        "print('tdx_wire_hosts=' + str('axdata_core._tdx_wire.hosts' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "default_count=43" in result.stdout
    assert "fallback_count=43" in result.stdout
    assert "first_default=116.205.183.150:7709" in result.stdout
    assert "first_fallback=116.205.183.150:7709" in result.stdout
    assert "tdx_wire_hosts=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_tdx_provider_wire_seam_uses_shared_host_sources() -> None:
    from axdata_source_tdx import wire
    from axdata_source_tdx._tdx_wire import _host_defaults, _host_resource

    assert wire.default_tdx_hosts() == list(_host_resource.DEFAULT_HOSTS)
    assert wire.fallback_tdx_hosts() == list(_host_defaults.DEFAULT_QUOTE_HOSTS)

    text = (TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx" / "wire.py").read_text(encoding="utf-8")
    assert "FALLBACK_QUOTE_HOSTS" not in text
    assert "_quote_hosts_from_resource" not in text
    assert "json.loads" not in text
    assert "resources.files" not in text


def test_tdx_provider_wire_host_defaults_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _host_defaults as defaults\n"
        "print('count=' + str(len(defaults.DEFAULT_QUOTE_HOSTS)))\n"
        "print('first=' + defaults.DEFAULT_QUOTE_HOSTS[0])\n"
        "print('client_loaded=' + str('axdata_source_tdx._tdx_wire.client' in sys.modules))\n"
        "print('hosts_loaded=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('host_resource=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('transport_loaded=' + str('axdata_source_tdx._tdx_wire.transport' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "count=43" in result.stdout
    assert "first=116.205.183.150:7709" in result.stdout
    assert "client_loaded=False" in result.stdout
    assert "hosts_loaded=False" in result.stdout
    assert "host_resource=False" in result.stdout
    assert "transport_loaded=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_stack_is_provider_owned() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx.wire import tdx_client_class\n"
        "from axdata_source_tdx._tdx_wire.protocol.commands import build_command_frame\n"
        "from axdata_source_tdx._tdx_wire.transport.socket import SocketTransport\n"
        "client_class = tdx_client_class()\n"
        "print('client_module=' + client_class.__module__)\n"
        "print('command_module=' + build_command_frame.__module__)\n"
        "print('transport_module=' + SocketTransport.__module__)\n"
        "print('core_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('core_wire_socket=' + str('axdata_core._tdx_wire.transport.socket' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "client_module=axdata_source_tdx._tdx_wire.client" in result.stdout
    assert "command_module=axdata_source_tdx._tdx_wire._command_codec" in result.stdout
    assert "transport_module=axdata_source_tdx._tdx_wire.transport.socket" in result.stdout
    assert "core_wire_client=False" in result.stdout
    assert "core_wire_socket=False" in result.stdout


def test_tdx_provider_options_and_price_limits_import_are_lightweight() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.options',\n"
        "    'axdata_source_tdx.price_limits',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.client_factory',\n"
        "    'axdata_core.adapters.tdx.host_config',\n"
        "    'axdata_core.tdx_server_config',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_options=' + str('axdata_source_tdx.options' in sys.modules))\n"
        "print('provider_price_limits=' + str('axdata_source_tdx.price_limits' in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_options=True" in result.stdout
    assert "provider_price_limits=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_price_limit_calendar_and_history_import_are_lightweight() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.price_limit_calendar',\n"
        "    'axdata_source_tdx.price_limit_history',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.client_factory',\n"
        "    'axdata_core.adapters.tdx.host_config',\n"
        "    'axdata_core.tdx_server_config',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_price_limit_calendar=' + str('axdata_source_tdx.price_limit_calendar' in sys.modules))\n"
        "print('provider_price_limit_history=' + str('axdata_source_tdx.price_limit_history' in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_price_limit_calendar=True" in result.stdout
    assert "provider_price_limit_history=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_adjustment_derived_interface_sets_import_are_lightweight() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        "from types import SimpleNamespace\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.adjustment',\n"
        "    'axdata_source_tdx.derived_rows',\n"
        "    'axdata_source_tdx.interface_sets',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "from axdata_source_tdx import adjustment, derived_rows, interface_sets\n"
        "xdxr = SimpleNamespace(c1_float=1.0, c2_float=0.0, c3_float=1.0, c4_float=0.0)\n"
        "adjusted = adjustment.apply_xdxr_to_last_close(10000, xdxr)\n"
        "share_source = derived_rows.daily_share_source(1, None, 3)\n"
        "adj_row = derived_rows.normalize_adj_factor_row('sz000001', '20260626', 1.23456789123)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.adjustment',\n"
        "    'axdata_core.adapters.tdx.derived_rows',\n"
        "    'axdata_core.adapters.tdx.interface_sets',\n"
        "    'axdata_core.adapters.tdx.codes',\n"
        "    'axdata_core.adapters.tdx.price_limits',\n"
        "    'axdata_core.adapters.tdx.snapshot_normalize',\n"
        "    'axdata_core.tdx_f10_names',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_adjustment=' + str('axdata_source_tdx.adjustment' in sys.modules))\n"
        "print('provider_derived_rows=' + str('axdata_source_tdx.derived_rows' in sys.modules))\n"
        "print('provider_interface_sets=' + str('axdata_source_tdx.interface_sets' in sys.modules))\n"
        "print('provider_codes=' + str('axdata_source_tdx.codes' in sys.modules))\n"
        "print('provider_price_limits=' + str('axdata_source_tdx.price_limits' in sys.modules))\n"
        "print('provider_snapshot=' + str('axdata_source_tdx.snapshot_normalize' in sys.modules))\n"
        "print('adjustment_module=' + adjustment.apply_xdxr_to_last_close.__module__)\n"
        "print('derived_module=' + derived_rows.normalize_adj_factor_row.__module__)\n"
        "print('interface_count=' + str(len(interface_sets.SUPPORTED_INTERFACES)))\n"
        "print('adjusted=' + str(adjusted))\n"
        "print('share_source=' + share_source)\n"
        "print('adj_factor=' + str(adj_row['adj_factor']))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_adjustment=True" in result.stdout
    assert "provider_derived_rows=True" in result.stdout
    assert "provider_interface_sets=True" in result.stdout
    assert "provider_codes=True" in result.stdout
    assert "provider_price_limits=True" in result.stdout
    assert "provider_snapshot=True" in result.stdout
    assert "adjustment_module=axdata_source_tdx.adjustment" in result.stdout
    assert "derived_module=axdata_source_tdx.derived_rows" in result.stdout
    assert "interface_count=" in result.stdout
    assert "adjusted=9000" in result.stdout
    assert "share_source=finance_snapshot+tdxstat" in result.stdout
    assert "adj_factor=1.2345678912" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_f10_helpers_import_without_core_runtime() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.f10_normalize',\n"
        "    'axdata_source_tdx.f10_params',\n"
        "    'axdata_source_tdx.f10_postprocess',\n"
        "    'axdata_source_tdx.f10_render',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.client_factory',\n"
        "    'axdata_core.adapters.tdx.provider_bridge',\n"
        "    'axdata_core.tdx_f10_models',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_f10_normalize=' + str('axdata_source_tdx.f10_normalize' in sys.modules))\n"
        "print('provider_f10_params=' + str('axdata_source_tdx.f10_params' in sys.modules))\n"
        "print('provider_f10_postprocess=' + str('axdata_source_tdx.f10_postprocess' in sys.modules))\n"
        "print('provider_f10_render=' + str('axdata_source_tdx.f10_render' in sys.modules))\n"
        "print('provider_f10_models=' + str('axdata_source_tdx.tdx_f10_models' in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_f10_normalize=True" in result.stdout
    assert "provider_f10_params=True" in result.stdout
    assert "provider_f10_postprocess=True" in result.stdout
    assert "provider_f10_render=True" in result.stdout
    assert "provider_f10_models=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_f10_executor_and_limit_ladder_topics_import_are_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import f10_executor, limit_ladder_topics\n"
        "dispatch = f10_executor.request_f10_dispatch_result(\n"
        "    {'code': ['000001.SZ', '600000.SH']},\n"
        "    interface_name='stock_topic_exposure_tdx',\n"
        "    spec=type('Spec', (), {'category': 'topic', 'evaluation': 'stable'})(),\n"
        "    requested_codes=['000001.SZ', '600000.SH'],\n"
        "    configured_worker_count=2,\n"
        "    request_one=lambda params: [{'code': params['code']}],\n"
        ")\n"
        "topic_rows = limit_ladder_topics.limit_ladder_theme_rank_rows(\n"
        "    [\n"
        "        {'instrument_id': '000001.SZ', 'name': '一号', 'limit_status': 'sealed', 'ladder_level': 2, 'seal_amount': 10, 'amount': 20},\n"
        "        {'instrument_id': '600000.SH', 'name': '二号', 'limit_status': 'sealed', 'ladder_level': 1, 'seal_amount': 5, 'amount': 10},\n"
        "    ],\n"
        "    {\n"
        "        '000001.SZ': [{'topic_name': 'AI', 'topic_id': 't1'}],\n"
        "        '600000.SH': [{'topic_name': 'AI', 'topic_id': 't1'}],\n"
        "    },\n"
        "    topic_type='theme',\n"
        ")\n"
        "client_class = f10_executor.TdxTqlexClient\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.f10_executor',\n"
        "    'axdata_core.adapters.tdx.limit_ladder_topics',\n"
        "    'axdata_core.adapters.tdx.tqlex',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_f10_executor=' + str('axdata_source_tdx.f10_executor' in sys.modules))\n"
        "print('provider_limit_ladder_topics=' + str('axdata_source_tdx.limit_ladder_topics' in sys.modules))\n"
        "print('provider_tqlex=' + str('axdata_source_tdx.tqlex' in sys.modules))\n"
        "print('executor_module=' + f10_executor.request_f10_dispatch_result.__module__)\n"
        "print('topic_module=' + limit_ladder_topics.limit_ladder_theme_rank_rows.__module__)\n"
        "print('client_module=' + client_class.__module__)\n"
        "print('dispatch_count=' + str(len(dispatch.rows)))\n"
        "print('dispatch_workers=' + str(dispatch.worker_count))\n"
        "print('topic_count=' + str(topic_rows[0]['limit_up_count']))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_f10_executor=True" in result.stdout
    assert "provider_limit_ladder_topics=True" in result.stdout
    assert "provider_tqlex=True" in result.stdout
    assert "executor_module=axdata_source_tdx.f10_executor" in result.stdout
    assert "topic_module=axdata_source_tdx.limit_ladder_topics" in result.stdout
    assert "client_module=axdata_source_tdx.tqlex" in result.stdout
    assert "dispatch_count=2" in result.stdout
    assert "dispatch_workers=2" in result.stdout
    assert "topic_count=2" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_tqlex_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import tqlex\n"
        "tables = tqlex.parse_tqlex_tables({\n"
        "    'ResultSets': [\n"
        "        {'ColName': ['A', 'A', ''], 'Content': [[1, 2, 3], {'B': 4}]},\n"
        "    ]\n"
        "})\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_tqlex=' + str('axdata_source_tdx.tqlex' in sys.modules))\n"
        "print('table_module=' + tables[0].__class__.__module__)\n"
        "print('columns=' + repr(tables[0].columns))\n"
        "print('rows=' + repr(tables[0].rows))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_tqlex=True" in result.stdout
    assert "table_module=axdata_source_tdx.tqlex" in result.stdout
    assert "columns=('A', 'A__1', 'column_2')" in result.stdout
    assert "rows=({'A': 1, 'A__1': 2, 'column_2': 3}, {'B': 4})" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_finance_maps_load_builtin_resources_without_core_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import finance_maps\n"
        "maps = finance_maps.load_finance_local_maps()\n"
        "profile = finance_maps.lookup_finance_profile_maps(\n"
        "    '000001', market_id=0, province_raw=18, local_maps=maps\n"
        ")\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.finance_maps',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_finance_maps=' + str('axdata_source_tdx.finance_maps' in sys.modules))\n"
        "print('map_module=' + maps.__class__.__module__)\n"
        "print('loaded_flag=' + str(maps.loaded))\n"
        "print('root=' + str(maps.root))\n"
        "print('province=' + str(profile['province_name']))\n"
        "print('industry=' + str(profile['tdx_industry_name']))\n"
        "print('research=' + str(profile['tdx_research_industry_name']))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_finance_maps=True" in result.stdout
    assert "map_module=axdata_source_tdx.finance_maps" in result.stdout
    assert "loaded_flag=True" in result.stdout
    assert "root=builtin" in result.stdout
    assert "province=深圳" in result.stdout
    assert "industry=银行" in result.stdout
    assert "research=股份制银行" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_finance_normalize_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import finance_normalize\n"
        "row = finance_normalize.normalize_finance_info_row({\n"
        "    'full_code': 'sz000001',\n"
        "    'updated_date': '20260425',\n"
        "    'total_share': 19405.92,\n"
        "})\n"
        "categories = finance_normalize.requested_capital_change_categories('xdxr,5')\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.finance_normalize',\n"
        "    'axdata_core.adapters.tdx.finance_maps',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_finance_normalize=' + str('axdata_source_tdx.finance_normalize' in sys.modules))\n"
        "print('row_module=' + finance_normalize.normalize_finance_info_row.__module__)\n"
        "print('instrument_id=' + str(row['instrument_id']))\n"
        "print('total_share=' + str(row['total_share']))\n"
        "print('categories=' + repr(categories))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_finance_normalize=True" in result.stdout
    assert "row_module=axdata_source_tdx.finance_normalize" in result.stdout
    assert "instrument_id=000001.SZ" in result.stdout
    assert "total_share=19405.92" in result.stdout
    assert "categories={1, 5}" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_quote_snapshot_time_series_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import quote_identity, snapshot_normalize, time_series_normalize\n"
        "market, symbol = quote_identity.quote_security_from_tdx_code('sh600000')\n"
        "snapshot = snapshot_normalize.normalize_realtime_snapshot_row({\n"
        "    'full_code': 'sz000001',\n"
        "    'last_price': 10.55,\n"
        "    'pre_close': 10.0,\n"
        "    'open': 10.2,\n"
        "    'high': 10.8,\n"
        "    'low': 10.1,\n"
        "    'total_hand': 123,\n"
        "    'current_hand': 4,\n"
        "    'amount': 123456.0,\n"
        "})\n"
        "kline = time_series_normalize.normalize_kline_row(\n"
        "    {'full_code': 'sh600000', 'code': '600000', 'period_name': 'day'},\n"
        "    {'time': '2026-06-26 15:00:00', 'open': 10.0, 'high': 10.5, 'low': 9.9, 'close': 10.2},\n"
        ")\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.quote_identity',\n"
        "    'axdata_core.adapters.tdx.snapshot_normalize',\n"
        "    'axdata_core.adapters.tdx.time_series_normalize',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_quote_identity=' + str('axdata_source_tdx.quote_identity' in sys.modules))\n"
        "print('provider_snapshot_normalize=' + str('axdata_source_tdx.snapshot_normalize' in sys.modules))\n"
        "print('provider_time_series_normalize=' + str('axdata_source_tdx.time_series_normalize' in sys.modules))\n"
        "print('quote_module=' + quote_identity.quote_security_from_tdx_code.__module__)\n"
        "print('snapshot_module=' + snapshot_normalize.normalize_realtime_snapshot_row.__module__)\n"
        "print('time_series_module=' + time_series_normalize.normalize_kline_row.__module__)\n"
        "print('market_symbol=' + repr((market, symbol)))\n"
        "print('snapshot_id=' + str(snapshot['instrument_id']))\n"
        "print('kline_id=' + str(kline['instrument_id']))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_quote_identity=True" in result.stdout
    assert "provider_snapshot_normalize=True" in result.stdout
    assert "provider_time_series_normalize=True" in result.stdout
    assert "quote_module=axdata_source_tdx.quote_identity" in result.stdout
    assert "snapshot_module=axdata_source_tdx.snapshot_normalize" in result.stdout
    assert "time_series_module=axdata_source_tdx.time_series_normalize" in result.stdout
    assert "market_symbol=('sh', '600000')" in result.stdout
    assert "snapshot_id=000001.SZ" in result.stdout
    assert "kline_id=600000.SH" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_quote_fetch_and_kline_helpers_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import kline_helpers, quote_fetch\n"
        "quote_result = quote_fetch.QuoteRowsResult(rows=[], quote_count=2)\n"
        "quote_meta = quote_fetch.quote_rows_meta(\n"
        "    quote_result,\n"
        "    protocol='0x0547',\n"
        "    requested_code_count=3,\n"
        ")\n"
        "kline_meta = kline_helpers.kline_meta(\n"
        "    {'period_raw': 9, 'adjust_mode': 'none'},\n"
        "    page_size=800,\n"
        "    page_count=2,\n"
        "    requested_code_count=1,\n"
        "    concurrency_limit=1,\n"
        "    concurrency_capacity=1,\n"
        "    get_value=lambda obj, key, default=None: obj.get(key, default),\n"
        ")\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.quote_fetch',\n"
        "    'axdata_core.adapters.tdx.kline_helpers',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_quote_fetch=' + str('axdata_source_tdx.quote_fetch' in sys.modules))\n"
        "print('provider_kline_helpers=' + str('axdata_source_tdx.kline_helpers' in sys.modules))\n"
        "print('quote_module=' + quote_fetch.quote_rows_meta.__module__)\n"
        "print('kline_module=' + kline_helpers.kline_meta.__module__)\n"
        "print('quote_count=' + str(quote_meta['tdx_quote_count']))\n"
        "print('kline_protocol=' + str(kline_meta['tdx_protocol']))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_quote_fetch=True" in result.stdout
    assert "provider_kline_helpers=True" in result.stdout
    assert "quote_module=axdata_source_tdx.quote_fetch" in result.stdout
    assert "kline_module=axdata_source_tdx.kline_helpers" in result.stdout
    assert "quote_count=2" in result.stdout
    assert "kline_protocol=0x052d" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_intraday_and_finance_fetch_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        "import types\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import finance_fetch, intraday_fetch\n"
        "point_result = intraday_fetch.PointRowsResult(rows=[], point_count=6)\n"
        "point_meta = intraday_fetch.point_rows_meta(\n"
        "    point_result,\n"
        "    protocol='0x0537',\n"
        "    requested_code_count=2,\n"
        ")\n"
        "capital_result = finance_fetch.CapitalChangeResult(\n"
        "    rows=[],\n"
        "    event_count=3,\n"
        "    returned_event_count=1,\n"
        "    concurrency=2,\n"
        ")\n"
        "capital_meta = finance_fetch.capital_change_meta(\n"
        "    capital_result,\n"
        "    ['sz000001'],\n"
        "    expanded_scope=None,\n"
        "    requested_categories={1},\n"
        ")\n"
        "fake_stats = types.ModuleType('axdata_source_tdx.stats_resource')\n"
        "fake_stats.ensure_tdx_stats_resource_for_params = lambda *args, **kwargs: ('provider-stats', True)\n"
        "sys.modules['axdata_source_tdx.stats_resource'] = fake_stats\n"
        "stats_resource, stats_refreshed = finance_fetch._ensure_tdx_stats_resource_for_params(None, {})\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.finance_fetch',\n"
        "    'axdata_core.adapters.tdx.intraday_fetch',\n"
        "    'axdata_core.adapters.tdx.stats_resource',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_finance_fetch=' + str('axdata_source_tdx.finance_fetch' in sys.modules))\n"
        "print('provider_intraday_fetch=' + str('axdata_source_tdx.intraday_fetch' in sys.modules))\n"
        "print('finance_module=' + finance_fetch.capital_change_meta.__module__)\n"
        "print('intraday_module=' + intraday_fetch.point_rows_meta.__module__)\n"
        "print('point_count=' + str(point_meta['tdx_point_count']))\n"
        "print('capital_events=' + str(capital_meta['tdx_event_count']))\n"
        "print('stats_resource=' + str(stats_resource))\n"
        "print('stats_refreshed=' + str(stats_refreshed))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_finance_fetch=True" in result.stdout
    assert "provider_intraday_fetch=True" in result.stdout
    assert "finance_module=axdata_source_tdx.finance_fetch" in result.stdout
    assert "intraday_module=axdata_source_tdx.intraday_fetch" in result.stdout
    assert "point_count=6" in result.stdout
    assert "capital_events=3" in result.stdout
    assert "stats_resource=provider-stats" in result.stdout
    assert "stats_refreshed=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_auction_price_limit_ladder_fetch_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        "import types\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import auction_fetch, limit_ladder_fetch, price_limit_fetch\n"
        "auction_result = auction_fetch.AuctionIndicatorRowsResult(\n"
        "    rows=[],\n"
        "    quote_count=1,\n"
        "    kline_page_count=2,\n"
        "    kline_volume_days=3,\n"
        "    target_trade_date='20260626',\n"
        "    previous_trade_date='20260625',\n"
        "    market_date_source='test',\n"
        "    alignment_counts={'same_day': 1},\n"
        ")\n"
        "fake_stats_obj = types.SimpleNamespace(source_path='provider-stats-path', stats_date='20260626')\n"
        "auction_meta = auction_fetch.auction_indicator_meta(\n"
        "    auction_result,\n"
        "    stats=fake_stats_obj,\n"
        "    stats_refreshed=True,\n"
        "    requested_code_count=4,\n"
        ")\n"
        "price_result = price_limit_fetch.DailyPriceLimitResult(\n"
        "    rows=[],\n"
        "    mode='latest_snapshot',\n"
        "    target_trade_date='20260626',\n"
        "    pre_close_trade_date='20260625',\n"
        "    calendar_source='source_request',\n"
        "    snapshot_base_field='pre_close',\n"
        "    kline_page_count=0,\n"
        "    quote_count=5,\n"
        "    quote_batch_count=1,\n"
        "    quote_concurrency=1,\n"
        ")\n"
        "price_meta = price_limit_fetch.daily_price_limit_meta(\n"
        "    price_result,\n"
        "    trade_date=None,\n"
        "    expanded_scope=None,\n"
        "    requested_code_count=5,\n"
        "    client_pool_size=2,\n"
        "    default_quote_batch_size=80,\n"
        ")\n"
        "source_rows = limit_ladder_fetch.LimitLadderSourceRowsResult(\n"
        "    stats=fake_stats_obj,\n"
        "    stats_refreshed=False,\n"
        "    rank_rows=[{}],\n"
        "    page_count=2,\n"
        "    rows=[{}],\n"
        "    stats_ms=1,\n"
        "    rank_ms=2,\n"
        "    names_ms=3,\n"
        "    normalize_ms=4,\n"
        ")\n"
        "ladder_meta = limit_ladder_fetch.limit_ladder_meta(\n"
        "    source_rows,\n"
        "    [{}],\n"
        "    count=None,\n"
        "    boards=None,\n"
        "    include_touched=False,\n"
        "    topic_type='theme',\n"
        "    theme_lookup_performed=False,\n"
        "    topic_worker_count=None,\n"
        "    topic_lookup_meta={},\n"
        "    topic_missing_stock_count=0,\n"
        "    theme_lookup_ms=0,\n"
        "    theme_attach_ms=0,\n"
        "    sort_ms=0,\n"
        "    total_ms=10,\n"
        ")\n"
        "fake_stats = types.ModuleType('axdata_source_tdx.stats_resource')\n"
        "fake_stats.ensure_tdx_stats_resource_for_params = lambda *args, **kwargs: ('provider-stats', True)\n"
        "sys.modules['axdata_source_tdx.stats_resource'] = fake_stats\n"
        "stats_resource, stats_refreshed = auction_fetch._ensure_tdx_stats_resource_for_params(None, {})\n"
        "ladder_stats_resource, ladder_stats_refreshed = limit_ladder_fetch._ensure_tdx_stats_resource_for_params(None, {})\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.auction_fetch',\n"
        "    'axdata_core.adapters.tdx.price_limit_fetch',\n"
        "    'axdata_core.adapters.tdx.limit_ladder_fetch',\n"
        "    'axdata_core.adapters.tdx.stats_resource',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_auction_fetch=' + str('axdata_source_tdx.auction_fetch' in sys.modules))\n"
        "print('provider_price_limit_fetch=' + str('axdata_source_tdx.price_limit_fetch' in sys.modules))\n"
        "print('provider_limit_ladder_fetch=' + str('axdata_source_tdx.limit_ladder_fetch' in sys.modules))\n"
        "print('auction_module=' + auction_fetch.auction_indicator_meta.__module__)\n"
        "print('price_module=' + price_limit_fetch.daily_price_limit_meta.__module__)\n"
        "print('ladder_module=' + limit_ladder_fetch.limit_ladder_meta.__module__)\n"
        "print('auction_quote_count=' + str(auction_meta['tdx_quote_count']))\n"
        "print('price_mode=' + str(price_meta['tdx_price_limit_mode']))\n"
        "print('ladder_rank_count=' + str(ladder_meta['tdx_rank_scanned_count']))\n"
        "print('stats_resource=' + str(stats_resource))\n"
        "print('stats_refreshed=' + str(stats_refreshed))\n"
        "print('ladder_stats_resource=' + str(ladder_stats_resource))\n"
        "print('ladder_stats_refreshed=' + str(ladder_stats_refreshed))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_auction_fetch=True" in result.stdout
    assert "provider_price_limit_fetch=True" in result.stdout
    assert "provider_limit_ladder_fetch=True" in result.stdout
    assert "auction_module=axdata_source_tdx.auction_fetch" in result.stdout
    assert "price_module=axdata_source_tdx.price_limit_fetch" in result.stdout
    assert "ladder_module=axdata_source_tdx.limit_ladder_fetch" in result.stdout
    assert "auction_quote_count=1" in result.stdout
    assert "price_mode=latest_snapshot" in result.stdout
    assert "ladder_rank_count=1" in result.stdout
    assert "stats_resource=provider-stats" in result.stdout
    assert "stats_refreshed=True" in result.stdout
    assert "ladder_stats_resource=provider-stats" in result.stdout
    assert "ladder_stats_refreshed=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_request_fetch_helpers_import_without_core_runtime() -> None:
    code = (
        "import sys\n"
        "from types import SimpleNamespace\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import adjustment_fetch, code_fetch, rank_fetch, series_history, status_fetch\n"
        "adjustment_result = adjustment_fetch.AdjustmentFactorRowsResult(\n"
        "    rows=[],\n"
        "    page_count=2,\n"
        "    event_count=3,\n"
        "    xdxr_event_count=1,\n"
        ")\n"
        "adjustment_meta = adjustment_fetch.adjustment_factor_meta(\n"
        "    adjustment_result,\n"
        "    adjust='qfq',\n"
        "    anchor_date='20260626',\n"
        "    requested_code_count=4,\n"
        ")\n"
        "pool = code_fetch.stock_rows_to_tdx_code_pool([\n"
        "    {'tdx_code': 'sz000001', 'name': '平安银行'},\n"
        "    {'tdx_code': 'sz000001', 'name': '重复'},\n"
        "    {'tdx_code': 'sh600000', 'name': '浦发银行'},\n"
        "])\n"
        "rank_result = rank_fetch.RealtimeRankResult(rows=[{}], page_count=2, sort_reverse=False)\n"
        "rank_rows = rank_fetch.RealtimeRankRowsResult(rows=rank_result.rows, meta={'tdx_protocol': '0x054b'})\n"
        "parallel_options = series_history.KlineParallelOptions(hosts=['1.1.1.1'], pool_size=2)\n"
        "status_rows = status_fetch.st_rows_from_stock_rows(\n"
        "    [{'instrument_id': '000001.SZ', 'name': '*ST测试'}],\n"
        "    st_type_from_name=lambda name: 'st' if 'ST' in str(name) else None,\n"
        "    normalize_st_row=lambda row, st_type: {'instrument_id': row['instrument_id'], 'st_type': st_type},\n"
        ")\n"
        "status_result = status_fetch.StockStatusResult(rows=status_rows, meta={'checked_at': '2026-06-26T15:00:00+08:00'})\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.adjustment_fetch',\n"
        "    'axdata_core.adapters.tdx.code_fetch',\n"
        "    'axdata_core.adapters.tdx.rank_fetch',\n"
        "    'axdata_core.adapters.tdx.series_history',\n"
        "    'axdata_core.adapters.tdx.status_fetch',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_adjustment_fetch=' + str('axdata_source_tdx.adjustment_fetch' in sys.modules))\n"
        "print('provider_code_fetch=' + str('axdata_source_tdx.code_fetch' in sys.modules))\n"
        "print('provider_rank_fetch=' + str('axdata_source_tdx.rank_fetch' in sys.modules))\n"
        "print('provider_series_history=' + str('axdata_source_tdx.series_history' in sys.modules))\n"
        "print('provider_status_fetch=' + str('axdata_source_tdx.status_fetch' in sys.modules))\n"
        "print('adjustment_module=' + adjustment_fetch.adjustment_factor_meta.__module__)\n"
        "print('code_module=' + code_fetch.stock_rows_to_tdx_code_pool.__module__)\n"
        "print('rank_module=' + rank_fetch.RealtimeRankRowsResult.__module__)\n"
        "print('series_module=' + series_history.KlineParallelOptions.__module__)\n"
        "print('status_module=' + status_fetch.StockStatusResult.__module__)\n"
        "print('adjustment_event_count=' + str(adjustment_meta['tdx_event_count']))\n"
        "print('pool_count=' + str(len(pool[0])))\n"
        "print('rank_protocol=' + str(rank_rows.meta['tdx_protocol']))\n"
        "print('parallel_pool_size=' + str(parallel_options.pool_size))\n"
        "print('status_count=' + str(len(status_result.rows)))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_adjustment_fetch=True" in result.stdout
    assert "provider_code_fetch=True" in result.stdout
    assert "provider_rank_fetch=True" in result.stdout
    assert "provider_series_history=True" in result.stdout
    assert "provider_status_fetch=True" in result.stdout
    assert "adjustment_module=axdata_source_tdx.adjustment_fetch" in result.stdout
    assert "code_module=axdata_source_tdx.code_fetch" in result.stdout
    assert "rank_module=axdata_source_tdx.rank_fetch" in result.stdout
    assert "series_module=axdata_source_tdx.series_history" in result.stdout
    assert "status_module=axdata_source_tdx.status_fetch" in result.stdout
    assert "adjustment_event_count=3" in result.stdout
    assert "pool_count=2" in result.stdout
    assert "rank_protocol=0x054b" in result.stdout
    assert "parallel_pool_size=2" in result.stdout
    assert "status_count=1" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_server_config_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.tdx_server_config as server_config\n"
        "servers = server_config.built_in_servers('quote')\n"
        "print('provider_server_config=' + str('axdata_source_tdx.tdx_server_config' in sys.modules))\n"
        "print('server_count=' + str(len(servers)))\n"
        "print('first_port=' + str(servers[0].port))\n"
        "print('core_server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_server_config=True" in result.stdout
    assert "server_count=" in result.stdout
    assert "server_count=0" not in result.stdout
    assert "first_port=7709" in result.stdout
    assert "core_server_config=False" in result.stdout
    assert "request=False" in result.stdout
    assert "source_request=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_core_tdx_client_factory_prefers_provider_package_when_available() -> None:
    code = (
        "import sys\n"
        "from axdata_core.adapters.tdx import client_factory\n"
        "import axdata_source_tdx.client_factory as provider_client_factory\n"
        "def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):\n"
        "    return {\n"
        "        'hosts': list(hosts or []),\n"
        "        'pool_size': pool_size,\n"
        "        'heartbeat_interval': heartbeat_interval,\n"
        "    }\n"
        "provider_client_factory.create_tdx_client = fake_create_tdx_client\n"
        "provider_client_factory.tdx_env_int = lambda name, default, *, minimum: 42\n"
        "client = client_factory.create_tdx_client(\n"
        "    hosts=['provider-host:7709'],\n"
        "    pool_size=5,\n"
        "    heartbeat_interval=None,\n"
        ")\n"
        "print('client=' + repr(client))\n"
        "print('env_int=' + str(client_factory.tdx_env_int('AXDATA_TDX_DEMO_INT', 1, minimum=1)))\n"
        "print('provider_client_factory=' + str('axdata_source_tdx.client_factory' in sys.modules))\n"
        "print('provider_host_config=' + str('axdata_source_tdx.host_config' in sys.modules))\n"
        "print('core_client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('core_host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "client={'hosts': ['provider-host:7709'], 'pool_size': 5, 'heartbeat_interval': None}" in result.stdout
    assert "env_int=42" in result.stdout
    assert "provider_client_factory=True" in result.stdout
    assert "provider_host_config=False" in result.stdout
    assert "core_client_factory=True" in result.stdout
    assert "core_host_config=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_core_tdx_stats_resource_import_is_provider_first_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_core.adapters.tdx import stats_resource\n"
        "print('core_stats_resource=' + str('axdata_core.adapters.tdx.stats_resource' in sys.modules))\n"
        "print('core_stats_cache_before=' + str('axdata_core.adapters.tdx.stats_cache' in sys.modules))\n"
        "print('core_stats_models_before=' + str('axdata_core.adapters.tdx.stats_models' in sys.modules))\n"
        "print('provider_stats_resource_before=' + str('axdata_source_tdx.stats_resource' in sys.modules))\n"
        "print('provider_stats_cache_before=' + str('axdata_source_tdx.stats_cache' in sys.modules))\n"
        "value = stats_resource._refresh_stats_param('yes')\n"
        "print('value=' + str(value))\n"
        "print('provider_stats_resource_after=' + str('axdata_source_tdx.stats_resource' in sys.modules))\n"
        "print('provider_stats_cache_after=' + str('axdata_source_tdx.stats_cache' in sys.modules))\n"
        "print('core_stats_cache_after=' + str('axdata_core.adapters.tdx.stats_cache' in sys.modules))\n"
        "print('core_stats_models_after=' + str('axdata_core.adapters.tdx.stats_models' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "core_stats_resource=True" in result.stdout
    assert "core_stats_cache_before=False" in result.stdout
    assert "core_stats_models_before=False" in result.stdout
    assert "provider_stats_resource_before=False" in result.stdout
    assert "provider_stats_cache_before=False" in result.stdout
    assert "value=True" in result.stdout
    assert "provider_stats_resource_after=True" in result.stdout
    assert "provider_stats_cache_after=True" in result.stdout
    assert "core_stats_cache_after=False" in result.stdout
    assert "core_stats_models_after=False" in result.stdout
    assert "request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_server_config_provider_and_core_resource_files_match() -> None:
    core_resource = REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "resources"
    provider_resource = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx" / "resources"

    for resource_name in ["tdx_quote_servers.json", "tdx_extended_servers.json"]:
        assert (provider_resource / resource_name).read_text(encoding="utf-8") == (
            core_resource / resource_name
        ).read_text(encoding="utf-8")


def test_tdx_provider_wire_hosts_reexport_static_defaults() -> None:
    from axdata_source_tdx._tdx_wire import hosts
    from axdata_source_tdx._tdx_wire._host_defaults import DEFAULT_QUOTE_HOSTS

    assert hosts.FALLBACK_HOSTS is DEFAULT_QUOTE_HOSTS
    assert hosts.FALLBACK_HOSTS[0] == "116.205.183.150:7709"


def test_tdx_provider_wire_hosts_reexport_resource_loader() -> None:
    from axdata_source_tdx._tdx_wire import _host_resource, hosts
    from axdata_source_tdx._tdx_wire._host_defaults import DEFAULT_QUOTE_HOSTS

    assert hosts.DEFAULT_HOSTS is _host_resource.DEFAULT_HOSTS
    assert list(DEFAULT_QUOTE_HOSTS) == _host_resource.load_server_hosts()
    assert hosts.SERVER_RESOURCE_PACKAGE == _host_resource.SERVER_RESOURCE_PACKAGE == "axdata_source_tdx.resources"
    assert hosts.SERVER_FILE == _host_resource.SERVER_FILE == "tdx_quote_servers.json"
    assert hosts.load_server_config is _host_resource.load_server_config
    assert hosts.load_server_hosts is _host_resource.load_server_hosts


def test_tdx_provider_wire_hosts_resource_exports_are_lazy() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import hosts\n"
        "print('host_resource_after_import=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('fallback_count=' + str(len(hosts.FALLBACK_HOSTS)))\n"
        "print('host_resource_after_static=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('default_count=' + str(len(hosts.DEFAULT_HOSTS)))\n"
        "print('resource_package=' + hosts.SERVER_RESOURCE_PACKAGE)\n"
        "print('server_count=' + str(len(hosts.load_server_hosts())))\n"
        "print('host_resource_after_resource=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "host_resource_after_import=False" in result.stdout
    assert "fallback_count=43" in result.stdout
    assert "host_resource_after_static=False" in result.stdout
    assert "default_count=43" in result.stdout
    assert "resource_package=axdata_source_tdx.resources" in result.stdout
    assert "server_count=43" in result.stdout
    assert "host_resource_after_resource=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_hosts_support_legacy_resource_imports() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.hosts import DEFAULT_HOSTS, SERVER_RESOURCE_PACKAGE, load_server_hosts\n"
        "print('default_count=' + str(len(DEFAULT_HOSTS)))\n"
        "print('resource_package=' + SERVER_RESOURCE_PACKAGE)\n"
        "print('server_count=' + str(len(load_server_hosts())))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "default_count=43" in result.stdout
    assert "resource_package=axdata_source_tdx.resources" in result.stdout
    assert "server_count=43" in result.stdout


def test_tdx_provider_wire_modules_share_connection_defaults() -> None:
    from axdata_source_tdx._tdx_wire import _connection_defaults, client, hosts
    from axdata_source_tdx._tdx_wire.transport import pool, socket

    assert client.DEFAULT_PROBE_TIMEOUT is _connection_defaults.DEFAULT_PROBE_TIMEOUT
    assert client.DEFAULT_PROBE_WORKERS is _connection_defaults.DEFAULT_PROBE_WORKERS
    assert hosts.DEFAULT_PROBE_TIMEOUT is _connection_defaults.DEFAULT_PROBE_TIMEOUT
    assert hosts.DEFAULT_PROBE_WORKERS is _connection_defaults.DEFAULT_PROBE_WORKERS
    assert pool.DEFAULT_PROBE_TIMEOUT is _connection_defaults.DEFAULT_PROBE_TIMEOUT
    assert pool.DEFAULT_PROBE_WORKERS is _connection_defaults.DEFAULT_PROBE_WORKERS
    assert pool.DEFAULT_HEARTBEAT_INTERVAL is _connection_defaults.DEFAULT_HEARTBEAT_INTERVAL
    assert socket.DEFAULT_HEARTBEAT_INTERVAL is _connection_defaults.DEFAULT_HEARTBEAT_INTERVAL


def test_tdx_provider_wire_default_hosts_avoid_hosts_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire.transport.socket import SocketTransport\n"
        "from axdata_source_tdx._tdx_wire.transport.pool import PooledSocketTransport\n"
        "socket_transport = SocketTransport()\n"
        "pool_transport = PooledSocketTransport(pool_size=1)\n"
        "print('socket_default_count=' + str(len(socket_transport._hosts)))\n"
        "print('pool_default_count=' + str(len(pool_transport.hosts)))\n"
        "print('first_socket=' + socket_transport._hosts[0])\n"
        "print('first_pool=' + pool_transport.hosts[0])\n"
        "print('host_resource=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('hosts_module=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "socket_default_count=43" in result.stdout
    assert "pool_default_count=43" in result.stdout
    assert "first_socket=116.205.183.150:7709" in result.stdout
    assert "first_pool=116.205.183.150:7709" in result.stdout
    assert "host_resource=True" in result.stdout
    assert "hosts_module=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_host_probe_defers_tcp_runtime_imports() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _host_probe\n"
        "print('socket_after_import=' + str('socket' in sys.modules))\n"
        "print('time_after_import=' + str('time' in sys.modules))\n"
        "print('futures_after_import=' + str('concurrent.futures' in sys.modules))\n"
        "invalid = _host_probe.probe_host('bad')\n"
        "empty = _host_probe.probe_hosts(['bad'])\n"
        "print('invalid=' + repr((invalid.host, invalid.ok, invalid.error)))\n"
        "print('empty=' + repr(empty))\n"
        "print('socket_after_invalid=' + str('socket' in sys.modules))\n"
        "print('time_after_invalid=' + str('time' in sys.modules))\n"
        "print('futures_after_empty=' + str('concurrent.futures' in sys.modules))\n"
        "print('hosts_module=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('host_resource=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "socket_after_import=False" in result.stdout
    assert "futures_after_import=False" in result.stdout
    assert "invalid=('bad', False, 'invalid host')" in result.stdout
    assert "empty=[]" in result.stdout
    assert "socket_after_invalid=False" in result.stdout
    assert "futures_after_empty=False" in result.stdout
    assert "hosts_module=False" in result.stdout
    assert "host_resource=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_frame_defers_stdlib_runtime_imports() -> None:
    code = (
        "import sys\n"
        "import zlib as zlib_for_payload\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "payload = zlib_for_payload.compress(b'abc')\n"
        "del sys.modules['zlib']\n"
        "from axdata_source_tdx._tdx_wire.protocol import frame\n"
        "print('socket_after_import=' + str('socket' in sys.modules))\n"
        "print('struct_after_import=' + str('struct' in sys.modules))\n"
        "print('zlib_after_import=' + str('zlib' in sys.modules))\n"
        "request = frame.RequestFrame(msg_id=5, msg_type=0x053e, data=b'abc')\n"
        "print('frame_bytes=' + request.to_bytes().hex())\n"
        "print('socket_after_to_bytes=' + str('socket' in sys.modules))\n"
        "print('struct_after_to_bytes=' + str('struct' in sys.modules))\n"
        "print('zlib_after_to_bytes=' + str('zlib' in sys.modules))\n"
        "raw = b'\\xb1\\xcb\\x74\\x00\\x01\\x05\\x00\\x00\\x00\\x00\\x3e\\x05' + len(payload).to_bytes(2, 'little') + (3).to_bytes(2, 'little') + payload\n"
        "decoded = frame.decode_response(raw)\n"
        "print('decoded=' + decoded.data.decode('ascii'))\n"
        "print('socket_after_decode=' + str('socket' in sys.modules))\n"
        "print('zlib_after_decode=' + str('zlib' in sys.modules))\n"
        "print('socket_attr=' + frame.socket.__name__)\n"
        "print('socket_after_attr=' + str('socket' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "socket_after_import=False" in result.stdout
    assert "struct_after_import=False" in result.stdout
    assert "zlib_after_import=False" in result.stdout
    assert "frame_bytes=0c0500000001050005003e05616263" in result.stdout
    assert "socket_after_to_bytes=False" in result.stdout
    assert "struct_after_to_bytes=True" in result.stdout
    assert "zlib_after_to_bytes=False" in result.stdout
    assert "decoded=abc" in result.stdout
    assert "socket_after_decode=False" in result.stdout
    assert "zlib_after_decode=True" in result.stdout
    assert "socket_attr=socket" in result.stdout
    assert "socket_after_attr=True" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_pool_probe_uses_host_probe_without_hosts_runtime() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx._tdx_wire import _host_probe\n"
        "from axdata_source_tdx._tdx_wire.transport.pool import PooledSocketTransport\n"
        "calls = []\n"
        "def fake_sort(hosts, *, timeout, max_workers):\n"
        "    calls.append((tuple(hosts), timeout, max_workers))\n"
        "    return list(reversed(hosts))\n"
        "_host_probe.sort_hosts_by_latency = fake_sort\n"
        "transport = PooledSocketTransport(hosts=['127.0.0.1:7709', '127.0.0.2:7709'], probe_hosts=True, probe_timeout=0.25, probe_workers=3)\n"
        "print('pool_hosts=' + ','.join(transport.hosts))\n"
        "print('calls=' + repr(calls))\n"
        "print('host_probe=' + str('axdata_source_tdx._tdx_wire._host_probe' in sys.modules))\n"
        "print('hosts_module=' + str('axdata_source_tdx._tdx_wire.hosts' in sys.modules))\n"
        "print('host_resource=' + str('axdata_source_tdx._tdx_wire._host_resource' in sys.modules))\n"
        "print('socket_module=' + str('axdata_source_tdx._tdx_wire.transport.socket' in sys.modules))\n"
        "print('core_wire=' + str('axdata_core._tdx_wire' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "pool_hosts=127.0.0.2:7709,127.0.0.1:7709" in result.stdout
    assert "('127.0.0.1:7709', '127.0.0.2:7709'), 0.25, 3" in result.stdout
    assert "host_probe=True" in result.stdout
    assert "hosts_module=False" in result.stdout
    assert "host_resource=False" in result.stdout
    assert "socket_module=False" in result.stdout
    assert "core_wire=False" in result.stdout


def test_tdx_provider_wire_hosts_share_normalization_helpers() -> None:
    from axdata_source_tdx._tdx_wire import _host_utils, hosts

    assert hosts.normalize_host is _host_utils.normalize_host
    assert hosts.unique_hosts is _host_utils.unique_hosts
    assert hosts.unique_hosts([" 127.0.0.1:7709 ", "127.0.0.1:7709", "bad"]) == [
        "127.0.0.1:7709"
    ]
    for relative_path in [
        "transport/socket.py",
        "transport/pool.py",
        "hosts.py",
    ]:
        text = (
            REPO_ROOT
            / "packages"
            / "axdata-source-tdx"
            / "src"
            / "axdata_source_tdx"
            / "_tdx_wire"
            / relative_path
        ).read_text(encoding="utf-8")
        assert "def _normalize_host" not in text
        assert "def _unique_hosts" not in text


def test_tdx_provider_stats_modules_import_without_core_runtime() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.stats_models',\n"
        "    'axdata_source_tdx.stats_cache',\n"
        "    'axdata_source_tdx.stats_resource',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.client_factory',\n"
        "    'axdata_core.adapters.tdx.provider_bridge',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('provider_stats_models=' + str('axdata_source_tdx.stats_models' in sys.modules))\n"
        "print('provider_stats_cache=' + str('axdata_source_tdx.stats_cache' in sys.modules))\n"
        "print('provider_stats_resource=' + str('axdata_source_tdx.stats_resource' in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_stats_models=True" in result.stdout
    assert "provider_stats_cache=True" in result.stdout
    assert "provider_stats_resource=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_core_tdx_stats_resource_prefers_provider_package_when_available() -> None:
    code = (
        "import sys\n"
        "from types import SimpleNamespace\n"
        "from axdata_core.adapters.tdx import stats_resource\n"
        "import axdata_source_tdx.stats_resource as provider_stats_resource\n"
        "def fake_ensure(client, params, *, validation_error=ValueError, **kwargs):\n"
        "    return SimpleNamespace(source_path='provider-stats', stats_date='20260626'), True\n"
        "provider_stats_resource.ensure_tdx_stats_resource_for_params = fake_ensure\n"
        "resource, refreshed = stats_resource.ensure_tdx_stats_resource_for_params(\n"
        "    object(), {'refresh_stats': True}\n"
        ")\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.client_factory',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('source_path=' + resource.source_path)\n"
        "print('refreshed=' + str(refreshed))\n"
        "print('provider_stats_resource=' + str('axdata_source_tdx.stats_resource' in sys.modules))\n"
        "print('core_stats_resource=' + str('axdata_core.adapters.tdx.stats_resource' in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "source_path=provider-stats" in result.stdout
    assert "refreshed=True" in result.stdout
    assert "provider_stats_resource=True" in result.stdout
    assert "core_stats_resource=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_rule_modules_import_without_core_request_runtime() -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "for module_name in [\n"
        "    'axdata_source_tdx.codes',\n"
        "    'axdata_source_tdx.normalize_utils',\n"
        "    'axdata_source_tdx.security_codes',\n"
        "    'axdata_source_tdx.request_filters',\n"
        "    'axdata_source_tdx.request_params',\n"
        "    'axdata_source_tdx.rank_params',\n"
        "    'axdata_source_tdx.request_limits',\n"
        "]:\n"
        "    importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.client_factory',\n"
        "    'axdata_core.adapters.tdx.provider_bridge',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "]\n"
        "print('provider_codes=' + str('axdata_source_tdx.codes' in sys.modules))\n"
        "print('provider_normalize_utils=' + str('axdata_source_tdx.normalize_utils' in sys.modules))\n"
        "print('provider_security_codes=' + str('axdata_source_tdx.security_codes' in sys.modules))\n"
        "print('provider_request_filters=' + str('axdata_source_tdx.request_filters' in sys.modules))\n"
        "print('provider_request_params=' + str('axdata_source_tdx.request_params' in sys.modules))\n"
        "print('provider_rank_params=' + str('axdata_source_tdx.rank_params' in sys.modules))\n"
        "print('provider_request_limits=' + str('axdata_source_tdx.request_limits' in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_codes=True" in result.stdout
    assert "provider_normalize_utils=True" in result.stdout
    assert "provider_security_codes=True" in result.stdout
    assert "provider_request_filters=True" in result.stdout
    assert "provider_request_params=True" in result.stdout
    assert "provider_rank_params=True" in result.stdout
    assert "provider_request_limits=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_stats_resource_import_does_not_load_source_request() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.stats_resource\n"
        "print('stats_resource=' + str('axdata_core.adapters.tdx.stats_resource' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('f10_request=' + str('axdata_core.adapters.tdx.f10_request' in sys.modules))\n"
        "print('tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "stats_resource=True" in result.stdout
    assert "request=False" in result.stdout
    assert "downloader=False" in result.stdout
    assert "f10_request=False" in result.stdout
    assert "tqlex=False" in result.stdout
    assert "f10_specs=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "host_config=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_stats_models_import_does_not_load_cache_resource() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.stats_models\n"
        "print('stats_models=' + str('axdata_core.adapters.tdx.stats_models' in sys.modules))\n"
        "print('stats_resource=' + str('axdata_core.adapters.tdx.stats_resource' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "stats_models=True" in result.stdout
    assert "stats_resource=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_stats_cache_import_does_not_load_resource_adapter() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.stats_cache\n"
        "print('stats_cache=' + str('axdata_core.adapters.tdx.stats_cache' in sys.modules))\n"
        "print('stats_models=' + str('axdata_core.adapters.tdx.stats_models' in sys.modules))\n"
        "print('stats_resource=' + str('axdata_core.adapters.tdx.stats_resource' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "stats_cache=True" in result.stdout
    assert "stats_models=False" in result.stdout
    assert "stats_resource=False" in result.stdout
    assert "request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_stats_cache_should_refresh_by_market_date_alignment() -> None:
    from axdata_core.adapters.tdx import stats_cache

    same_day_resource = SimpleNamespace(stats_date="20260612", metadata={})
    previous_trade_date_resource = SimpleNamespace(stats_date="20260611", metadata={})
    stale_resource = SimpleNamespace(stats_date="20260610", metadata={})
    checked_without_calendar = SimpleNamespace(
        stats_date="20260611",
        metadata={"checked_target_trade_date": "20260612"},
    )

    assert stats_cache.stats_cache_should_refresh(
        same_day_resource,
        target_trade_date="20260612",
        previous_trade_date="20260611",
    ) is False
    assert stats_cache.stats_cache_should_refresh(
        previous_trade_date_resource,
        target_trade_date="20260612",
        previous_trade_date="20260611",
    ) is False
    assert stats_cache.stats_cache_should_refresh(
        stale_resource,
        target_trade_date="20260612",
        previous_trade_date="20260611",
    ) is True
    assert stats_cache.stats_cache_should_refresh(
        checked_without_calendar,
        target_trade_date="20260612",
    ) is False


def test_tdx_stats_resolve_source_prefers_explicit_zip_cfg_dir_and_dir_zip(tmp_path) -> None:
    from axdata_core.adapters.tdx import stats_cache

    explicit_zip = tmp_path / "explicit.zip"
    explicit_zip.write_bytes(
        _stats_zip_bytes(
            _minimal_stat_line("20260612"),
            _minimal_stat2_line("20260612", prev_amount="200.00"),
        )
    )
    cfg_dir = tmp_path / "cfg-dir"
    cfg_dir.mkdir()
    (cfg_dir / "tdxstat.cfg").write_text(_minimal_stat_line("20260613"), encoding="gbk")
    (cfg_dir / "tdxstat2.cfg").write_text(
        _minimal_stat2_line("20260613", prev_amount="300.00"),
        encoding="gbk",
    )
    zip_dir = tmp_path / "zip-dir"
    zip_dir.mkdir()
    (zip_dir / "zhb.zip").write_bytes(
        _stats_zip_bytes(
            _minimal_stat_line("20260614"),
            _minimal_stat2_line("20260614", prev_amount="400.00"),
        )
    )

    assert stats_cache.resolve_stats_source(explicit_zip) == explicit_zip
    assert stats_cache.resolve_stats_source(cfg_dir) == cfg_dir
    assert stats_cache.resolve_stats_source(zip_dir) == zip_dir / "zhb.zip"

    with pytest.raises(FileNotFoundError, match="TDX stats resource not found"):
        stats_cache.resolve_stats_source(tmp_path / "missing")


def test_tdx_stats_resource_explicit_root_bypasses_refresh_and_cache(tmp_path) -> None:
    from axdata_core.adapters.tdx.stats_resource import ensure_tdx_stats_resource_for_params

    stats_root = tmp_path / "local-stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(
        _minimal_stat2_line("20260612", prev_amount="200.00"),
        encoding="gbk",
    )
    cache_root = tmp_path / "cache"
    client = FakeTdxClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        _minimal_stat_line("20260613"),
        _minimal_stat2_line("20260613", prev_amount="300.00"),
    )

    resource, refreshed = ensure_tdx_stats_resource_for_params(
        client,
        {
            "stats_root": str(stats_root),
            "stats_cache_root": str(cache_root),
            "refresh_stats": True,
        },
    )

    assert resource.stats_date == "20260612"
    assert resource.source_path == str(stats_root)
    assert refreshed is False
    assert client.download_file_calls == []
    assert not cache_root.exists()


def test_tdx_realtime_refresh_import_does_not_load_request_adapter() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.realtime_refresh\n"
        "print('realtime_refresh=' + str('axdata_core.adapters.tdx.realtime_refresh' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "realtime_refresh=True" in result.stdout
    assert "request=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "host_config=False" in result.stdout
    assert "server_config=False" in result.stdout
    assert "f10_specs=False" in result.stdout


def test_tdx_provider_realtime_refresh_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.realtime_refresh\n"
        "print('realtime_refresh=' + str('axdata_source_tdx.realtime_refresh' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "realtime_refresh=True" in result.stdout
    assert "request=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "host_config=False" in result.stdout
    assert "server_config=False" in result.stdout
    assert "f10_specs=False" in result.stdout


def test_tdx_request_compat_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.request_compat\n"
        "print('request_compat=' + str('axdata_core.adapters.tdx.request_compat' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('f10_request=' + str('axdata_core.adapters.tdx.f10_request' in sys.modules))\n"
        "print('f10_normalize=' + str('axdata_core.adapters.tdx.f10_normalize' in sys.modules))\n"
        "print('f10_postprocess=' + str('axdata_core.adapters.tdx.f10_postprocess' in sys.modules))\n"
        "print('price_limit_calendar=' + str('axdata_core.adapters.tdx.price_limit_calendar' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "request_compat=True" in result.stdout
    assert "request=False" in result.stdout
    assert "f10_request=False" in result.stdout
    assert "f10_normalize=False" in result.stdout
    assert "f10_postprocess=False" in result.stdout
    assert "price_limit_calendar=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_request_compat_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.request_compat\n"
        "print('provider_request_compat=' + str('axdata_source_tdx.request_compat' in sys.modules))\n"
        "print('core_request_compat=' + str('axdata_core.adapters.tdx.request_compat' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('provider_f10_request=' + str('axdata_source_tdx.f10_request' in sys.modules))\n"
        "print('provider_f10_normalize=' + str('axdata_source_tdx.f10_normalize' in sys.modules))\n"
        "print('provider_f10_postprocess=' + str('axdata_source_tdx.f10_postprocess' in sys.modules))\n"
        "print('provider_price_limit_calendar=' + str('axdata_source_tdx.price_limit_calendar' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_request_compat=True" in result.stdout
    assert "core_request_compat=False" in result.stdout
    assert "request=False" in result.stdout
    assert "provider_f10_request=False" in result.stdout
    assert "provider_f10_normalize=False" in result.stdout
    assert "provider_f10_postprocess=False" in result.stdout
    assert "provider_price_limit_calendar=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_request_adapter_runtime_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.request_adapter_runtime\n"
        "print('provider_runtime=' + str('axdata_source_tdx.request_adapter_runtime' in sys.modules))\n"
        "print('core_runtime=' + str('axdata_core.adapters.tdx.request_adapter_runtime' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('provider_dispatch=' + str('axdata_source_tdx.request_dispatch' in sys.modules))\n"
        "print('provider_client=' + str('axdata_source_tdx.request_client' in sys.modules))\n"
        "print('core_dispatch=' + str('axdata_core.adapters.tdx.request_dispatch' in sys.modules))\n"
        "print('core_client=' + str('axdata_core.adapters.tdx.request_client' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_runtime=True" in result.stdout
    assert "core_runtime=False" in result.stdout
    assert "request=False" in result.stdout
    assert "provider_dispatch=False" in result.stdout
    assert "provider_client=False" in result.stdout
    assert "core_dispatch=False" in result.stdout
    assert "core_client=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_request_methods_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.request_methods\n"
        "print('provider_methods=' + str('axdata_source_tdx.request_methods' in sys.modules))\n"
        "print('core_methods=' + str('axdata_core.adapters.tdx.request_methods' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('provider_code_fetch=' + str('axdata_source_tdx.code_fetch' in sys.modules))\n"
        "print('provider_quote_fetch=' + str('axdata_source_tdx.quote_fetch' in sys.modules))\n"
        "print('provider_kline_helpers=' + str('axdata_source_tdx.kline_helpers' in sys.modules))\n"
        "print('provider_wire_requests=' + str('axdata_source_tdx.wire_requests' in sys.modules))\n"
        "print('core_code_fetch=' + str('axdata_core.adapters.tdx.code_fetch' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_methods=True" in result.stdout
    assert "core_methods=False" in result.stdout
    assert "request=False" in result.stdout
    assert "provider_code_fetch=False" in result.stdout
    assert "provider_quote_fetch=False" in result.stdout
    assert "provider_kline_helpers=False" in result.stdout
    assert "provider_wire_requests=False" in result.stdout
    assert "core_code_fetch=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_request_host_config_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.request_host_config\n"
        "print('provider_request_host_config=' + str('axdata_source_tdx.request_host_config' in sys.modules))\n"
        "print('core_request_host_config=' + str('axdata_core.adapters.tdx.request_host_config' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('provider_host_config=' + str('axdata_source_tdx.host_config' in sys.modules))\n"
        "print('provider_server_config=' + str('axdata_source_tdx.tdx_server_config' in sys.modules))\n"
        "print('core_host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('core_server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_request_host_config=True" in result.stdout
    assert "core_request_host_config=False" in result.stdout
    assert "request=False" in result.stdout
    assert "provider_host_config=False" in result.stdout
    assert "provider_server_config=False" in result.stdout
    assert "core_host_config=False" in result.stdout
    assert "core_server_config=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_request_seams_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.request_seams\n"
        "print('provider_request_seams=' + str('axdata_source_tdx.request_seams' in sys.modules))\n"
        "print('core_request_seams=' + str('axdata_core.adapters.tdx.request_seams' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('provider_quote_fetch=' + str('axdata_source_tdx.quote_fetch' in sys.modules))\n"
        "print('provider_finance_fetch=' + str('axdata_source_tdx.finance_fetch' in sys.modules))\n"
        "print('provider_series_history=' + str('axdata_source_tdx.series_history' in sys.modules))\n"
        "print('provider_price_limit_history=' + str('axdata_source_tdx.price_limit_history' in sys.modules))\n"
        "print('core_quote_fetch=' + str('axdata_core.adapters.tdx.quote_fetch' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_request_seams=True" in result.stdout
    assert "core_request_seams=False" in result.stdout
    assert "request=False" in result.stdout
    assert "provider_quote_fetch=False" in result.stdout
    assert "provider_finance_fetch=False" in result.stdout
    assert "provider_series_history=False" in result.stdout
    assert "provider_price_limit_history=False" in result.stdout
    assert "core_quote_fetch=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_core_tdx_host_config_default_hosts_require_provider_wire() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_core.adapters.tdx import host_config\n"
        "hosts = host_config.default_tdx_hosts()\n"
        "print('count=' + str(len(hosts)))\n"
        "print('first=' + hosts[0])\n"
        "print('provider_wire=' + str('axdata_source_tdx.wire' in sys.modules))\n"
        "print('wire_hosts=' + str('axdata_core._tdx_wire.hosts' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('provider_host_config=' + str('axdata_source_tdx.host_config' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "count=43" in result.stdout
    assert "first=116.205.183.150:7709" in result.stdout
    assert "provider_wire=True" in result.stdout
    assert "wire_hosts=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "provider_host_config=True" in result.stdout
    assert "request=False" in result.stdout


def test_core_tdx_host_config_fallback_hosts_raise_when_provider_missing() -> None:
    code = (
        "from axdata_core.source_errors import SourceUnavailableError\n"
        "from axdata_core.tdx_plugin_required import TDX_PLUGIN_REQUIRED_MESSAGE\n"
        "from axdata_core.adapters.tdx import host_config\n"
        "try:\n"
        "    host_config._fallback_default_tdx_hosts()\n"
        "except SourceUnavailableError as exc:\n"
        "    print('error=' + str(exc))\n"
        "    print('matches=' + str(str(exc) == TDX_PLUGIN_REQUIRED_MESSAGE))\n"
    )
    result = _core_without_site_subprocess(code)

    assert f"error={TDX_PLUGIN_REQUIRED_MESSAGE}" in result.stdout
    assert "matches=True" in result.stdout


def test_tdx_wire_request_wrappers_do_not_load_wire_runtime() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.wire_requests\n"
        "print('wire_requests=' + str('axdata_core.adapters.tdx.wire_requests' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('wire_transport_socket=' + str('axdata_core._tdx_wire.transport.socket' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "wire_requests=True" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "wire_transport_socket=False" in result.stdout
    assert "request=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_wire_requests_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_source_tdx.wire_requests\n"
        "print('wire_requests=' + str('axdata_source_tdx.wire_requests' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "wire_requests=True" in result.stdout
    assert "request=False" in result.stdout
    assert "wire_client=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_request_import_does_not_load_f10_request_runtime() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.request as request\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('f10_request_before=' + str('axdata_core.adapters.tdx.f10_request' in sys.modules))\n"
        "print('f10_normalize_before=' + str('axdata_core.adapters.tdx.f10_normalize' in sys.modules))\n"
        "print('f10_postprocess_before=' + str('axdata_core.adapters.tdx.f10_postprocess' in sys.modules))\n"
        "print('f10_render_before=' + str('axdata_core.adapters.tdx.f10_render' in sys.modules))\n"
        "print('stats_resource_before=' + str('axdata_core.adapters.tdx.stats_resource' in sys.modules))\n"
        "print('stats_cache_before=' + str('axdata_core.adapters.tdx.stats_cache' in sys.modules))\n"
        "print('stats_models_before=' + str('axdata_core.adapters.tdx.stats_models' in sys.modules))\n"
        "print('host_config_before=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('server_config_before=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('client_factory_before=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('finance_maps_before=' + str('axdata_core.adapters.tdx.finance_maps' in sys.modules))\n"
        "print('adjustment_before=' + str('axdata_core.adapters.tdx.adjustment' in sys.modules))\n"
        "print('price_limit_calendar_before=' + str('axdata_core.adapters.tdx.price_limit_calendar' in sys.modules))\n"
        "print('price_limit_history_before=' + str('axdata_core.adapters.tdx.price_limit_history' in sys.modules))\n"
        "print('tqlex_before=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('wire_client_before=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('wire_transport_socket_before=' + str('axdata_core._tdx_wire.transport.socket' in sys.modules))\n"
        "print('f10_names_before=' + str('axdata_core.tdx_f10_names' in sys.modules))\n"
        "print('f10_models_before=' + str('axdata_core.tdx_f10_models' in sys.modules))\n"
        "print('f10_specs_before=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('compat_before=' + str('_request_f10_tables' in request.__dict__))\n"
        "normalize = request._f10_number\n"
        "print('f10_normalize_after_normalize=' + str('axdata_core.adapters.tdx.f10_normalize' in sys.modules))\n"
        "print('f10_request_after_normalize=' + str('axdata_core.adapters.tdx.f10_request' in sys.modules))\n"
        "postprocess = request._filter_f10_rows\n"
        "render = request._render_f10_body\n"
        "print('f10_normalize_after_helper=' + str('axdata_core.adapters.tdx.f10_normalize' in sys.modules))\n"
        "print('f10_postprocess_after_helper=' + str('axdata_core.adapters.tdx.f10_postprocess' in sys.modules))\n"
        "print('f10_render_after_helper=' + str('axdata_core.adapters.tdx.f10_render' in sys.modules))\n"
        "print('finance_maps_after_f10_helpers=' + str('axdata_core.adapters.tdx.finance_maps' in sys.modules))\n"
        "value = request._request_f10_tables\n"
        "print('f10_request_after=' + str('axdata_core.adapters.tdx.f10_request' in sys.modules))\n"
        "print('tqlex_after=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_models_after=' + str('axdata_core.tdx_f10_models' in sys.modules))\n"
        "print('f10_specs_after=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('finance_maps_after_f10_request=' + str('axdata_core.adapters.tdx.finance_maps' in sys.modules))\n"
        "print('adjustment_after_f10_request=' + str('axdata_core.adapters.tdx.adjustment' in sys.modules))\n"
        "print('price_limit_calendar_after_f10_request=' + str('axdata_core.adapters.tdx.price_limit_calendar' in sys.modules))\n"
        "print('price_limit_history_after_f10_request=' + str('axdata_core.adapters.tdx.price_limit_history' in sys.modules))\n"
        "print('compat_after=' + str('_request_f10_tables' in request.__dict__))\n"
        "price_limit_dates = request._PriceLimitCalendarDates\n"
        "print('price_limit_calendar_after_compat=' + str('axdata_core.adapters.tdx.price_limit_calendar' in sys.modules))\n"
        "print('callable=' + str(callable(value)))\n"
        "print('helpers_callable=' + str(callable(normalize) and callable(postprocess) and callable(render)))\n"
        "print('price_limit_compat=' + str(callable(price_limit_dates)))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "request=True" in result.stdout
    assert "f10_request_before=False" in result.stdout
    assert "f10_normalize_before=False" in result.stdout
    assert "f10_postprocess_before=False" in result.stdout
    assert "f10_render_before=False" in result.stdout
    assert "stats_resource_before=False" in result.stdout
    assert "stats_cache_before=False" in result.stdout
    assert "stats_models_before=False" in result.stdout
    assert "host_config_before=False" in result.stdout
    assert "server_config_before=False" in result.stdout
    assert "client_factory_before=False" in result.stdout
    assert "finance_maps_before=False" in result.stdout
    assert "adjustment_before=False" in result.stdout
    assert "price_limit_calendar_before=False" in result.stdout
    assert "price_limit_history_before=False" in result.stdout
    assert "tqlex_before=False" in result.stdout
    assert "wire_client_before=False" in result.stdout
    assert "wire_transport_socket_before=False" in result.stdout
    assert "f10_names_before=False" in result.stdout
    assert "f10_models_before=False" in result.stdout
    assert "f10_specs_before=False" in result.stdout
    assert "compat_before=False" in result.stdout
    assert "f10_normalize_after_normalize=False" in result.stdout
    assert "f10_request_after_normalize=False" in result.stdout
    assert "f10_request_after=False" in result.stdout
    assert "f10_normalize_after_helper=False" in result.stdout
    assert "f10_postprocess_after_helper=False" in result.stdout
    assert "f10_render_after_helper=False" in result.stdout
    assert "finance_maps_after_f10_helpers=False" in result.stdout
    assert "tqlex_after=False" in result.stdout
    assert "f10_models_after=False" in result.stdout
    assert "f10_specs_after=False" in result.stdout
    assert "finance_maps_after_f10_request=False" in result.stdout
    assert "adjustment_after_f10_request=False" in result.stdout
    assert "price_limit_calendar_after_f10_request=False" in result.stdout
    assert "price_limit_history_after_f10_request=False" in result.stdout
    assert "price_limit_calendar_after_compat=False" in result.stdout
    assert "compat_after=True" in result.stdout
    assert "callable=True" in result.stdout
    assert "helpers_callable=True" in result.stdout
    assert "price_limit_compat=True" in result.stdout


def test_source_error_exports_do_not_load_source_request_gateway() -> None:
    code = (
        "import sys\n"
        "from axdata_core import SourceRequestValidationError\n"
        "from axdata_core.source_errors import SourceRequestValidationError as DirectError\n"
        "print('same=' + str(SourceRequestValidationError is DirectError))\n"
        "print('source_errors=' + str('axdata_core.source_errors' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "same=True" in result.stdout
    assert "source_errors=True" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_validation_helpers_do_not_load_source_request_gateway() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.options\n"
        "import axdata_core.adapters.tdx.request_params\n"
        "import axdata_core.adapters.tdx.codes\n"
        "import axdata_core.adapters.tdx.wire_requests\n"
        "print('options=' + str('axdata_core.adapters.tdx.options' in sys.modules))\n"
        "print('request_params=' + str('axdata_core.adapters.tdx.request_params' in sys.modules))\n"
        "print('codes=' + str('axdata_core.adapters.tdx.codes' in sys.modules))\n"
        "print('wire_requests=' + str('axdata_core.adapters.tdx.wire_requests' in sys.modules))\n"
        "print('source_errors=' + str('axdata_core.source_errors' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "options=True" in result.stdout
    assert "request_params=True" in result.stdout
    assert "codes=True" in result.stdout
    assert "wire_requests=True" in result.stdout
    assert "source_errors=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_options_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.options\n"
        "print('options=' + str('axdata_core.adapters.tdx.options' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('f10_request=' + str('axdata_core.adapters.tdx.f10_request' in sys.modules))\n"
        "print('tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "options=True" in result.stdout
    assert "request=False" in result.stdout
    assert "downloader=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "f10_request=False" in result.stdout
    assert "tqlex=False" in result.stdout
    assert "f10_specs=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_request_dispatch_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.request_dispatch\n"
        "print('request_dispatch=' + str('axdata_core.adapters.tdx.request_dispatch' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "request_dispatch=True" in result.stdout
    assert "request=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "tqlex=False" in result.stdout
    assert "f10_specs=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_request_client_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.request_client\n"
        "print('request_client=' + str('axdata_core.adapters.tdx.request_client' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "request_client=True" in result.stdout
    assert "request=False" in result.stdout
    assert "client_factory=False" in result.stdout
    assert "tqlex=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_execution_utils_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.execution_utils\n"
        "print('execution_utils=' + str('axdata_core.adapters.tdx.execution_utils' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('request_client=' + str('axdata_core.adapters.tdx.request_client' in sys.modules))\n"
        "print('request_dispatch=' + str('axdata_core.adapters.tdx.request_dispatch' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
        "print('tdx_f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "execution_utils=True" in result.stdout
    assert "request=False" in result.stdout
    assert "request_client=False" in result.stdout
    assert "request_dispatch=False" in result.stdout
    assert "source_request=False" in result.stdout
    assert "tdx_f10_specs=False" in result.stdout


def test_tdx_source_execution_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.source_execution\n"
        "print('source_execution=' + str('axdata_core.adapters.tdx.source_execution' in sys.modules))\n"
        "print('server_cache=' + str('axdata_core.adapters.tdx.server_cache' in sys.modules))\n"
        "print('server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "source_execution=True" in result.stdout
    assert "server_cache=False" in result.stdout
    assert "server_config=False" in result.stdout
    assert "request=False" in result.stdout
    assert "downloader=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_realtime_refresh_accepts_injected_client_factory() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx import realtime_refresh\n"
        "calls = []\n"
        "class FakeClient:\n"
        "    def get_quote_refresh(self, cursors):\n"
        "        return {'records': [{'exchange': 'sz', 'code': '000001', 'last_price': 10.15}]}\n"
        "def fake_create_client(**kwargs):\n"
        "    calls.append(kwargs)\n"
        "    return FakeClient()\n"
        "rows = realtime_refresh.request_realtime_refresh_rows(\n"
        "    code='000001.SZ', fields=['instrument_id', 'last_price'], create_client=fake_create_client\n"
        ")\n"
        "print('rows=' + repr(rows))\n"
        "print('calls=' + repr(calls))\n"
        "print('provider_realtime_refresh=' + str('axdata_source_tdx.realtime_refresh' in sys.modules))\n"
        "print('core_realtime_refresh=' + str('axdata_core.adapters.tdx.realtime_refresh' in sys.modules))\n"
        "print('request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('provider_client_factory=' + str('axdata_source_tdx.client_factory' in sys.modules))\n"
        "print('core_client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "rows=[{'instrument_id': '000001.SZ', 'last_price': 10.15}]" in result.stdout
    assert "calls=[{}]" in result.stdout
    assert "provider_realtime_refresh=True" in result.stdout
    assert "core_realtime_refresh=False" in result.stdout
    assert "request=False" in result.stdout
    assert "provider_client_factory=False" in result.stdout
    assert "core_client_factory=False" in result.stdout


def test_core_downloader_registry_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.downloader_registry\n"
        "print('downloader_registry=' + str('axdata_core.downloader_registry' in sys.modules))\n"
        "print('tdx_downloader_profiles=' + str('axdata_core.adapters.tdx.downloader_profiles' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "downloader_registry=True" in result.stdout
    assert "tdx_downloader_profiles=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_core_downloader_registry_factory_maps_are_lightweight() -> None:
    code = (
        "import sys\n"
        "from axdata_core.downloader_registry import (\n"
        "    load_builtin_downloader_adapter_factories,\n"
        "    load_builtin_runtime_source_server_max_factories,\n"
        ")\n"
        "adapter_factories = load_builtin_downloader_adapter_factories()\n"
        "server_max_factories = load_builtin_runtime_source_server_max_factories()\n"
        "print('adapter_keys=' + ','.join(sorted(adapter_factories)))\n"
        "print('server_max_keys=' + ','.join(sorted(server_max_factories)))\n"
        "print('tdx_downloader_profiles=' + str('axdata_core.adapters.tdx.downloader_profiles' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "adapter_keys=\n" in result.stdout
    assert "server_max_keys=\n" in result.stdout
    assert "tdx_downloader_profiles=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_core_source_execution_registry_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "from axdata_core.source_execution_registry import load_builtin_source_execution_enrichers\n"
        "enrichers = load_builtin_source_execution_enrichers()\n"
        "print('enricher_keys=' + ','.join(sorted(enrichers)))\n"
        "print('tdx_module=' + str(enrichers.get('tdx')))\n"
        "print('tdx_ext_module=' + enrichers['tdx_ext'].__module__)\n"
        "print('source_execution_options=' + str('axdata_core.source_execution_options' in sys.modules))\n"
        "print('tdx_source_execution=' + str('axdata_core.adapters.tdx.source_execution' in sys.modules))\n"
        "print('tdx_ext_source_execution=' + str('axdata_core.adapters.tdx_ext.source_execution' in sys.modules))\n"
        "print('tdx_server_cache=' + str('axdata_core.adapters.tdx.server_cache' in sys.modules))\n"
        "print('tdx_ext_server_cache=' + str('axdata_core.adapters.tdx_ext.server_cache' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_ext_request=' + str('axdata_core.adapters.tdx_ext.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert (
        "enricher_keys=axdata.source.tdx_ext,axdata.source.tdx_ext_external,tdx_ext"
    ) in result.stdout
    assert "tdx_module=None" in result.stdout
    assert "tdx_ext_module=axdata_core.adapters.tdx_ext.source_execution_registry" in result.stdout
    assert "source_execution_options=False" in result.stdout
    assert "tdx_source_execution=False" in result.stdout
    assert "tdx_ext_source_execution=False" in result.stdout
    assert "tdx_server_cache=False" in result.stdout
    assert "tdx_ext_server_cache=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_ext_request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_builtin_source_execution_registry_prefers_tdx_provider_package(tmp_path) -> None:
    code = (
        "import sys\n"
        "from axdata_core.source_execution_registry import load_builtin_source_execution_enrichers\n"
        "enrichers = load_builtin_source_execution_enrichers()\n"
        "print('enricher_keys=' + ','.join(sorted(enrichers)))\n"
        "print('tdx_module=' + enrichers['tdx'].__module__)\n"
        "print('tdx_provider_module=' + enrichers['axdata.source.tdx'].__module__)\n"
        "print('tdx_external_module=' + enrichers['axdata.source.tdx_external'].__module__)\n"
        "print('tdx_ext_module=' + enrichers['tdx_ext'].__module__)\n"
        "print('provider_source_execution_registry=' + str('axdata_source_tdx.source_execution_registry' in sys.modules))\n"
        "print('provider_source_execution=' + str('axdata_source_tdx.source_execution' in sys.modules))\n"
        "print('core_tdx_source_execution_registry=' + str('axdata_core.adapters.tdx.source_execution_registry' in sys.modules))\n"
        "print('core_tdx_source_execution=' + str('axdata_core.adapters.tdx.source_execution' in sys.modules))\n"
        "print('tdx_server_cache=' + str('axdata_core.adapters.tdx.server_cache' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert (
        "enricher_keys=axdata.source.tdx,axdata.source.tdx_ext,"
        "axdata.source.tdx_ext_external,axdata.source.tdx_external,tdx,tdx_ext"
    ) in result.stdout
    assert "tdx_module=axdata_source_tdx.source_execution_registry" in result.stdout
    assert "tdx_provider_module=axdata_source_tdx.source_execution_registry" in result.stdout
    assert "tdx_external_module=axdata_source_tdx.source_execution_registry" in result.stdout
    assert "tdx_ext_module=axdata_core.adapters.tdx_ext.source_execution_registry" in result.stdout
    assert "provider_source_execution_registry=True" in result.stdout
    assert "provider_source_execution=False" in result.stdout
    assert "core_tdx_source_execution_registry=False" in result.stdout
    assert "core_tdx_source_execution=False" in result.stdout
    assert "tdx_server_cache=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout


def test_tdx_provider_package_source_execution_injects_cache_roots(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("AXDATA_TDX_STATS_ROOT", raising=False)
    code = (
        "import sys\n"
        "from axdata_core.source_execution_options import execution_options_for_source\n"
        f"options = execution_options_for_source({{}}, data_root={str(tmp_path)!r}, source_code='tdx')\n"
        "print('server_cache_root=' + str(options.get('server_cache_root')).replace('\\\\', '/'))\n"
        "print('stats_cache_root=' + str(options.get('stats_cache_root')).replace('\\\\', '/'))\n"
        "print('provider_source_execution_registry=' + str('axdata_source_tdx.source_execution_registry' in sys.modules))\n"
        "print('provider_source_execution=' + str('axdata_source_tdx.source_execution' in sys.modules))\n"
        "print('core_tdx_source_execution_registry=' + str('axdata_core.adapters.tdx.source_execution_registry' in sys.modules))\n"
        "print('core_tdx_source_execution=' + str('axdata_core.adapters.tdx.source_execution' in sys.modules))\n"
        "print('provider_server_cache=' + str('axdata_source_tdx.server_cache' in sys.modules))\n"
        "print('tdx_server_cache=' + str('axdata_core.adapters.tdx.server_cache' in sys.modules))\n"
        "print('tdx_server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    expected_cache_root = str((tmp_path / "cache" / "tdx_servers").resolve()).replace("\\", "/")
    expected_stats_cache_root = str((tmp_path / "cache" / "tdx" / "stats").resolve()).replace("\\", "/")
    assert f"server_cache_root={expected_cache_root}" in result.stdout
    assert f"stats_cache_root={expected_stats_cache_root}" in result.stdout
    assert "provider_source_execution_registry=True" in result.stdout
    assert "provider_source_execution=True" in result.stdout
    assert "core_tdx_source_execution_registry=False" in result.stdout
    assert "core_tdx_source_execution=False" in result.stdout
    assert "provider_server_cache=True" in result.stdout
    assert "tdx_server_cache=False" in result.stdout
    assert "tdx_server_config=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout


def test_tdx_stats_environment_cache_overrides_data_root(monkeypatch, tmp_path) -> None:
    from axdata_source_tdx.server_cache import tdx_stats_cache_root

    configured = tmp_path / "configured-stats-cache"
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(configured))

    assert tdx_stats_cache_root(tmp_path / "data") == str(configured.resolve())


def test_tdx_source_execution_registry_is_provider_owned_and_lightweight() -> None:
    code = (
        "import sys\n"
        "from axdata_core.adapters.tdx.source_execution_registry import (\n"
        "    tdx_legacy_source_execution_enrichers,\n"
        "    tdx_source_execution_enricher_declarations,\n"
        "    tdx_source_execution_enrichers,\n"
        ")\n"
        "from axdata_core.adapters.tdx_ext.source_execution_registry import (\n"
        "    tdx_ext_legacy_source_execution_enrichers,\n"
        "    tdx_ext_source_execution_enricher_declarations,\n"
        "    tdx_ext_source_execution_enrichers,\n"
        ")\n"
        "tdx_enrichers = tdx_source_execution_enrichers()\n"
        "tdx_ext_enrichers = tdx_ext_source_execution_enrichers()\n"
        "tdx_legacy_enrichers = tdx_legacy_source_execution_enrichers()\n"
        "tdx_ext_legacy_enrichers = tdx_ext_legacy_source_execution_enrichers()\n"
        "tdx_declarations = tdx_source_execution_enricher_declarations()\n"
        "tdx_ext_declarations = tdx_ext_source_execution_enricher_declarations()\n"
        "print('tdx_keys=' + ','.join(sorted(tdx_enrichers)))\n"
        "print('tdx_ext_keys=' + ','.join(sorted(tdx_ext_enrichers)))\n"
        "print('tdx_legacy_keys=' + ','.join(sorted(tdx_legacy_enrichers)))\n"
        "print('tdx_ext_legacy_keys=' + ','.join(sorted(tdx_ext_legacy_enrichers)))\n"
        "print('tdx_declaration_keys=' + ','.join(sorted(tdx_declarations)))\n"
        "print('tdx_ext_declaration_keys=' + ','.join(sorted(tdx_ext_declarations)))\n"
        "print('tdx_source_execution=' + str('axdata_core.adapters.tdx.source_execution' in sys.modules))\n"
        "print('tdx_ext_source_execution=' + str('axdata_core.adapters.tdx_ext.source_execution' in sys.modules))\n"
        "print('tdx_server_cache=' + str('axdata_core.adapters.tdx.server_cache' in sys.modules))\n"
        "print('tdx_ext_server_cache=' + str('axdata_core.adapters.tdx_ext.server_cache' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_ext_request=' + str('axdata_core.adapters.tdx_ext.request' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_ext_pool=' + str('axdata_core.adapters.tdx_ext.pool' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "tdx_keys=axdata.source.tdx,axdata.source.tdx_external" in result.stdout
    assert "tdx_ext_keys=axdata.source.tdx_ext,axdata.source.tdx_ext_external" in result.stdout
    assert "tdx_legacy_keys=tdx" in result.stdout
    assert "tdx_ext_legacy_keys=tdx_ext" in result.stdout
    assert "tdx_declaration_keys=axdata.source.tdx,axdata.source.tdx_external,tdx" in result.stdout
    assert (
        "tdx_ext_declaration_keys=axdata.source.tdx_ext,axdata.source.tdx_ext_external,tdx_ext"
        in result.stdout
    )
    assert "tdx_source_execution=False" in result.stdout
    assert "tdx_ext_source_execution=False" in result.stdout
    assert "tdx_server_cache=False" in result.stdout
    assert "tdx_ext_server_cache=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_ext_request=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_ext_pool=False" in result.stdout


def test_core_source_adapter_factory_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        "import axdata_core.source_adapter_factory\n"
        "print('source_adapter_factory=' + str('axdata_core.source_adapter_factory' in sys.modules))\n"
        "print('tdx_provider_bridge=' + str('axdata_core.adapters.tdx.provider_bridge' in sys.modules))\n"
        "print('tdx_ext_provider_bridge=' + str('axdata_core.adapters.tdx_ext.provider_bridge' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_ext_request=' + str('axdata_core.adapters.tdx_ext.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "source_adapter_factory=True" in result.stdout
    assert "tdx_provider_bridge=False" in result.stdout
    assert "tdx_ext_provider_bridge=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_ext_request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_core_source_adapter_factory_map_is_lightweight() -> None:
    code = (
        "import sys\n"
        "from axdata_core.source_adapter_factory import legacy_source_adapter_factories\n"
        "factories = legacy_source_adapter_factories()\n"
        "print('factory_keys=' + ','.join(sorted(factories)))\n"
        "print('tdx_provider_bridge=' + str('axdata_core.adapters.tdx.provider_bridge' in sys.modules))\n"
        "print('tdx_ext_provider_bridge=' + str('axdata_core.adapters.tdx_ext.provider_bridge' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_ext_request=' + str('axdata_core.adapters.tdx_ext.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = _core_without_site_subprocess(code)

    assert "factory_keys=cls,cninfo,eastmoney,exchange,kph,sina,tdx_ext,tencent" in result.stdout
    assert "tdx_provider_bridge=False" in result.stdout
    assert "tdx_ext_provider_bridge=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_ext_request=False" in result.stdout
    assert "source_request=False" in result.stdout


@pytest.mark.parametrize(
    "module_name",
    [
        "axdata_core.adapters.tdx.adjustment_fetch",
        "axdata_core.adapters.tdx.auction_fetch",
        "axdata_core.adapters.tdx.code_fetch",
        "axdata_core.adapters.tdx.execution_utils",
        "axdata_core.adapters.tdx.finance_fetch",
        "axdata_core.adapters.tdx.f10_executor",
        "axdata_core.adapters.tdx.intraday_fetch",
        "axdata_core.adapters.tdx.kline_helpers",
        "axdata_core.adapters.tdx.limit_ladder_fetch",
        "axdata_core.adapters.tdx.price_limit_fetch",
        "axdata_core.adapters.tdx.quote_fetch",
        "axdata_core.adapters.tdx.rank_fetch",
        "axdata_core.adapters.tdx.series_history",
        "axdata_core.adapters.tdx.status_fetch",
    ],
)
def test_tdx_fetch_helper_imports_are_lightweight(module_name: str) -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"module_name = {module_name!r}\n"
        "importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.tqlex',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core.adapters.tdx.stats_resource',\n"
        "    'axdata_core.adapters.tdx.stats_cache',\n"
        "    'axdata_core.adapters.tdx.stats_models',\n"
        "    'axdata_core.adapters.tdx.finance_maps',\n"
        "]\n"
        "print('module=' + str(module_name in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "module=True" in result.stdout
    assert "loaded=\n" in result.stdout


@pytest.mark.parametrize(
    "module_name",
    [
        "axdata_core.adapters.tdx.adjustment",
        "axdata_core.adapters.tdx.derived_rows",
        "axdata_core.adapters.tdx.downloader_interface_sets",
        "axdata_core.adapters.tdx.downloader_profiles",
        "axdata_core.adapters.tdx.finance_maps",
        "axdata_core.adapters.tdx.interface_sets",
        "axdata_core.adapters.tdx.limit_ladder_topics",
        "axdata_core.adapters.tdx.price_limit_calendar",
        "axdata_core.adapters.tdx.price_limit_history",
        "axdata_core.adapters.tdx.request_limits",
        "axdata_core.adapters.tdx.request_params",
        "axdata_core.adapters.tdx.request_compat",
        "axdata_core.adapters.tdx.request_adapter_runtime",
        "axdata_core.adapters.tdx.request_host_config",
        "axdata_core.adapters.tdx.request_methods",
        "axdata_core.adapters.tdx.request_seams",
        "axdata_core.adapters.tdx.wire_requests",
    ],
)
def test_tdx_rule_helper_imports_are_lightweight(module_name: str) -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"module_name = {module_name!r}\n"
        "importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.tqlex',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "]\n"
        "print('module=' + str(module_name in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "module=True" in result.stdout
    assert "loaded=\n" in result.stdout


@pytest.mark.parametrize(
    "module_name",
    [
        "axdata_core.adapters.tdx.client_factory",
        "axdata_core.adapters.tdx.codes",
        "axdata_core.adapters.tdx.f10_normalize",
        "axdata_core.adapters.tdx.f10_postprocess",
        "axdata_core.adapters.tdx.f10_render",
        "axdata_core.adapters.tdx.finance_normalize",
        "axdata_core.adapters.tdx.host_config",
        "axdata_core.adapters.tdx.normalize_utils",
        "axdata_core.adapters.tdx.options",
        "axdata_core.adapters.tdx.price_limits",
        "axdata_core.adapters.tdx.quote_identity",
        "axdata_core.adapters.tdx.rank_params",
        "axdata_core.adapters.tdx.request_filters",
        "axdata_core.adapters.tdx.security_codes",
        "axdata_core.adapters.tdx.snapshot_normalize",
        "axdata_core.adapters.tdx.time_series_normalize",
    ],
)
def test_tdx_normalization_helper_imports_are_lightweight(module_name: str) -> None:
    code = (
        "import importlib\n"
        "import sys\n"
        f"module_name = {module_name!r}\n"
        "importlib.import_module(module_name)\n"
        "tracked = [\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.tqlex',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core.tdx_server_config',\n"
        "    'axdata_core._tdx_wire.hosts',\n"
        "    'axdata_core.adapters.tdx.finance_maps',\n"
        "]\n"
        "print('module=' + str(module_name in sys.modules))\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "module=True" in result.stdout
    assert "loaded=\n" in result.stdout


def test_tdx_provider_adapter_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.adapter\n"
        "print('provider_adapter=' + str('axdata_source_tdx.adapter' in sys.modules))\n"
        "print('provider_bridge=' + str('axdata_core.adapters.tdx.provider_bridge' in sys.modules))\n"
        "print('core_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
        "print('core_tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_adapter=True" in result.stdout
    assert "provider_bridge=False" in result.stdout
    assert "core_request=False" in result.stdout
    assert "source_request=False" in result.stdout
    assert "core_tqlex=False" in result.stdout
    assert "f10_specs=False" in result.stdout


def test_tdx_provider_package_has_no_core_tdx_adapter_imports() -> None:
    offenders: list[str] = []
    package_src = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx"

    for path in sorted(package_src.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module == "axdata_core.adapters.tdx" or module.startswith(
                    "axdata_core.adapters.tdx."
                ):
                    offenders.append(f"{path.name}:{node.lineno}:{module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name
                    if module == "axdata_core.adapters.tdx" or module.startswith(
                        "axdata_core.adapters.tdx."
                    ):
                        offenders.append(f"{path.name}:{node.lineno}:{module}")

    assert offenders == []


def test_tdx_provider_wire_dependency_is_centralized() -> None:
    offenders: list[str] = []
    package_src = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx"

    for path in sorted(package_src.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("axdata_core._tdx_wire"):
                    offenders.append(f"{path.name}:{node.lineno}:{module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name
                    if module.startswith("axdata_core._tdx_wire"):
                        offenders.append(f"{path.name}:{node.lineno}:{module}")

    assert offenders == []


def test_tdx_provider_core_tdx_runtime_references_are_allowlisted() -> None:
    offenders: list[str] = []
    package_src = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx"
    allowed_core_tdx_request_seams = {
        "request_entrypoints.py",
        "request_host_config.py",
        "request_methods.py",
    }

    for path in sorted(package_src.rglob("*.py")):
        relative_path = path.relative_to(package_src).as_posix()
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if "axdata_core.adapters.tdx" not in line:
                continue
            if (
                relative_path in allowed_core_tdx_request_seams
                and 'sys.modules.get("axdata_core.adapters.tdx.request")' in line
            ):
                continue
            offenders.append(f"{relative_path}:{lineno}:{line.strip()}")

    assert offenders == []


def test_tdx_provider_wire_core_dependency_reference_is_allowlisted() -> None:
    offenders: list[str] = []
    package_src = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx"

    for path in sorted(package_src.rglob("*.py")):
        relative_path = path.relative_to(package_src).as_posix()
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if "axdata_core._tdx_wire" not in line:
                continue
            offenders.append(f"{relative_path}:{lineno}:{line.strip()}")

    assert offenders == []


def test_tdx_provider_wire_stack_has_no_core_wire_dependency() -> None:
    wire_src = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx" / "_tdx_wire"
    offenders: list[str] = []

    for path in sorted(wire_src.rglob("*.py")):
        relative_path = path.relative_to(wire_src).as_posix()
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if "axdata_core._tdx_wire" in line or "axdata_core.adapters.tdx" in line:
                offenders.append(f"{relative_path}:{lineno}:{line.strip()}")

    assert offenders == []


def test_tdx_public_runtime_modules_are_provider_owned_or_compat_only() -> None:
    core_src = REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "adapters" / "tdx"
    package_src = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx"
    core_public = {
        path.name
        for path in core_src.glob("*.py")
        if path.name != "__init__.py" and not path.name.startswith("_")
    }
    provider_public = {
        path.name
        for path in package_src.glob("*.py")
        if path.name != "__init__.py" and not path.name.startswith("_")
    }

    assert sorted(core_public - provider_public) == ["request.py"]
    assert sorted(provider_public - core_public) == [
        "adapter.py",
        "catalog.py",
        "collectors.py",
        "market_dates.py",
        "metadata.py",
        "provider.py",
        "request_adapter.py",
        "request_entrypoints.py",
        "stats_errors.py",
        "stats_validation.py",
        "tdx_f10_catalog.py",
        "tdx_f10_models.py",
        "tdx_f10_names.py",
        "tdx_f10_specs.py",
        "tdx_server_config.py",
        "wire.py",
    ]


def test_core_tdx_wire_provider_first_wrappers_do_not_import_typing_for_private_getattr() -> None:
    core_wire_root = REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "_tdx_wire"
    relative_paths = [
        "__init__.py",
        "api/__init__.py",
        "models/__init__.py",
        "hosts.py",
        "protocol/__init__.py",
        "protocol/constants.py",
        "protocol/frame.py",
        "protocol/unit.py",
        "protocol/commands/__init__.py",
        "protocol/commands/codec.py",
        "protocol/commands/lookup.py",
        "protocol/commands/price_limits.py",
        "protocol/commands/quotes.py",
        "protocol/commands/registry.py",
        "protocol/commands/security.py",
        "protocol/commands/session.py",
        "transport/__init__.py",
    ]

    for relative_path in relative_paths:
        text = (core_wire_root / relative_path).read_text(encoding="utf-8")
        assert "def __getattr__(name: str):" in text
        assert "from typing import Any" not in text
        assert "def __getattr__(name: str) -> Any" not in text


def test_core_tdx_provider_probes_are_cached() -> None:
    offenders: list[str] = []
    package_src = REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "adapters" / "tdx"

    for path in sorted(package_src.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        if "axdata_source_tdx" not in text:
            continue
        if "_IMPLEMENTATION" in text:
            continue
        offenders.append(path.name)

    assert offenders == []


def test_legacy_core_tdx_request_module_stays_thin_compat_shell() -> None:
    request_path = REPO_ROOT / "libs" / "axdata_core" / "axdata_core" / "adapters" / "tdx" / "request.py"
    tree = ast.parse(request_path.read_text(encoding="utf-8"), filename=str(request_path))

    import_from_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    function_names = [
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    ]
    class_names = [
        node.name
        for node in tree.body
        if isinstance(node, ast.ClassDef)
    ]

    assert import_from_modules == {
        "__future__",
        "typing",
        "axdata_core.adapters.tdx.request_compat",
    }
    assert function_names == ["__getattr__"]
    assert class_names == []


def test_tdx_provider_request_adapter_has_no_core_tdx_runtime_references() -> None:
    offenders: list[str] = []
    request_adapter_path = TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx" / "request_adapter.py"

    for lineno, line in enumerate(request_adapter_path.read_text(encoding="utf-8").splitlines(), start=1):
        if "axdata_core.adapters.tdx" in line or "axdata_core._tdx_wire" in line:
            offenders.append(f"{request_adapter_path.name}:{lineno}:{line.strip()}")

    assert offenders == []


def test_tdx_provider_create_adapter_does_not_load_core_request() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx.provider import provider\n"
        "adapter = provider.create_adapter(options={'client': object()})\n"
        "print('provider_adapter=' + str('axdata_source_tdx.adapter' in sys.modules))\n"
        "print('provider_bridge=' + str('axdata_core.adapters.tdx.provider_bridge' in sys.modules))\n"
        "print('core_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
        "print('core_tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
        "print('has_adapter=' + str(adapter is not None))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_adapter=True" in result.stdout
    assert "provider_bridge=False" in result.stdout
    assert "core_request=False" in result.stdout
    assert "source_request=False" in result.stdout
    assert "core_tqlex=False" in result.stdout
    assert "has_adapter=True" in result.stdout


def test_tdx_provider_request_adapter_import_is_lightweight() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "import axdata_source_tdx.request_adapter\n"
        "print('provider_request_adapter=' + str('axdata_source_tdx.request_adapter' in sys.modules))\n"
        "print('provider_methods=' + str('axdata_source_tdx.request_methods' in sys.modules))\n"
        "print('provider_runtime=' + str('axdata_source_tdx.request_adapter_runtime' in sys.modules))\n"
        "print('core_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
        "print('core_tqlex=' + str('axdata_core.adapters.tdx.tqlex' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_request_adapter=True" in result.stdout
    assert "provider_methods=False" in result.stdout
    assert "provider_runtime=False" in result.stdout
    assert "core_request=False" in result.stdout
    assert "source_request=False" in result.stdout
    assert "core_tqlex=False" in result.stdout


def test_tdx_provider_bridge_create_uses_provider_request_adapter() -> None:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TDX_PACKAGE_ROOT / 'src')!r})\n"
        "from axdata_source_tdx.provider_bridge import create_tdx_request_adapter\n"
        "client = object()\n"
        "adapter = create_tdx_request_adapter({'client': client, 'server_cache_root': 'cache'})\n"
        "print('adapter_module=' + adapter.__class__.__module__)\n"
        "print('client_same=' + str(adapter._client is client))\n"
        "print('options=' + repr(adapter._options))\n"
        "print('provider_request_adapter=' + str('axdata_source_tdx.request_adapter' in sys.modules))\n"
        "print('core_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "adapter_module=axdata_source_tdx.request_adapter" in result.stdout
    assert "client_same=True" in result.stdout
    assert "options={'server_cache_root': 'cache'}" in result.stdout
    assert "provider_request_adapter=True" in result.stdout
    assert "core_request=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_provider_package_manifest_matches_provider(monkeypatch, tmp_path, capsys) -> None:
    install_root = _install_tdx_provider(tmp_path)
    manifest_path = install_root / "axdata_source_tdx" / "axdata-provider.json"

    monkeypatch.syspath_prepend(str(install_root))
    _clear_tdx_provider_modules()

    assert (
        main(
            [
                "plugin",
                "check",
                "--provider",
                "axdata_source_tdx.provider:provider",
                "--manifest",
                str(manifest_path),
            ]
        )
        == 0
    )
    assert f"OK {TDX_PROVIDER_ID} interfaces=90 downloaders=10 collectors=0" in capsys.readouterr().out
    assert "axdata_source_tdx.provider" in sys.modules
    assert "axdata_source_tdx.adapter" not in sys.modules


def test_tdx_provider_plugin_check_does_not_load_f10_runtime_specs(tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    manifest_path = install_root / "axdata_source_tdx" / "axdata-provider.json"
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(install_root)!r})\n"
        "from axdata_core.cli import main\n"
        "exit_code = main([\n"
        "    'plugin',\n"
        "    'check',\n"
        "    '--provider',\n"
        "    'axdata_source_tdx.provider:provider',\n"
        "    '--manifest',\n"
        f"    {str(manifest_path)!r},\n"
        "])\n"
        "print('exit_code=' + str(exit_code))\n"
        "tracked = [\n"
        "    'axdata_core.sources.tdx.catalog',\n"
        "    'axdata_core.tdx_f10_catalog',\n"
        "    'axdata_core.tdx_f10_specs',\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core.adapters.tdx.f10_request',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_source_tdx.adapter',\n"
        "]\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "exit_code=0" in result.stdout
    assert "axdata_core.sources.tdx.catalog" not in result.stdout
    assert "axdata_core.tdx_f10_catalog" not in result.stdout
    assert "axdata_core.tdx_f10_specs" not in result.stdout
    assert "axdata_core.adapters.tdx.request" not in result.stdout
    assert "axdata_core.adapters.tdx.f10_request" not in result.stdout
    assert "axdata_core.adapters.tdx.downloader" not in result.stdout
    assert "axdata_source_tdx.adapter" not in result.stdout


def test_tdx_provider_package_discovery_defaults_enabled(monkeypatch, tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    data_root = tmp_path / "data"

    monkeypatch.syspath_prepend(str(install_root))
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    _clear_tdx_provider_modules()

    registry = build_builtin_provider_registry(data_root=data_root)
    snapshot = registry.snapshot()

    provider = snapshot.providers[TDX_PROVIDER_ID]
    assert provider.status == "enabled"
    assert provider.enabled is True
    assert provider.provider is None
    assert BUILTIN_TDX_PROVIDER_ID not in snapshot.providers
    assert snapshot.interfaces[TDX_INTERFACE_NAME].provider_id == TDX_PROVIDER_ID
    assert "axdata_source_tdx.provider" not in sys.modules


def test_external_tdx_request_respects_explicit_disable(monkeypatch, tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    data_root = tmp_path / "data"

    monkeypatch.syspath_prepend(str(install_root))
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    disable_provider(TDX_PROVIDER_ID, data_root=data_root)
    _clear_tdx_provider_modules()

    with pytest.raises(SourceUnavailableError, match=TDX_PLUGIN_REQUIRED_MESSAGE):
        request_interface(
            TDX_INTERFACE_NAME,
            params={"scope": "all"},
            data_root=data_root,
        )
    assert "axdata_source_tdx.provider" not in sys.modules


def test_external_tdx_routes_when_enabled(monkeypatch, tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    data_root = tmp_path / "data"

    monkeypatch.syspath_prepend(str(install_root))
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    enable_provider(TDX_PROVIDER_ID, data_root=data_root)
    _clear_tdx_provider_modules()

    registry = build_builtin_provider_registry(data_root=data_root)
    snapshot = registry.snapshot()

    assert BUILTIN_TDX_PROVIDER_ID not in snapshot.providers
    assert snapshot.interfaces[TDX_INTERFACE_NAME].provider_id == TDX_PROVIDER_ID
    assert snapshot.providers[TDX_PROVIDER_ID].status == "enabled"
    assert snapshot.providers[TDX_PROVIDER_ID].effective_trust_level == "community"
    assert "axdata_source_tdx.provider" not in sys.modules


def test_external_tdx_routes_when_builtin_tdx_disabled(monkeypatch, tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    data_root = tmp_path / "data"

    monkeypatch.syspath_prepend(str(install_root))
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    enable_provider(TDX_PROVIDER_ID, data_root=data_root)
    disable_provider(BUILTIN_TDX_PROVIDER_ID, data_root=data_root)
    _clear_tdx_provider_modules()
    sys.modules.pop("axdata_core.adapters.tdx.provider_bridge", None)

    registry = build_builtin_provider_registry(data_root=data_root)
    snapshot = registry.snapshot()

    assert BUILTIN_TDX_PROVIDER_ID not in snapshot.providers
    assert snapshot.providers[TDX_PROVIDER_ID].status == "enabled"
    assert snapshot.interfaces[TDX_INTERFACE_NAME].provider_id == TDX_PROVIDER_ID
    discovered_downloaders = {
        downloader.interface_name: downloader
        for downloader in snapshot.providers[TDX_PROVIDER_ID].manifest.downloaders
    }
    assert discovered_downloaders["stock_daily_share_tdx"].resource_group == "tdx.quote"
    assert discovered_downloaders["stock_daily_share_tdx"].default_limits["max_connections_total"] == 8
    assert discovered_downloaders["stock_limit_ladder_tdx"].resource_group == "tdx.f10"
    assert "axdata_source_tdx.provider" not in sys.modules

    result = request_interface(
        TDX_INTERFACE_NAME,
        params={"scope": "all", "code": "000001.SZ"},
        fields=["instrument_id", "name", "market"],
        options={"client": FakeTdxClient()},
        data_root=data_root,
    )

    assert result.records == [
        {"instrument_id": "000001.SZ", "name": "平安银行", "market": "主板"}
    ]
    assert result.meta["source"] == "tdx"
    assert result.meta["provider_id"] == TDX_PROVIDER_ID
    assert result.meta["tdx_connected_host"] is None
    assert "axdata_source_tdx.provider" in sys.modules
    assert "axdata_source_tdx.provider_bridge" in sys.modules
    assert "axdata_core.adapters.tdx.provider_bridge" not in sys.modules
    assert "axdata_source_tdx.request_adapter" in sys.modules


def test_external_tdx_request_route_uses_provider_adapter_without_core_request(tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    data_root = tmp_path / "data"
    code = (
        "from pathlib import Path\n"
        "from types import SimpleNamespace\n"
        "import sys\n"
        "from axdata_core.plugin_config import disable_provider, enable_provider\n"
        "from axdata_core.source_request import request_interface\n"
        f"sys.path.insert(0, {str(install_root)!r})\n"
        f"data_root = Path({str(data_root)!r})\n"
        f"enable_provider({TDX_PROVIDER_ID!r}, data_root=data_root)\n"
        f"disable_provider({BUILTIN_TDX_PROVIDER_ID!r}, data_root=data_root)\n"
        "class FakeTdxClient:\n"
        "    def connect(self):\n"
        "        pass\n"
        "    def get_codes(self, market, *, start=0, limit=None):\n"
        "        if market != 'sz' or start:\n"
        "            return []\n"
        "        return [SimpleNamespace(\n"
        "            full_code='sz000001', code='000001', exchange='sz', name='平安银行',\n"
        "            category='a_share', category_reason='test', board='szse_main_board',\n"
        "            board_reason='test', decimal=2, multiple=100,\n"
        "            previous_close_price=12.34, volume_ratio_base=1.0,\n"
        "        )]\n"
        "result = request_interface(\n"
        f"    {TDX_INTERFACE_NAME!r},\n"
        "    params={'scope': 'all', 'code': '000001.SZ'},\n"
        "    fields=['instrument_id', 'name', 'market'],\n"
        "    options={'client': FakeTdxClient()},\n"
        "    data_root=data_root,\n"
        ")\n"
        "print('records=' + repr(result.records))\n"
        "print('provider_id=' + result.meta['provider_id'])\n"
        "print('provider_request_adapter=' + str('axdata_source_tdx.request_adapter' in sys.modules))\n"
        "print('core_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('core_bridge=' + str('axdata_core.adapters.tdx.provider_bridge' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "records=[{'instrument_id': '000001.SZ', 'name': '平安银行', 'market': '主板'}]" in result.stdout
    assert f"provider_id={TDX_PROVIDER_ID}" in result.stdout
    assert "provider_request_adapter=True" in result.stdout
    assert "core_request=False" in result.stdout
    assert "core_bridge=False" in result.stdout


def test_external_tdx_routes_f10_when_builtin_tdx_disabled(monkeypatch, tmp_path) -> None:
    install_root = _install_tdx_provider(tmp_path)
    data_root = tmp_path / "data"

    monkeypatch.syspath_prepend(str(install_root))
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    enable_provider(TDX_PROVIDER_ID, data_root=data_root)
    disable_provider(BUILTIN_TDX_PROVIDER_ID, data_root=data_root)
    _clear_tdx_provider_modules()
    sys.modules.pop("axdata_core.adapters.tdx.provider_bridge", None)

    registry = build_builtin_provider_registry(data_root=data_root)
    snapshot = registry.snapshot()

    assert snapshot.interfaces["stock_topic_exposure_tdx"].provider_id == TDX_PROVIDER_ID
    assert "axdata_source_tdx.provider" not in sys.modules

    result = request_interface(
        "stock_topic_exposure_tdx",
        params={"code": "000034.SZ"},
        fields=["instrument_id", "symbol", "topic_type", "topic_name"],
        options={"client": EchoTqlexClient()},
        data_root=data_root,
    )

    assert result.records == [
        {
            "instrument_id": "000034.SZ",
            "symbol": "000034",
            "topic_type": "theme",
            "topic_name": "1",
        }
    ]
    assert result.meta["source"] == "tdx"
    assert result.meta["provider_id"] == TDX_PROVIDER_ID
    assert result.meta["tdx_protocol_family"] == "7615"
    assert result.meta["tdx_requested_interface"] == "stock_topic_exposure_tdx"
    assert "axdata_source_tdx.provider" in sys.modules


def test_tdx_provider_package_pyproject_declares_entry_point() -> None:
    import tomllib

    pyproject = tomllib.loads((TDX_PACKAGE_ROOT / "pyproject.toml").read_text())

    assert pyproject["project"]["name"] == "axdata-source-tdx"
    assert "axdata-core>=0.1.0" in set(pyproject["project"]["dependencies"])
    assert (
        pyproject["project"]["entry-points"]["axdata.providers"]["tdx"]
        == "axdata_source_tdx.provider:provider"
    )
    package_data = set(pyproject["tool"]["setuptools"]["package-data"]["axdata_source_tdx"])
    assert "axdata-provider.json" in package_data
    assert "_tdx_wire/py.typed" in package_data


def test_tdx_provider_manifest_matches_runtime_profile_and_collector_declarations() -> None:
    sys.path.insert(0, str(TDX_PACKAGE_ROOT / "src"))

    from axdata_core.downloaders import ConcurrencyProfile, DownloaderProfile
    from axdata_source_tdx.collectors import (
        TDX_COLLECTOR_INTERFACES,
        TDX_LEGACY_COLLECTOR_INTERFACES,
        tdx_collector_manifest,
    )
    from axdata_source_tdx.downloader_profiles import tdx_downloader_profiles
    from axdata_source_tdx.provider import provider

    manifest_downloaders = {downloader.interface_name: downloader for downloader in provider.downloader_profiles()}
    runtime_profiles = tdx_downloader_profiles(ConcurrencyProfile, DownloaderProfile)
    collectors = {collector.name: collector for collector in provider.collectors()}

    assert set(manifest_downloaders) == set(runtime_profiles)
    assert set(TDX_COLLECTOR_INTERFACES) < set(manifest_downloaders)
    assert set(manifest_downloaders) - set(TDX_COLLECTOR_INTERFACES) == {
        "stock_capital_changes_tdx",
        "stock_adj_factor_tdx",
    }
    assert set(collectors) == {f"tdx.{interface_name}.snapshot" for interface_name in TDX_LEGACY_COLLECTOR_INTERFACES}
    assert manifest_downloaders["stock_kline_daily_tdx"].default_options["params"] == runtime_profiles[
        "stock_kline_daily_tdx"
    ].default_params
    assert manifest_downloaders["stock_adj_factor_tdx"].output["output_layer"] == runtime_profiles[
        "stock_adj_factor_tdx"
    ].output_layer
    assert manifest_downloaders["stock_adj_factor_tdx"].output["write_mode"] == runtime_profiles[
        "stock_adj_factor_tdx"
    ].write_mode
    assert manifest_downloaders["stock_kline_daily_tdx"].output["required_columns"] == [
        "instrument_id",
        "trade_time",
        "period",
    ]
    assert manifest_downloaders["stock_kline_daily_tdx"].output["field_mappings"] == {
        "instrument_id": "ts_code",
        "trade_time": "trade_date",
        "volume": "vol",
    }
    assert manifest_downloaders["stock_adj_factor_tdx"].output["numeric_positive_columns"] == ["adj_factor"]
    assert collectors == {}

    independent_manifest = tdx_collector_manifest()
    independent_collectors = {collector.name: collector for collector in independent_manifest.collectors}
    assert independent_manifest.provider is None
    assert independent_manifest.plugin is not None
    assert independent_manifest.plugin.plugin_id == TDX_COLLECTOR_PLUGIN_ID
    assert set(independent_collectors) == set(TDX_BASE_COLLECTOR_DATASET_IDS)
    stock_codes = independent_collectors["tdx.stock_codes_tdx.snapshot"]
    assert stock_codes.collector_plugin_id == TDX_COLLECTOR_PLUGIN_ID
    assert stock_codes.dataset_id == "tdx.stock_codes"
    assert stock_codes.runner_entry == TDX_COLLECTOR_RUNNER_ENTRY
    assert stock_codes.downloader_profile is None
    assert stock_codes.interfaces == ()
    assert stock_codes.required_interfaces == ()
    assert stock_codes.is_legacy is False
    for collector_id, dataset_id in TDX_BASE_COLLECTOR_DATASET_IDS.items():
        collector = independent_collectors[collector_id]
        assert collector.collector_plugin_id == TDX_COLLECTOR_PLUGIN_ID
        assert collector.dataset_id == dataset_id
        assert collector.runner_entry == TDX_COLLECTOR_RUNNER_ENTRY
        assert collector.downloader_profile is None
        assert collector.interfaces == ()
        assert collector.required_interfaces == ()
        assert collector.is_legacy is False
    daily = independent_collectors["tdx.stock_kline_daily_tdx.snapshot"]
    assert daily.dataset_id == "tdx.stock_daily"
    assert daily.output["layer"] == "core"
    assert daily.output["partition_by"] == ["trade_date"]
    assert daily.quality["datetime_field"] == "trade_time"
    assert daily.quality["field_mappings"] == {
        "instrument_id": "ts_code",
        "trade_time": "trade_date",
        "volume": "vol",
    }

def test_tdx_provider_package_builds_wheel_with_manifest_and_entry_point(tmp_path) -> None:
    wheel_path = _build_tdx_wheel(tmp_path)

    with ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        assert "axdata_source_tdx/axdata-provider.json" in names
        assert "axdata_source_tdx/client_factory.py" in names
        assert "axdata_source_tdx/downloader.py" in names
        assert "axdata_source_tdx/collectors.py" in names
        assert "axdata_source_tdx/codes.py" in names
        assert "axdata_source_tdx/downloader_interface_sets.py" in names
        assert "axdata_source_tdx/downloader_profiles.py" in names
        assert "axdata_source_tdx/downloader_registry.py" in names
        assert "axdata_source_tdx/execution_utils.py" in names
        assert "axdata_source_tdx/normalize_utils.py" in names
        assert "axdata_source_tdx/host_config.py" in names
        assert "axdata_source_tdx/tdx_server_config.py" in names
        assert "axdata_source_tdx/tdx_f10_names.py" in names
        assert "axdata_source_tdx/tdx_f10_models.py" in names
        assert "axdata_source_tdx/tdx_f10_catalog.py" in names
        assert "axdata_source_tdx/tdx_f10_specs.py" in names
        assert "axdata_source_tdx/adjustment.py" in names
        assert "axdata_source_tdx/adjustment_fetch.py" in names
        assert "axdata_source_tdx/derived_rows.py" in names
        assert "axdata_source_tdx/f10_executor.py" in names
        assert "axdata_source_tdx/f10_normalize.py" in names
        assert "axdata_source_tdx/f10_params.py" in names
        assert "axdata_source_tdx/f10_postprocess.py" in names
        assert "axdata_source_tdx/f10_request.py" in names
        assert "axdata_source_tdx/f10_render.py" in names
        assert "axdata_source_tdx/tqlex.py" in names
        assert "axdata_source_tdx/auction_fetch.py" in names
        assert "axdata_source_tdx/code_fetch.py" in names
        assert "axdata_source_tdx/finance_fetch.py" in names
        assert "axdata_source_tdx/finance_maps.py" in names
        assert "axdata_source_tdx/finance_normalize.py" in names
        assert "axdata_source_tdx/metadata.py" in names
        assert "axdata_source_tdx/options.py" in names
        assert "axdata_source_tdx/provider_bridge.py" in names
        assert "axdata_source_tdx/request_adapter.py" in names
        assert "axdata_source_tdx/price_limit_fetch.py" in names
        assert "axdata_source_tdx/price_limits.py" in names
        assert "axdata_source_tdx/price_limit_calendar.py" in names
        assert "axdata_source_tdx/price_limit_history.py" in names
        assert "axdata_source_tdx/quote_fetch.py" in names
        assert "axdata_source_tdx/quote_identity.py" in names
        assert "axdata_source_tdx/snapshot_normalize.py" in names
        assert "axdata_source_tdx/time_series_normalize.py" in names
        assert "axdata_source_tdx/intraday_fetch.py" in names
        assert "axdata_source_tdx/interface_sets.py" in names
        assert "axdata_source_tdx/kline_helpers.py" in names
        assert "axdata_source_tdx/wire_requests.py" in names
        assert "axdata_source_tdx/wire.py" in names
        assert "axdata_source_tdx/_tdx_wire/__init__.py" in names
        assert "axdata_source_tdx/_tdx_wire/_code_utils.py" in names
        assert "axdata_source_tdx/_tdx_wire/_command_codec.py" in names
        assert "axdata_source_tdx/_tdx_wire/_command_lookup.py" in names
        assert "axdata_source_tdx/_tdx_wire/_host_probe.py" in names
        assert "axdata_source_tdx/_tdx_wire/_time_utils.py" in names
        assert "axdata_source_tdx/_tdx_wire/client.py" in names
        assert "axdata_source_tdx/_tdx_wire/hosts.py" in names
        assert "axdata_source_tdx/_tdx_wire/py.typed" in names
        assert "axdata_source_tdx/_tdx_wire/models/quote.py" in names
        assert "axdata_source_tdx/_tdx_wire/protocol/commands/codec.py" in names
        assert "axdata_source_tdx/_tdx_wire/protocol/commands/lookup.py" in names
        assert "axdata_source_tdx/_tdx_wire/protocol/commands/quotes.py" in names
        assert "axdata_source_tdx/_tdx_wire/transport/socket.py" in names
        assert "axdata_source_tdx/limit_ladder_topics.py" in names
        assert "axdata_source_tdx/realtime_refresh.py" in names
        assert "axdata_source_tdx/request_adapter_runtime.py" in names
        assert "axdata_source_tdx/request_client.py" in names
        assert "axdata_source_tdx/request_compat.py" in names
        assert "axdata_source_tdx/request_dispatch.py" in names
        assert "axdata_source_tdx/request_entrypoints.py" in names
        assert "axdata_source_tdx/request_host_config.py" in names
        assert "axdata_source_tdx/request_methods.py" in names
        assert "axdata_source_tdx/request_seams.py" in names
        assert "axdata_source_tdx/rank_fetch.py" in names
        assert "axdata_source_tdx/rank_params.py" in names
        assert "axdata_source_tdx/request_filters.py" in names
        assert "axdata_source_tdx/request_limits.py" in names
        assert "axdata_source_tdx/request_params.py" in names
        assert "axdata_source_tdx/security_codes.py" in names
        assert "axdata_source_tdx/series_history.py" in names
        assert "axdata_source_tdx/limit_ladder_fetch.py" in names
        assert "axdata_source_tdx/status_fetch.py" in names
        assert "axdata_source_tdx/server_cache.py" in names
        assert "axdata_source_tdx/source_adapter_registry.py" in names
        assert "axdata_source_tdx/source_execution.py" in names
        assert "axdata_source_tdx/source_execution_registry.py" in names
        assert "axdata_source_tdx/stats_cache.py" in names
        assert "axdata_source_tdx/stats_errors.py" in names
        assert "axdata_source_tdx/stats_models.py" in names
        assert "axdata_source_tdx/stats_resource.py" in names
        assert "axdata_source_tdx/stats_validation.py" in names
        assert "axdata_source_tdx/market_dates.py" in names
        assert "axdata_source_tdx/resources/finance_maps/__init__.py" in names
        assert "axdata_source_tdx/resources/finance_maps/incon.dat" in names
        assert "axdata_source_tdx/resources/finance_maps/tdxhy.cfg" in names
        assert "axdata_source_tdx/resources/finance_maps/tdxzs.cfg" in names
        assert "axdata_source_tdx/resources/tdx_quote_servers.json" in names
        assert "axdata_source_tdx/resources/tdx_extended_servers.json" in names
        assert "axdata_source_tdx-0.1.0.dist-info/entry_points.txt" in names
        entry_points = wheel.read(
            "axdata_source_tdx-0.1.0.dist-info/entry_points.txt"
        ).decode("utf-8")
        manifest = ProviderManifest.from_dict(
            json.loads(wheel.read("axdata_source_tdx/axdata-provider.json"))
        )

    assert "tdx = axdata_source_tdx.provider:provider" in entry_points
    assert manifest.provider.provider_id == TDX_PROVIDER_ID
    assert manifest.provider.source_name_zh == "通达信"
    assert TDX_INTERFACE_NAME in {interface.name for interface in manifest.interfaces}
    interfaces = {interface.name: interface for interface in manifest.interfaces}
    assert interfaces[TDX_INTERFACE_NAME].source_name_zh == "通达信"
    assert interfaces[TDX_INTERFACE_NAME].menu_path == ("通达信", "股票数据", "基础数据")
    assert interfaces["index_kline_tdx"].menu_path == ("通达信", "指数数据", "行情数据")
    assert interfaces["etf_auction_process_tdx"].menu_path == ("通达信", "ETF数据", "竞价数据")
    category_reference = interfaces["stock_capital_changes_tdx"].reference_sections[0]
    assert category_reference.title == "类别码参考"
    assert category_reference.columns == ("category", "名称", "复权因子关系", "c1", "c2", "c3", "c4")
    assert category_reference.rows[0] == ("1", "除权除息", "直接参与", "现金分红", "配股价", "送转股", "配股")
    expected_reference_titles = {
        "stock_realtime_rank_tdx": ("榜单范围对照", "排序字段对照", "过滤条件对照"),
        "stock_shortline_indicators_tdx": ("核心计算口径", "数据来源"),
        "stock_limit_ladder_tdx": ("涨停状态", "题材排序"),
        "stock_theme_strength_rank_tdx": ("排序口径",),
        "stock_daily_price_limit_tdx": ("价格来源",),
        "stock_finance_profile_tdx": ("映射规则", "单位说明"),
        "stock_finance_summary_tdx": ("单位说明",),
        "stock_balance_summary_tdx": ("单位说明",),
        "stock_profit_cashflow_summary_tdx": ("单位说明",),
        "stock_share_capital_tdx": ("单位说明",),
    }
    for interface_name, titles in expected_reference_titles.items():
        assert tuple(section.title for section in interfaces[interface_name].reference_sections) == titles

    realtime_rank_references = interfaces["stock_realtime_rank_tdx"].reference_sections
    assert len(realtime_rank_references[1].rows) == 28
    assert realtime_rank_references[2].rows[2] == ("排除 ST", "exclude_st", "从结果中排除 ST 股票")
    downloaders = {downloader.interface_name: downloader for downloader in manifest.downloaders}
    assert set(downloaders) == {
        "stock_codes_tdx",
        "stock_suspensions_tdx",
        "stock_st_list_tdx",
        "stock_daily_share_tdx",
        "stock_daily_price_limit_tdx",
        "stock_capital_changes_tdx",
        "stock_kline_daily_tdx",
        "stock_adj_factor_tdx",
        "stock_limit_ladder_tdx",
        "stock_theme_strength_rank_tdx",
    }
    collectors = {collector.name: collector for collector in manifest.collectors}
    assert set(collectors) == TDX_LEGACY_COLLECTOR_NAMES
    assert downloaders["stock_codes_tdx"].resource_group == "tdx.quote"
    assert downloaders["stock_codes_tdx"].default_options["connections_per_server"] == 3
    assert downloaders["stock_suspensions_tdx"].default_options["source_server_count"] == 4
    assert downloaders["stock_suspensions_tdx"].default_options["batch_size"] == 80
    assert downloaders["stock_daily_share_tdx"].default_limits["max_connections_total"] == 8
    assert downloaders["stock_daily_price_limit_tdx"].default_options["max_concurrent_tasks"] == 8
    assert downloaders["stock_capital_changes_tdx"].default_limits["max_connections_total"] == 16
    assert downloaders["stock_kline_daily_tdx"].default_options["params"] == {
        "code": "000001.SZ",
        "count": 800,
        "adjust": "none",
    }
    assert downloaders["stock_kline_daily_tdx"].output["output_layer"] == "core"
    assert downloaders["stock_adj_factor_tdx"].default_options["params"] == {
        "code": "000001.SZ",
        "adjust": "qfq",
    }
    assert downloaders["stock_adj_factor_tdx"].output["output_layer"] == "core"
    assert downloaders["stock_adj_factor_tdx"].output["write_mode"] == "upsert_by_key"
    assert downloaders["stock_limit_ladder_tdx"].resource_group == "tdx.f10"
    assert downloaders["stock_theme_strength_rank_tdx"].resource_group == "tdx.f10"


def test_tdx_provider_installed_from_wheel_is_discovered_and_can_route(
    monkeypatch,
    tmp_path,
) -> None:
    wheel_path = _build_tdx_wheel(tmp_path)
    install_root = tmp_path / "installed"
    data_root = tmp_path / "data"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(install_root),
            str(wheel_path),
        ],
        check=True,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    monkeypatch.syspath_prepend(str(install_root))
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    enable_provider(TDX_PROVIDER_ID, data_root=data_root)
    disable_provider(BUILTIN_TDX_PROVIDER_ID, data_root=data_root)
    _clear_tdx_provider_modules()

    registry = build_builtin_provider_registry(data_root=data_root)
    snapshot = registry.snapshot()

    assert snapshot.providers[TDX_PROVIDER_ID].status == "enabled"
    assert snapshot.interfaces[TDX_INTERFACE_NAME].provider_id == TDX_PROVIDER_ID
    assert "axdata_source_tdx.provider" not in sys.modules

    result = request_interface(
        TDX_INTERFACE_NAME,
        params={"scope": "all", "code": "000001.SZ"},
        fields=["instrument_id", "name"],
        options={"client": FakeTdxClient()},
        data_root=data_root,
    )

    assert result.records == [{"instrument_id": "000001.SZ", "name": "平安银行"}]
    assert result.meta["provider_id"] == TDX_PROVIDER_ID
    assert "axdata_source_tdx.provider" in sys.modules
    assert "axdata_source_tdx.provider_bridge" in sys.modules
    assert "axdata_core.adapters.tdx.provider_bridge" not in sys.modules
    assert "axdata_core.adapters.tdx.request" in sys.modules

    first_cwd = tmp_path / "wheel-cwd-one"
    second_cwd = tmp_path / "wheel-cwd-two"
    local_app_data = tmp_path / "local-app-data"
    first_cwd.mkdir()
    second_cwd.mkdir()
    cache_code = (
        "from axdata_source_tdx.stats_cache import default_tdx_stats_cache_root\n"
        "print(default_tdx_stats_cache_root())\n"
    )
    cache_env = {
        key: value
        for key, value in os.environ.items()
        if key != "AXDATA_TDX_STATS_ROOT"
    }
    cache_env.update(
        {
            "LOCALAPPDATA": str(local_app_data),
            "PYTHONPATH": os.pathsep.join(
                [str(install_root), str(REPO_ROOT / "libs" / "axdata_core")]
            ),
        }
    )
    cache_paths = [
        subprocess.run(
            [sys.executable, "-c", cache_code],
            check=True,
            cwd=cwd,
            env=cache_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()
        for cwd in (first_cwd, second_cwd)
    ]
    expected_cache = str(_expected_default_tdx_stats_cache_root(local_app_data))
    assert cache_paths == [expected_cache, expected_cache]


def _build_tdx_wheel(tmp_path: Path) -> Path:
    wheel_dir = tmp_path / "wheelhouse"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "-w",
            str(wheel_dir),
            str(TDX_PACKAGE_ROOT),
        ],
        check=True,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    return next(wheel_dir.glob("axdata_source_tdx-0.1.0-*.whl"))


def _install_tdx_provider(tmp_path: Path) -> Path:
    install_root = tmp_path / "site-packages"
    package_root = install_root / "axdata_source_tdx"
    dist_info = install_root / "axdata_source_tdx-0.1.0.dist-info"
    shutil.copytree(TDX_PACKAGE_ROOT / "src" / "axdata_source_tdx", package_root)
    dist_info.mkdir(parents=True)

    manifest_path = package_root / "axdata-provider.json"
    manifest = ProviderManifest.from_dict(json.loads(manifest_path.read_text(encoding="utf-8")))
    assert manifest.provider.provider_id == TDX_PROVIDER_ID

    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\n"
        "Name: axdata-source-tdx\n"
        "Version: 0.1.0\n",
        encoding="utf-8",
    )
    (dist_info / "entry_points.txt").write_text(
        "[axdata.providers]\n"
        "tdx = axdata_source_tdx.provider:provider\n",
        encoding="utf-8",
    )
    (dist_info / "RECORD").write_text(
        "\n".join(
            [
                "axdata_source_tdx/__init__.py,,",
                "axdata_source_tdx/adapter.py,,",
                "axdata_source_tdx/catalog.py,,",
                "axdata_source_tdx/client_factory.py,,",
                "axdata_source_tdx/collectors.py,,",
                "axdata_source_tdx/codes.py,,",
                "axdata_source_tdx/downloader.py,,",
                "axdata_source_tdx/downloader_interface_sets.py,,",
                "axdata_source_tdx/downloader_profiles.py,,",
                "axdata_source_tdx/downloader_registry.py,,",
                "axdata_source_tdx/execution_utils.py,,",
                "axdata_source_tdx/normalize_utils.py,,",
                "axdata_source_tdx/host_config.py,,",
                "axdata_source_tdx/tdx_server_config.py,,",
                "axdata_source_tdx/tdx_f10_names.py,,",
                "axdata_source_tdx/tdx_f10_models.py,,",
                "axdata_source_tdx/tdx_f10_catalog.py,,",
                "axdata_source_tdx/tdx_f10_specs.py,,",
                "axdata_source_tdx/adjustment.py,,",
                "axdata_source_tdx/adjustment_fetch.py,,",
                "axdata_source_tdx/derived_rows.py,,",
                "axdata_source_tdx/f10_executor.py,,",
                "axdata_source_tdx/f10_normalize.py,,",
                "axdata_source_tdx/f10_params.py,,",
                "axdata_source_tdx/f10_postprocess.py,,",
                "axdata_source_tdx/f10_request.py,,",
                "axdata_source_tdx/f10_render.py,,",
                "axdata_source_tdx/tqlex.py,,",
                "axdata_source_tdx/auction_fetch.py,,",
                "axdata_source_tdx/code_fetch.py,,",
                "axdata_source_tdx/finance_fetch.py,,",
                "axdata_source_tdx/finance_maps.py,,",
                "axdata_source_tdx/finance_normalize.py,,",
                "axdata_source_tdx/metadata.py,,",
                "axdata_source_tdx/options.py,,",
                "axdata_source_tdx/provider.py,,",
                "axdata_source_tdx/provider_bridge.py,,",
                "axdata_source_tdx/request_adapter.py,,",
                "axdata_source_tdx/price_limit_fetch.py,,",
                "axdata_source_tdx/price_limits.py,,",
                "axdata_source_tdx/price_limit_calendar.py,,",
                "axdata_source_tdx/price_limit_history.py,,",
                "axdata_source_tdx/quote_fetch.py,,",
                "axdata_source_tdx/quote_identity.py,,",
                "axdata_source_tdx/snapshot_normalize.py,,",
                "axdata_source_tdx/time_series_normalize.py,,",
                "axdata_source_tdx/intraday_fetch.py,,",
                "axdata_source_tdx/interface_sets.py,,",
                "axdata_source_tdx/kline_helpers.py,,",
                "axdata_source_tdx/wire_requests.py,,",
                "axdata_source_tdx/wire.py,,",
                "axdata_source_tdx/_tdx_wire/__init__.py,,",
                "axdata_source_tdx/_tdx_wire/_code_utils.py,,",
                "axdata_source_tdx/_tdx_wire/_command_codec.py,,",
                "axdata_source_tdx/_tdx_wire/_command_lookup.py,,",
                "axdata_source_tdx/_tdx_wire/_time_utils.py,,",
                "axdata_source_tdx/_tdx_wire/client.py,,",
                "axdata_source_tdx/_tdx_wire/hosts.py,,",
                "axdata_source_tdx/_tdx_wire/py.typed,,",
                "axdata_source_tdx/_tdx_wire/models/quote.py,,",
                "axdata_source_tdx/_tdx_wire/protocol/commands/codec.py,,",
                "axdata_source_tdx/_tdx_wire/protocol/commands/lookup.py,,",
                "axdata_source_tdx/_tdx_wire/protocol/commands/quotes.py,,",
                "axdata_source_tdx/_tdx_wire/transport/socket.py,,",
                "axdata_source_tdx/limit_ladder_topics.py,,",
                "axdata_source_tdx/realtime_refresh.py,,",
                "axdata_source_tdx/request_adapter_runtime.py,,",
                "axdata_source_tdx/request_client.py,,",
                "axdata_source_tdx/request_compat.py,,",
                "axdata_source_tdx/request_dispatch.py,,",
                "axdata_source_tdx/request_entrypoints.py,,",
                "axdata_source_tdx/request_host_config.py,,",
                "axdata_source_tdx/request_methods.py,,",
                "axdata_source_tdx/request_seams.py,,",
                "axdata_source_tdx/rank_fetch.py,,",
                "axdata_source_tdx/rank_params.py,,",
                "axdata_source_tdx/request_filters.py,,",
                "axdata_source_tdx/request_limits.py,,",
                "axdata_source_tdx/request_params.py,,",
                "axdata_source_tdx/security_codes.py,,",
                "axdata_source_tdx/series_history.py,,",
                "axdata_source_tdx/limit_ladder_fetch.py,,",
                "axdata_source_tdx/status_fetch.py,,",
                "axdata_source_tdx/server_cache.py,,",
                "axdata_source_tdx/source_adapter_registry.py,,",
                "axdata_source_tdx/source_execution.py,,",
                "axdata_source_tdx/source_execution_registry.py,,",
                "axdata_source_tdx/stats_cache.py,,",
                "axdata_source_tdx/stats_models.py,,",
                "axdata_source_tdx/stats_resource.py,,",
                "axdata_source_tdx/resources/__init__.py,,",
                "axdata_source_tdx/resources/finance_maps/__init__.py,,",
                "axdata_source_tdx/resources/finance_maps/incon.dat,,",
                "axdata_source_tdx/resources/finance_maps/tdxhy.cfg,,",
                "axdata_source_tdx/resources/finance_maps/tdxzs.cfg,,",
                "axdata_source_tdx/resources/tdx_quote_servers.json,,",
                "axdata_source_tdx/resources/tdx_extended_servers.json,,",
                "axdata_source_tdx/axdata-provider.json,,",
                "axdata_source_tdx-0.1.0.dist-info/METADATA,,",
                "axdata_source_tdx-0.1.0.dist-info/entry_points.txt,,",
                "axdata_source_tdx-0.1.0.dist-info/RECORD,,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return install_root


def test_core_tdx_cached_provider_choices_keep_live_module_attributes(monkeypatch) -> None:
    from axdata_core.adapters.tdx import (
        client_factory,
        downloader,
        downloader_profiles,
        downloader_registry,
        host_config,
        provider_bridge,
        request_adapter_runtime,
        request_compat,
        request_host_config,
        request_methods,
        request_seams,
        server_cache,
        source_adapter_registry,
        source_execution,
        source_execution_registry,
        stats_cache,
        stats_models,
        stats_resource,
    )

    def install_fake_provider(wrapper, provider_loader_name: str, fake):
        calls = {"count": 0}

        def loader():
            calls["count"] += 1
            return fake

        monkeypatch.setattr(wrapper, "_IMPLEMENTATION", None)
        monkeypatch.setattr(wrapper, provider_loader_name, loader)
        return calls

    for wrapper, provider_loader_name in [
        (request_methods, "_provider_package_request_methods"),
        (request_adapter_runtime, "_provider_package_request_adapter_runtime"),
        (request_seams, "_provider_package_request_seams"),
    ]:
        fake = SimpleNamespace(marker="first")
        calls = install_fake_provider(wrapper, provider_loader_name, fake)
        assert getattr(wrapper, "marker") == "first"
        fake.marker = "second"
        assert getattr(wrapper, "marker") == "second"
        assert calls["count"] == 1

    fake_compat = SimpleNamespace(resolve_request_compat_export=lambda name, globals_, module_name: "first")
    calls = install_fake_provider(request_compat, "_provider_package_request_compat", fake_compat)
    assert request_compat.resolve_request_compat_export("demo", {}, "demo") == "first"
    fake_compat.resolve_request_compat_export = lambda name, globals_, module_name: "second"
    assert request_compat.resolve_request_compat_export("demo", {}, "demo") == "second"
    assert calls["count"] == 1

    fake_request_hosts = SimpleNamespace(
        effective_host_strings=lambda kind, *, cache_root=None: [f"first:{kind}:{cache_root}"]
    )
    calls = install_fake_provider(
        request_host_config,
        "_provider_package_request_host_config",
        fake_request_hosts,
    )
    monkeypatch.setattr(request_host_config, "_loaded_request_override", lambda name: None)
    assert request_host_config.effective_host_strings("quote", cache_root="cache") == ["first:quote:cache"]
    fake_request_hosts.effective_host_strings = lambda kind, *, cache_root=None: [f"second:{kind}:{cache_root}"]
    assert request_host_config.effective_host_strings("quote", cache_root="cache") == ["second:quote:cache"]
    assert calls["count"] == 1

    fake_client_factory = SimpleNamespace(tdx_env_int=lambda name, default, *, minimum: 11)
    calls = install_fake_provider(client_factory, "_provider_package_client_factory", fake_client_factory)
    assert client_factory.tdx_env_int("AXDATA_TDX_DEMO", 1, minimum=1) == 11
    fake_client_factory.tdx_env_int = lambda name, default, *, minimum: 22
    assert client_factory.tdx_env_int("AXDATA_TDX_DEMO", 1, minimum=1) == 22
    assert calls["count"] == 1

    fake_host_config = SimpleNamespace(env_hosts=lambda env_name="AXDATA_TDX_HOSTS": ["first"])
    calls = install_fake_provider(host_config, "_provider_package_host_config", fake_host_config)
    assert host_config.env_hosts() == ["first"]
    fake_host_config.env_hosts = lambda env_name="AXDATA_TDX_HOSTS": ["second"]
    assert host_config.env_hosts() == ["second"]
    assert calls["count"] == 1

    class FirstTdxClient:
        pass

    class SecondTdxClient:
        pass

    fake_server_cache = SimpleNamespace(tdx_server_cache_root=lambda data_root: "first-root")
    calls = install_fake_provider(server_cache, "_provider_package_server_cache", fake_server_cache)
    assert server_cache.tdx_server_cache_root("root") == "first-root"
    fake_server_cache.tdx_server_cache_root = lambda data_root: "second-root"
    assert server_cache.tdx_server_cache_root("root") == "second-root"
    assert calls["count"] == 1

    fake_bridge = SimpleNamespace(split_tdx_provider_options=lambda options=None: ("first", {}))
    calls = install_fake_provider(provider_bridge, "_provider_package_bridge", fake_bridge)
    assert provider_bridge.split_tdx_provider_options({}) == ("first", {})
    fake_bridge.split_tdx_provider_options = lambda options=None: ("second", {})
    assert provider_bridge.split_tdx_provider_options({}) == ("second", {})
    assert calls["count"] == 1

    fake_execution = SimpleNamespace(tdx_execution_enricher=lambda data_root: (lambda options: options.update(mark="first")))
    calls = install_fake_provider(source_execution, "_provider_package_source_execution", fake_execution)
    options: dict[str, object] = {}
    source_execution.tdx_execution_enricher("root")(options)
    assert options == {"mark": "first"}
    fake_execution.tdx_execution_enricher = lambda data_root: (lambda options: options.update(mark="second"))
    options = {}
    source_execution.tdx_execution_enricher("root")(options)
    assert options == {"mark": "second"}
    assert calls["count"] == 1

    fake_execution_registry = SimpleNamespace(tdx_source_execution_enrichers=lambda: {"tdx": "first"})
    calls = install_fake_provider(
        source_execution_registry,
        "_provider_package_source_execution_registry",
        fake_execution_registry,
    )
    assert source_execution_registry.tdx_source_execution_enrichers() == {"tdx": "first"}
    fake_execution_registry.tdx_source_execution_enrichers = lambda: {"tdx": "second"}
    assert source_execution_registry.tdx_source_execution_enrichers() == {"tdx": "second"}
    assert calls["count"] == 1

    fake_adapter_registry = SimpleNamespace(tdx_provider_adapter_factories=lambda: {"axdata.source.tdx": "first"})
    calls = install_fake_provider(
        source_adapter_registry,
        "_provider_package_source_adapter_registry",
        fake_adapter_registry,
    )
    assert source_adapter_registry.tdx_provider_adapter_factories() == {"axdata.source.tdx": "first"}
    fake_adapter_registry.tdx_provider_adapter_factories = lambda: {"axdata.source.tdx": "second"}
    assert source_adapter_registry.tdx_provider_adapter_factories() == {"axdata.source.tdx": "second"}
    assert calls["count"] == 1

    fake_downloader_registry = SimpleNamespace(tdx_downloader_adapter_factories=lambda: {"tdx": "first"})
    calls = install_fake_provider(downloader_registry, "_provider_package_registry", fake_downloader_registry)
    assert downloader_registry.tdx_downloader_adapter_factories() == {"tdx": "first"}
    fake_downloader_registry.tdx_downloader_adapter_factories = lambda: {"tdx": "second"}
    assert downloader_registry.tdx_downloader_adapter_factories() == {"tdx": "second"}
    assert calls["count"] == 1

    fake_downloader = SimpleNamespace(tdx_adapter_options=lambda **kwargs: {"mark": "first"})
    calls = install_fake_provider(downloader, "_provider_package_downloader", fake_downloader)
    assert downloader.tdx_adapter_options(interface_name="demo", pool_size=1) == {"mark": "first"}
    fake_downloader.tdx_adapter_options = lambda **kwargs: {"mark": "second"}
    assert downloader.tdx_adapter_options(interface_name="demo", pool_size=1) == {"mark": "second"}
    assert calls["count"] == 1

    fake_profiles = SimpleNamespace(tdx_downloader_profiles=lambda concurrency_cls, profile_cls: {"tdx": "first"})
    calls = install_fake_provider(downloader_profiles, "_provider_package_profiles", fake_profiles)
    assert downloader_profiles.tdx_downloader_profiles(object, object) == {"tdx": "first"}
    fake_profiles.tdx_downloader_profiles = lambda concurrency_cls, profile_cls: {"tdx": "second"}
    assert downloader_profiles.tdx_downloader_profiles(object, object) == {"tdx": "second"}
    assert calls["count"] == 1

    fake_stats_cache = SimpleNamespace(stats_cache_root=lambda cache_root: "first-cache")
    calls = install_fake_provider(stats_cache, "_provider_package_stats_cache", fake_stats_cache)
    assert stats_cache.stats_cache_root("root") == "first-cache"
    fake_stats_cache.stats_cache_root = lambda cache_root: "second-cache"
    assert stats_cache.stats_cache_root("root") == "second-cache"
    assert calls["count"] == 1

    fake_stats_models = SimpleNamespace(text_value=lambda value: "first-text")
    calls = install_fake_provider(stats_models, "_provider_package_stats_models", fake_stats_models)
    assert stats_models.text_value("demo") == "first-text"
    fake_stats_models.text_value = lambda value: "second-text"
    assert stats_models.text_value("demo") == "second-text"
    assert calls["count"] == 1

    fake_stats_resource = SimpleNamespace(_refresh_stats_param=lambda value, *, validation_error=ValueError: "first")
    calls = install_fake_provider(stats_resource, "_provider_package_stats_resource", fake_stats_resource)
    assert stats_resource._refresh_stats_param("yes") == "first"
    fake_stats_resource._refresh_stats_param = lambda value, *, validation_error=ValueError: "second"
    assert stats_resource._refresh_stats_param("yes") == "second"
    assert calls["count"] == 1


def _class_function_names(path: Path, class_name: str) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return [
                child.name
                for child in node.body
                if isinstance(child, ast.FunctionDef)
            ]
    raise AssertionError(f"{class_name} not found in {path}")


def _clear_tdx_provider_modules() -> None:
    for module_name in list(sys.modules):
        if module_name == "axdata_source_tdx" or module_name.startswith("axdata_source_tdx."):
            sys.modules.pop(module_name, None)
