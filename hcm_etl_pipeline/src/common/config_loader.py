import os
import yaml
from typing import Any, Dict

_CONFIG_BASE = os.path.join(os.path.dirname(__file__), "..", "..", "conf")

def load_config(env: str = "dev") -> Dict[str, Any]:
    config_path = os.path.join(_CONFIG_BASE, "env", f"{env}.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def load_contract(dataset_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    version = config.get("contract_versions", {}).get(dataset_name, "v1")
    contract_path = os.path.join(
        _CONFIG_BASE, "data_contracts", f"{dataset_name}_{version}.yaml"
    )
    with open(contract_path, "r") as f:
        return yaml.safe_load(f)
