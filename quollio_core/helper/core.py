from typing import Dict

import yaml
from blake3 import blake3
from jinja2 import Environment, FileSystemLoader


def new_global_id(tenant_id: str, cluster_id: str, data_id: str, data_type: str) -> str:
    prefix = ""
    data_types = {
        "schema": "schm-",
        "table": "tbl-",
        "column": "clmn-",
        "bigroup": "bgrp-",
        "dashboard": "dsbd-",
        "sheet": "sht-",
    }
    prefix = data_types[data_type]

    data_to_hash = f"{tenant_id}{cluster_id}{data_id}"
    hashed = blake3(data_to_hash.encode()).digest()
    global_id: str = prefix + hashed[:16].hex()
    return global_id


def setup_dbt_profile(connections_json: Dict[str, str], template_path: str, template_name: str) -> None:
    profile_path = f"{template_path}/profiles.yml"
    loader = Environment(loader=(FileSystemLoader(template_path, encoding="utf-8")))
    template = loader.get_template(template_name)
    profiles_body = template.render(connections_json)
    with open(profile_path, "w") as profiles:
        yaml.dump(yaml.safe_load(profiles_body), profiles, default_flow_style=False, allow_unicode=True)
    return


def trim_prefix(s: str, prefix: str) -> str:
    return s.lstrip(prefix)
