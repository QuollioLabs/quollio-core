from blake3 import blake3


def new_global_id(company_id: str, cluster_id: str, data_id: str, data_type: str) -> str:
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

    data_to_hash = f"{company_id}{cluster_id}{data_id}"
    hashed = blake3(data_to_hash.encode()).digest()
    global_id: str = prefix + hashed[:16].hex()
    return global_id
