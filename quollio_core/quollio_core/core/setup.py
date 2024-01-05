import yaml
from jinja2 import Environment, FileSystemLoader

from quollio_core.repository.snowflake import SnowflakeConnectionConfig


def setup_dbt_profile(
    connections: SnowflakeConnectionConfig, project_path: str, template_path: str, template_name: str
) -> None:
    connections_json = connections.as_dict()

    profile_path = f"{project_path}/profiles.yml"
    loader = Environment(loader=(FileSystemLoader(template_path, encoding="utf-8")))
    template = loader.get_template(template_name)
    profiles_body = template.render(connections_json)
    with open(profile_path, "w") as profiles:
        yaml.dump(yaml.safe_load(profiles_body), profiles, default_flow_style=False, allow_unicode=True)
    return
