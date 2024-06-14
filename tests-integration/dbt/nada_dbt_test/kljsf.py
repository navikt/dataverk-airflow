DBT_DOCS_GENERATE = "dbt docs generate"
profiles_dir = "."
project_dir = "nada-metabase"
host = "host.no"
team = "team"

cmds = [
            f"{DBT_DOCS_GENERATE} --profiles-dir {profiles_dir} --project-dir {project_dir}",
            f"""export DBT_PROJECT=$(cat {project_dir}/dbt_project.yml | grep name: | cut -d' ' -f2 | tr -d \\' | tr -d \\")""",
            f"cd {project_dir}/target && curl -X PUT --fail-with-body --retry 2 -F manifest.json=@manifest.json -F catalog.json=@catalog.json -F index.html=@index.html https://{host}/docs/{team}/$DBT_PROJECT"
        ]

print(" && ".join(cmds))