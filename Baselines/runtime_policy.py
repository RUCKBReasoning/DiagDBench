"""Runtime policy for diagnosis of already validated benchmark samples."""


def diagnosis_llm_config(config):
    """Use bounded transport attempts without multiplying SDK-level retries."""
    result = dict(config)
    result.setdefault("retry_times", 50)
    result.setdefault("request_retry_times", 49)
    result.setdefault("request_retry_delay_seconds", 30)
    result.setdefault("timeout", 300)
    result.setdefault("sdk_max_retries", 0)
    return result


def direct_injection_command(script_path, dbms, database, schema, user_config, timeout):
    """Run the supplied injection once, without replaying or scoring workloads."""
    timeout = int(timeout)
    if timeout <= 0:
        raise ValueError("direct_injection_timeout_seconds must be positive")
    host = str(user_config["DB_HOST"])
    port = "3306" if "mysql" in dbms.lower() else "5432"
    user = str(user_config["DB_USER"])
    password = str(user_config["DB_PASSWORD"])
    env = {
        "DB_HOST": host, "DB_PORT": port, "DB_USER": user,
        "DB_PASSWORD": password, "DB_NAME": database, "DB_SCHEMA": schema,
        "DBMS": dbms, "PGHOST": host, "PGPORT": port, "PGUSER": user,
        "PGPASSWORD": password, "PGDATABASE": database,
        "MYSQL_HOST": host, "MYSQL_PORT": port, "MYSQL_USER": user,
        "MYSQL_DATABASE": database, "MYSQL_PWD": password,
        "PYTHONUNBUFFERED": "1",
    }
    if "mysql" not in dbms.lower():
        env["PGOPTIONS"] = f"-c search_path={schema}"
    return [
        "timeout", "--signal=TERM", "--kill-after=30s", str(timeout),
        "env", *(f"{key}={value}" for key, value in env.items()),
        "bash", str(script_path),
    ]
