# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from textwrap import dedent
from typing import TYPE_CHECKING, Any

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.util import MzVersion

if TYPE_CHECKING:
    from materialize.checks.scenarios import Scenario


class MzcomposeAction(Action):
    def join(self, e: Executor) -> None:
        # Most of these actions are already blocking
        pass


class StartMz(MzcomposeAction):
    def __init__(
        self,
        tag: MzVersion | None = None,
        environment_extra: list[str] = [],
        system_parameter_defaults: dict[str, str] | None = None,
    ) -> None:
        self.tag = tag
        self.environment_extra = environment_extra
        self.system_parameter_defaults = system_parameter_defaults

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/materialized:{self.tag}" if self.tag is not None else None
        print(f"Starting Mz using image {image}")
        if self._tag_or_cargo_version() >= MzVersion.parse("0.71.0-dev"):
            print("Using statically configured replicas")
            static_replicas = {
                "clusterd": {
                    "allocation": {
                        "workers": 1,
                        "scale": 1,
                        "credits_per_hour": "0",
                    },
                    "ports": {
                        "storagectl": ["clusterd_compute_1:2100"],
                        "storage": ["clusterd_compute_1:2103"],
                        "compute": ["clusterd_compute_1:2102"],
                        "computectl": ["clusterd_compute_1:2101"],
                    },
                }
            }

            options = [f"--orchestrator-static-replicas={json.dumps(static_replicas)}"]
        else:
            print("Using legacy remote replicas")
            options = []

        mz = Materialized(
            image=image,
            options=options,
            external_cockroach=True,
            environment_extra=self.environment_extra,
            system_parameter_defaults=self.system_parameter_defaults,
        )

        with c.override(mz):
            c.up("materialized")

        mz_version = MzVersion.parse_sql(c)
        tag_or_cargo_verion = self._tag_or_cargo_version()
        assert (
            tag_or_cargo_verion == mz_version
        ), f"Materialize version mismatch, expected {tag_or_cargo_verion}, but got {mz_version}"

        e.current_mz_version = mz_version

    def _tag_or_cargo_version(self) -> MzVersion:
        if self.tag:
            return self.tag
        else:
            return MzVersion.parse_cargo()


class ConfigureMz(MzcomposeAction):
    def __init__(self, scenario: "Scenario") -> None:
        self.handle: Any | None = None

    def execute(self, e: Executor) -> None:
        input = dedent(
            """
            # Run any query to have the materialize user implicitly created if
            # it didn't exist yet. Required for the GRANT later.
            > SELECT 1;
            1
            """
        )

        system_settings = {
            "ALTER SYSTEM SET max_tables = 1000;",
            "ALTER SYSTEM SET max_sinks = 1000;",
            "ALTER SYSTEM SET max_sources = 1000;",
            "ALTER SYSTEM SET max_materialized_views = 1000;",
            "ALTER SYSTEM SET max_objects_per_schema = 1000;",
            "ALTER SYSTEM SET max_secrets = 1000;",
            "ALTER SYSTEM SET max_clusters = 1000;",
        }

        # Since we already test with RBAC enabled, we have to give materialize
        # user the relevant attributes so the existing tests keep working.
        if MzVersion(0, 45, 0) <= e.current_mz_version < MzVersion.parse("0.59.0-dev"):
            system_settings.add(
                "ALTER ROLE materialize CREATEROLE CREATEDB CREATECLUSTER;"
            )
        elif e.current_mz_version >= MzVersion.parse("0.59.0"):
            system_settings.add("GRANT ALL PRIVILEGES ON SYSTEM TO materialize;")

        if e.current_mz_version >= MzVersion(0, 47, 0):
            system_settings.add("ALTER SYSTEM SET enable_rbac_checks TO true;")

        if e.current_mz_version >= MzVersion.parse("0.51.0-dev"):
            system_settings.add("ALTER SYSTEM SET enable_ld_rbac_checks TO true;")

        if e.current_mz_version >= MzVersion.parse("0.52.0-dev"):
            # Since we already test with RBAC enabled, we have to give materialize
            # user the relevant privileges so the existing tests keep working.
            system_settings.add("GRANT CREATE ON DATABASE materialize TO materialize;")
            system_settings.add(
                "GRANT CREATE ON SCHEMA materialize.public TO materialize;"
            )
            system_settings.add("GRANT CREATE ON CLUSTER default TO materialize;")

        if (
            MzVersion.parse("0.58.0-dev")
            <= e.current_mz_version
            <= MzVersion.parse("0.63.99")
        ):
            system_settings.add("ALTER SYSTEM SET enable_managed_clusters = on;")

        system_settings = system_settings - e.system_settings

        if system_settings:
            input += (
                "$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}\n"
                + "\n".join(system_settings)
            )

        self.handle = e.testdrive(input=input)
        e.system_settings.update(system_settings)

    def join(self, e: Executor) -> None:
        e.join(self.handle)


class KillMz(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.kill("materialized")


class UseClusterdCompute(MzcomposeAction):
    def __init__(self, scenario: "Scenario") -> None:
        self.base_version = scenario.base_version()

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        if e.current_mz_version >= MzVersion.parse("0.71.0-dev"):
            c.sql(
                """

            DROP CLUSTER REPLICA default.r1;
            CREATE CLUSTER REPLICA default.r1 SIZE 'clusterd';
            """,
                port=6877,
                user="mz_system",
            )
            return

        storage_addresses = (
            """STORAGECTL ADDRESSES ['clusterd_compute_1:2100'],
                STORAGE ADDRESSES ['clusterd_compute_1:2103']"""
            if self.base_version >= MzVersion(0, 44, 0)
            else "STORAGECTL ADDRESS 'clusterd_compute_1:2100'"
        )

        if self.base_version >= MzVersion(0, 55, 0):
            c.sql(
                "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = on;",
                port=6877,
                user="mz_system",
            )

        c.sql(
            f"""
            ALTER CLUSTER default SET (MANAGED = false);
            DROP CLUSTER REPLICA default.r1;
            CREATE CLUSTER REPLICA default.r1
                {storage_addresses},
                COMPUTECTL ADDRESSES ['clusterd_compute_1:2101'],
                COMPUTE ADDRESSES ['clusterd_compute_1:2102'],
                WORKERS 1;
            """,
            port=6877,
            user="mz_system",
        )


class KillClusterdCompute(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        with c.override(Clusterd(name="clusterd_compute_1")):
            c.kill("clusterd_compute_1")


class StartClusterdCompute(MzcomposeAction):
    def __init__(self, tag: MzVersion | None = None) -> None:
        self.tag = tag

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        clusterd = Clusterd(name="clusterd_compute_1")
        if self.tag:
            clusterd = Clusterd(
                name="clusterd_compute_1",
                image=f"materialize/clusterd:{self.tag}",
            )
        print(f"Starting Compute using image {clusterd.config.get('image')}")

        with c.override(clusterd):
            c.up("clusterd_compute_1")


class RestartRedpandaDebezium(MzcomposeAction):
    """Restarts Redpanda and Debezium. Debezium is unable to survive Redpanda restarts so the two go together."""

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        for service in ["redpanda", "debezium"]:
            c.kill(service)
            c.up(service)


class RestartCockroach(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("cockroach")
        c.up("cockroach")


class RestartSourcePostgres(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("postgres")
        c.up("postgres")


class KillClusterdStorage(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd` || true")


class DropCreateDefaultReplica(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            """
            ALTER CLUSTER default SET (MANAGED = false);
            DROP CLUSTER REPLICA default.r1;
            CREATE CLUSTER REPLICA default.r1 SIZE '1';
            """,
            port=6877,
            user="mz_system",
        )
