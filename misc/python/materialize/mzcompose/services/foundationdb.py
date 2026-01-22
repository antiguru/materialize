# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import MZ_ROOT
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class FoundationDB(Service):
    def __init__(
        self,
        name: str = "foundationdb",
        mzbuild: str = "foundationdb",
        image: str | None = None,
        ports: list[str] = ["4500"],
        environment: list[str] = [
            "FDB_NETWORKING_MODE=container",
        ],
        volumes: list[str] = [],
        restart: str = "no",
    ) -> None:

        env_extra = [
            f"FDB_COORDINATOR_PORT={ports[0]}",
            f"FDB_PORT={ports[0]}",
        ]

        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}

        volumes += [f"{MZ_ROOT}/misc/foundationdb/:/etc/foundationdb/"]

        config.update(
            {
                "ports": ports,
                "environment": env_extra + environment,
                "restart": restart,
                "volumes": volumes,
            }
        )
        super().__init__(name=name, config=config)
