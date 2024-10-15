-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- depends_on: {{ ref('product_category') }}
{{ config(
    materialized='source_table'
) }}
FROM SOURCE {{ ref('product_category') }}
(REFERENCE "datagen_demo_snowflakeschema_product_category")
KEY FORMAT BYTES
VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
INCLUDE TIMESTAMP as kafka_timestamp
ENVELOPE UPSERT;