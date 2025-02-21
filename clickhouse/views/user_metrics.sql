CREATE VIEW default.user_metrics
(

    `user` String,

    `role` String,

    `user_profile` Nullable(String),

    `role_profile` Nullable(String),

    `user_grants` Array(String),

    `role_grants` Array(String),

    `user_queries_per_15_days` UInt64,

    `role_queries_per_15_days` UInt64,

    `user_profile_queries_per_15_days` UInt64,

    `role_profile_queries_per_15_days` UInt64,

    `user_queries_per_30_days` UInt64,

    `role_queries_per_30_days` UInt64,

    `user_profile_queries_per_30_days` UInt64,

    `role_profile_queries_per_30_days` UInt64,

    `user_queries_per_60_days` UInt64,

    `role_queries_per_60_days` UInt64,

    `user_profile_queries_per_60_days` UInt64,

    `role_profile_queries_per_60_days` UInt64
)
AS WITH
    toDate(now('Asia/Vladivostok')) - toIntervalDay(15) AS day_interval_15,

    toDate(now('Asia/Vladivostok')) - toIntervalDay(30) AS day_interval_30,

    toDate(now('Asia/Vladivostok')) - toIntervalDay(60) AS day_interval_60,

    _query_count_by_user AS
    (
        SELECT
            user,

            sum(if((event_time >= day_interval_15) AND (event_time <= toDate(now('Asia/Vladivostok'))),
 1,
 0)) AS user_queries_per_15_days,

            sum(if((event_time >= day_interval_30) AND (event_time <= toDate(now('Asia/Vladivostok'))),
 1,
 0)) AS user_queries_per_30_days,

            sum(if((event_time >= day_interval_60) AND (event_time <= toDate(now('Asia/Vladivostok'))),
 1,
 0)) AS user_queries_per_60_days
        FROM system.query_log
        WHERE (type IN (1,
 3)) AND (event_time >= (toDate(now('Asia/Vladivostok')) - toIntervalDay(60))) AND (event_time <= toDate(now('Asia/Vladivostok')))
        GROUP BY user
    )
SELECT
    users.name AS user,

    roles.granted_role_name AS role,

    if(user_profiles.inherit_profile IS NOT NULL,
 user_profiles.inherit_profile,
 '') AS user_profile,

    if(role_profiles.inherit_profile IS NOT NULL,
 role_profiles.inherit_profile,
 '') AS role_profile,

    user_grants.grants AS user_grants,

    role_grants.grants AS role_grants,

    if(user != '',
 _query_count_by_user.user_queries_per_15_days,
 0) AS user_queries_per_15_days,

    if(role != '',
 sum(user_queries_per_15_days) OVER (PARTITION BY role),
 0) AS role_queries_per_15_days,

    if(user_profile != '',
 sum(user_queries_per_15_days) OVER (PARTITION BY user_profile),
 0) AS user_profile_queries_per_15_days,

    if(role_profile != '',
 sum(user_queries_per_15_days) OVER (PARTITION BY role_profile),
 0) AS role_profile_queries_per_15_days,

    if(user != '',
 _query_count_by_user.user_queries_per_30_days,
 0) AS user_queries_per_30_days,

    if(role != '',
 sum(user_queries_per_30_days) OVER (PARTITION BY role),
 0) AS role_queries_per_30_days,

    if(user_profile != '',
 sum(user_queries_per_30_days) OVER (PARTITION BY user_profile),
 0) AS user_profile_queries_per_30_days,

    if(role_profile != '',
 sum(user_queries_per_30_days) OVER (PARTITION BY role_profile),
 0) AS role_profile_queries_per_30_days,

    if(user != '',
 _query_count_by_user.user_queries_per_60_days,
 0) AS user_queries_per_60_days,

    if(role != '',
 sum(user_queries_per_60_days) OVER (PARTITION BY role),
 0) AS role_queries_per_60_days,

    if(user_profile != '',
 sum(user_queries_per_60_days) OVER (PARTITION BY user_profile),
 0) AS user_profile_queries_per_60_days,

    if(role_profile != '',
 sum(user_queries_per_60_days) OVER (PARTITION BY role_profile),
 0) AS role_profile_queries_per_60_days
FROM
(
    SELECT name
    FROM system.users
    UNION ALL
    SELECT '' AS name
) AS users
LEFT JOIN
(
    SELECT
        user_name,

        granted_role_name
    FROM system.role_grants
    UNION ALL
    SELECT
        '' AS user_name,

        name AS granted_role_name
    FROM system.roles
    WHERE granted_role_name NOT IN (
        SELECT granted_role_name
        FROM system.role_grants
    )
) AS roles ON users.name = roles.user_name
LEFT JOIN system.settings_profile_elements AS user_profiles ON users.name = user_profiles.user_name
LEFT JOIN system.settings_profile_elements AS role_profiles ON roles.granted_role_name = role_profiles.role_name
LEFT JOIN
(
    SELECT
        user_name,

        groupArray(if(database IS NULL,
 toString(access_type),
 concat(toString(access_type),
 ' ON ',
 database,
 '.',
 if(`table` IS NULL,
 '*',
 `table`)))) AS grants
    FROM system.grants
    GROUP BY user_name
) AS user_grants ON users.name = user_grants.user_name
LEFT JOIN
(
    SELECT
        role_name,

        groupArray(if(database IS NULL,
 toString(access_type),
 concat(toString(access_type),
 ' ON ',
 database,
 '.',
 if(`table` IS NULL,
 '*',
 `table`)))) AS grants
    FROM system.grants
    GROUP BY role_name
) AS role_grants ON roles.granted_role_name = role_grants.role_name
LEFT JOIN _query_count_by_user ON users.name = _query_count_by_user.user;