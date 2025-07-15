import mysql.connector
import os
import time
import glob
from typing import List, Tuple

def get_connection(autocommit: bool = True):
    db_conf = {
        "host": "tidb.ruulwo1ugj7r.clusters.staging.tidb-cloud.com",
        "port": 4000,
        "user": "root",
        "password": "12345678",
        "database": "gharchive_dev",
        "autocommit": autocommit,
        "use_pure": True
    }
    return mysql.connector.connect(**db_conf)

def read_sql_files(directory: str) -> List[str]:
    return sorted(glob.glob(os.path.join(directory, "*.sql")))

def execute_sql_file(cur, sql_file: str) -> Tuple[str, float]:
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read().strip()
        if not sql_content:
            return os.path.basename(sql_file), 0.0
        start_time = time.time()
        cur.execute(sql_content)
        # if sql_content.strip().upper().startswith('SELECT'):
        cur.fetchall()
        end_time = time.time()
        return os.path.basename(sql_file), end_time - start_time
    except Exception as e:
        print(f"执行SQL文件 {sql_file} 时出错: {str(e)}")
        return os.path.basename(sql_file), -1.0

def run():
    sql_directory = "/data3/dzh/project/grep/dev/OSS/workload"
    sql_files = read_sql_files(sql_directory)
    if not sql_files:
        print(f"未找到SQL文件: {sql_directory}")
        return

    results = []
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            
            sql = """explain analyze WITH /*PLACEHOLDER*/ /*trending-repos-past_24_hours-C#.sql*/ stars AS (
            SELECT
                ge.repo_id AS repo_id,
                COUNT(1) AS total,
                COUNT(DISTINCT actor_id) AS actors,
                -- Calculate the score of each star according to the time of the star, the closer to the 
                -- current time, the higher the score got, the score range is between 2-5. Then sum the
                -- scores of all stars to get the total score obtained from the stars for the repository.
                SUM(
                    GREATEST (
                        LEAST (
                            (
                                (
                                    TIMESTAMPDIFF(SECOND, DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY), ge.created_at) / 
                                    TIMESTAMPDIFF(SECOND, DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY), '2022-09-28 08:59:59')
                                ) * (5 - 2)
                            ), 5
                        ), 2
                    )
                ) AS score
            FROM github_events ge
            WHERE
                -- Notice: In the GitHub events, WatchEvent means star, not watch.
                type = 'WatchEvent'
                AND (ge.created_at >= DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY) AND ge.created_at <= '2022-09-28 08:59:59')
            GROUP BY ge.repo_id
            -- Exclude code repositories that use the same user to duplicate stars.
            HAVING actors > 0.9 * total
            ), forks AS (
            SELECT
                ge.repo_id AS repo_id,
                COUNT(1) AS total,
                COUNT(DISTINCT actor_id) AS actors,
                -- Calculate the score of each fork according to the time of the fork, the closer to the 
                -- current time, the higher the score got, the score range is between 1-4. Then sum the
                -- scores of all forks to get the total score obtained from the forks for the repository.
                SUM(
                    GREATEST (
                        LEAST (
                            (
                                (
                                    TIMESTAMPDIFF(SECOND, DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY), ge.created_at) / 
                                    TIMESTAMPDIFF(SECOND, DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY), '2022-09-28 08:59:59')
                                ) * (4 - 1)
                            ), 4
                        ), 1
                    )
                ) AS score
            FROM github_events ge
            WHERE
                type = 'ForkEvent'
                AND (ge.created_at >= DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY) AND ge.created_at <= '2022-09-28 08:59:59')
            GROUP BY ge.repo_id
            -- Exclude code repositories that use the same user to duplicate froks.
            HAVING actors > 0.9 * total
            ), topRepos AS (
                SELECT
                    r.repo_id,
                    r.repo_name,
                    r.primary_language,
                    r.description,
                    s.total AS stars_inc,
                    IFNULL(f.total, 0) AS forks_inc,
                    -- Calculate the composite score for the repository.
                    SUM(
                        s.score + 
                        IFNULL(f.score, 0) +
                        -- Give the new repository a higher score base.
                        ABS(1 /  (1 + TIMESTAMPDIFF(YEAR, r.created_at, '2022-09-28 08:59:59'))) * 200
                    ) AS total_score
                FROM github_repos r
                    JOIN stars s ON r.repo_id = s.repo_id
                    LEFT JOIN forks f ON r.repo_id = f.repo_id
                WHERE
                    -- Filter rule: The repository must have at least 5 stars.
                    stars > 5
                    AND stars < 50000
                    -- Filter rule: The repository must have at least 5 forks.
                    AND forks > 5
                    -- Filter rule: The repository must have pushed new code within the last three months.
                    AND pushed_at > DATE_SUB('2022-09-28 08:59:59', INTERVAL 3 MONTH)
                    -- Filter rule: Exclude some malicious new repositories.
                    AND created_at < DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY)
                    -- Filter rule: There should be no uncivilized words in the name of the repository.
                    AND LOWER(repo_name) NOT LIKE '%fuck%'
                    -- Filter by repository language.
                    AND primary_language = 'C#'
                    AND repo_name NOT IN (SELECT name FROM blacklist_repos)
                    AND is_deleted = 0
                GROUP BY r.repo_id
                ORDER BY total_score DESC
                LIMIT 100
            ), pull_requests AS (
            SELECT
                ge.repo_id AS repo_id,
                COUNT(1) AS total
            FROM github_events ge
            JOIN topRepos tr ON ge.repo_id = tr.repo_id
            WHERE
                type = 'PullRequestEvent'
                AND action = 'opened'
                AND (ge.created_at >= DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY) AND ge.created_at <= '2022-09-28 08:59:59')
                AND actor_login NOT LIKE '%[bot]'
            GROUP BY ge.repo_id
            ), pushes AS (
            SELECT
                ge.repo_id AS repo_id,
                COUNT(1) AS total
            FROM github_events ge
            JOIN topRepos tr ON ge.repo_id = tr.repo_id
            WHERE
                type = 'PushEvent'
                AND (ge.created_at >= DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY) AND ge.created_at <= '2022-09-28 08:59:59')
                AND actor_login NOT LIKE '%[bot]'
            GROUP BY ge.repo_id
            ), repo_with_top_contributors AS (
                SELECT
                    repo_id, SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT actor_login ORDER BY cnt DESC SEPARATOR ','), ',', 5) AS actor_logins
                FROM (
                    SELECT
                        ge.repo_id AS repo_id,
                        ge.actor_login AS actor_login,
                        COUNT(*) AS cnt
                    FROM github_events ge
                    WHERE
                        (
                            (type = 'PullRequestEvent' AND action = 'opened') OR
                            (type = 'IssuesEvent' AND action = 'opened') OR
                            (type = 'PullRequestReviewEvent' AND action = 'created') OR
                            (type = 'PushEvent' AND action = '')
                        )
                        AND (ge.created_at >= DATE_SUB('2022-09-28 08:59:59', INTERVAL 1 DAY) AND ge.created_at <= '2022-09-28 08:59:59')
                        AND ge.repo_id IN (SELECT tr.repo_id FROM topRepos tr)
                        AND ge.actor_login NOT IN (SELECT bu.login FROM blacklist_users bu)
                        AND ge.actor_login NOT LIKE '%bot%'
                    GROUP BY ge.repo_id, ge.actor_login
                    ORDER BY ge.repo_id, cnt DESC
                ) sub
                GROUP BY repo_id
            ), repo_with_collections AS (
                SELECT
                    tr.repo_id, GROUP_CONCAT(DISTINCT c.name) AS collection_names
                FROM topRepos tr
                JOIN collection_items ci ON ci.repo_name = tr.repo_name
                JOIN collections c ON ci.collection_id = c.id
                WHERE c.public = true
                GROUP BY tr.repo_id
            )
            SELECT /*PLACEHOLDER*/ /*trending-repos-past_24_hours-C#.sql*/ 
                tr.repo_id,
                tr.repo_name,
                tr.primary_language AS language,
                tr.description,
                tr.stars_inc AS stars,
                tr.forks_inc AS forks,
                pr.total AS pull_requests,
                pu.total AS pushes,
                tr.total_score,
                tc.actor_logins AS contributor_logins,
                rc.collection_names AS collection_names
            FROM
                topRepos tr
                LEFT JOIN repo_with_top_contributors tc ON tr.repo_id = tc.repo_id
                LEFT JOIN repo_with_collections rc ON tr.repo_id = rc.repo_id
                LEFT JOIN pull_requests pr ON tr.repo_id = pr.repo_id
                LEFT JOIN pushes pu ON tr.repo_id = pu.repo_id
            ORDER BY total_score DESC;"""

            cur.execute(sql)
            rows = cur.fetchall()
            with open('execution plan.txt', 'w', encoding='utf-8') as f:
                for row in rows:
                    # 将每一行转为字符串并写入文件，每行一个结果
                    f.write(str(row) + '\n')        

    #         for sql_file in sql_files:
    #             filename, execution_time = execute_sql_file(cur, sql_file)
    #             results.append((filename, execution_time))
    #             if execution_time >= 0:
    #                 print(f"{filename:<30} {execution_time:.4f} 秒")
    #             else:
    #                 print(f"{filename:<30} 执行失败")

    # # 统计
    # times = [r[1] for r in results if r[1] >= 0]
    # if times:
    #     print("=" * 60)
    #     print(f"平均执行时间: {sum(times)/len(times):.4f} 秒")
    # else:
    #     print("没有成功执行的SQL文件")

if __name__ == "__main__":
    run()