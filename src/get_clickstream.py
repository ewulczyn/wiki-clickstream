from db_utils import exec_hive_stat2
from db_utils import execute_hive_expression,get_hive_timespan
import argparse
from sqoop_utils import sqoop_prod_dbs


"""
Usage:

python get_clickstream.py \
    --start 2016-09-01 \
    --stop  2016-09-01 \
    --table test_en \
    --lang en \
    --priority

"""

def get_clickstream(table, lang, start, stop, priority = False, min_count = 10):

    params = {  'time_conditions': get_hive_timespan(start, stop, hour = False),
                'table': table,
                'lang': lang,
                'min_count': min_count,
                }



    query = """

    -- ############################################
    -- create helper tables


    -- create copy of page table and insert rows for our special prev pages
    -- this will let us work with ids instead of titles later, which is much less error prone

    DROP TABLE IF EXISTS clickstream.%(table)s_page_helper;
    CREATE TABLE clickstream.%(table)s_page_helper AS
    SELECT
        *
    FROM
        clickstream.%(lang)s_page
    ;

    INSERT INTO TABLE clickstream.%(table)s_page_helper 
    SELECT
        -1 AS page_id,
        0 AS page_namespace,
        false AS page_is_redirect,
        'other-empty' AS page_title 
    FROM   clickstream.%(table)s_page_helper 
    LIMIT   1;


    INSERT INTO TABLE clickstream.%(table)s_page_helper 
    SELECT
        -2 AS page_id,
        0 AS page_namespace,
        false AS page_is_redirect,
        'other-internal' AS page_title 
    FROM   clickstream.%(table)s_page_helper 
    LIMIT   1;

    INSERT INTO TABLE clickstream.%(table)s_page_helper 
    SELECT
        -3 AS page_id,
        0 AS page_namespace,
        false AS page_is_redirect,
        'other-external' AS page_title 
    FROM   clickstream.%(table)s_page_helper 
    LIMIT   1;

    INSERT INTO TABLE clickstream.%(table)s_page_helper 
    SELECT
        -4 AS page_id,
        0 AS page_namespace,
        false AS page_is_redirect,
        'other-search' AS page_title 
    FROM   clickstream.%(table)s_page_helper 
    LIMIT   1;

    INSERT INTO TABLE clickstream.%(table)s_page_helper 
    SELECT
        -5 AS page_id,
        0 AS page_namespace,
        false AS page_is_redirect,
        'other-other' AS page_title 
    FROM   clickstream.%(table)s_page_helper 
    LIMIT   1;


    -- create pagelinks table that resolves links that end in a redirect
    -- this means that if A links to B, and B redirects to C, we replace the link (A,B) with (A,C)
    -- this lets us properly annotate link types after resolving redirects in the clickstream, since
    -- a user will experience following A as if it linked to C
    -- the group be ensures that each link only occurs once

    DROP TABLE IF EXISTS clickstream.%(table)s_pagelinks_helper;
    CREATE TABLE clickstream.%(table)s_pagelinks_helper AS
    SELECT
        pl_from_page_id,
        pl_to_page_id
    FROM
        (SELECT
            pl_from_page_id,
            CASE
                WHEN r.rd_to_page_id IS NULL THEN pl_to_page_id
                ELSE rd_to_page_id
            END AS pl_to_page_id
        FROM
            clickstream.%(lang)s_pagelinks l
        LEFT JOIN
            clickstream.%(lang)s_redirect r ON (r.rd_from_page_id = l.pl_to_page_id)            
        ) a
    GROUP BY
        pl_from_page_id,
        pl_to_page_id
    ;

    -- ############################################




    -- extract raw prev, curr pairs

    DROP VIEW IF EXISTS clickstream.%(table)s_temp1;
    CREATE VIEW clickstream.%(table)s_temp1 AS
    SELECT 
        CASE
            -- empty or malformed referer
            WHEN referer IS NULL THEN 'other-empty'
            WHEN referer == '' THEN 'other-empty'
            WHEN referer == '-' THEN 'other-empty'
            WHEN parse_url(referer,'HOST') is NULL THEN 'other-empty'
            -- internal referer from the same wikipedia
            WHEN 
                parse_url(referer,'HOST') in ('%(lang)s.wikipedia.org', '%(lang)s.m.wikipedia.org')
                AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
            THEN REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)
            -- other referers 
            WHEN referer_class = 'internal' THEN 'other-internal'
            WHEN referer_class = 'external' THEN 'other-external'
            WHEN referer_class = 'external (search engine)' THEN 'other-search'
            ELSE 'other-other'
        END as prev,
        pageview_info['page_title'] as curr
    FROM
        wmf.webrequest
    WHERE 
        %(time_conditions)s
        AND webrequest_source = 'text'
        AND normalized_host.project_class = 'wikipedia'
        AND normalized_host.project = '%(lang)s'
        AND is_pageview 
        AND agent_type = 'user'
    ;



    -- count raw prev, curr pairs, this speeds up later queries

    DROP TABLE IF EXISTS clickstream.%(table)s_temp2;
    CREATE TABLE clickstream.%(table)s_temp2 AS
    SELECT
        prev, curr, COUNT(*) as n
    FROM
        clickstream.%(table)s_temp1
    GROUP BY 
        prev, curr
    ;


    -- we enforce that curr and prev are main namespace pages
    -- the joins accomplish this because, in the logs, the non main namespace pages have the namespace prepended
    -- at this point curr and prev are ids

    DROP TABLE IF EXISTS clickstream.%(table)s_temp3;
    CREATE TABLE clickstream.%(table)s_temp3 AS
    SELECT 
        pp.page_id as prev,
        pc.page_id as curr,
        n
    FROM
        clickstream.%(table)s_temp2
    JOIN
        clickstream.%(table)s_page_helper pp ON (prev = pp.page_title)
    JOIN
        clickstream.%(table)s_page_helper pc ON (curr = pc.page_title)
    WHERE
        pp.page_namespace = 0
        AND pc.page_namespace = 0
    ;



    -- resolve curr redirects, one step
    -- note that prev should not be a redirect, so we do not bother resolving it
    -- and prev redirects will be filtered out at the end

    DROP TABLE IF EXISTS clickstream.%(table)s_temp4;
    CREATE TABLE clickstream.%(table)s_temp4 AS
    SELECT 
        prev,
        CASE
            WHEN rd_to_page_id IS NULL THEN curr
            ELSE rd_to_page_id
        END AS curr,
        n
    FROM
        clickstream.%(table)s_temp3
    LEFT JOIN
        clickstream.%(lang)s_redirect ON (curr = rd_from_page_id)
    ;

    -- re-aggregate after resolving redirects and filter out pairs that occur infrequently

    DROP TABLE IF EXISTS clickstream.%(table)s_temp5;
    CREATE TABLE clickstream.%(table)s_temp5 AS
    SELECT
        prev, curr, SUM(n) as n
    FROM
        clickstream.%(table)s_temp4
    GROUP BY
        prev, curr
    HAVING
        SUM(n) > %(min_count)s
    ;



    -- annotate link types

    DROP TABLE IF EXISTS clickstream.%(table)s_temp6;
    CREATE TABLE clickstream.%(table)s_temp6 AS
    SELECT
        prev,
        curr,
        CASE
            WHEN prev < 0 THEN 'external'
            WHEN (pl_from_page_id IS NOT NULL AND pl_to_page_id IS NOT NULL) THEN 'link'
            ELSE 'other'
        END AS type,
        n
    FROM
        clickstream.%(table)s_temp5
    LEFT JOIN
        clickstream.%(table)s_pagelinks_helper ON (prev = pl_from_page_id AND curr = pl_to_page_id)
    ;



    -- create final table
    -- remove self loops
    -- restrict prev and curr to main namespace, no redirects
    -- get page titles

    DROP TABLE IF EXISTS clickstream.%(table)s;
    CREATE TABLE clickstream.%(table)s
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE AS
    SELECT
        pp.page_title as prev,
        pc.page_title as curr,
        a.type,
        a.n
    FROM
        clickstream.%(table)s_temp6 a
    JOIN
        clickstream.%(table)s_page_helper pp ON (prev = pp.page_id)
    JOIN
        clickstream.%(table)s_page_helper pc ON (curr = pc.page_id)
    WHERE
        pp.page_is_redirect = false
        AND pp.page_namespace = 0
        AND pc.page_is_redirect = false
        AND pc.page_namespace = 0
        AND a.curr != a.prev
    ;



    DROP TABLE clickstream.%(table)s_temp1;
    DROP TABLE clickstream.%(table)s_temp2;
    DROP TABLE clickstream.%(table)s_temp3;
    DROP TABLE clickstream.%(table)s_temp4;
    DROP TABLE clickstream.%(table)s_temp5;
    DROP TABLE clickstream.%(table)s_temp6;
    DROP TABLE clickstream.%(table)s_page_helper;
    DROP TABLE clickstream.%(table)s_pagelinks_helper;
    """

    exec_hive_stat2(query % params, priority = priority)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument( '--start', required=True,  help='start day')
    parser.add_argument( '--stop', required=True,help='start day')
    parser.add_argument('--table', required=True, help='hive table')
    parser.add_argument('--lang', required=True, help='e.g. en')
    parser.add_argument('--min_count', default = 10, help='')
    parser.add_argument('--priority', default=False, action="store_true",help='queue')
    parser.add_argument('--refresh_etl', default=False, action="store_true",help='re-sqoop prod tables')

    args = parser.parse_args()

    if args.refresh_etl:
        sqoop_prod_dbs('clickstream', [args.lang,], ['page', 'redirect', 'pagelinks'])

    get_clickstream(args.table, args.lang, args.start, args.stop, priority = args.priority, min_count = args.min_count)


