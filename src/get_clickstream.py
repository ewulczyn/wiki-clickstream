from db_utils import exec_hive_stat2
from db_utils import execute_hive_expression,get_hive_timespan
import argparse
from sqoop_utils import sqoop_prod_dbs


"""
Usage:

python get_clickstream.py \
    --start 2016-04-01 \
    --stop  2016-04-30 \
    --table 2016_04_en \
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
        AND agent_type = 'user';


    -- count raw prev, curr pairs
    DROP VIEW IF EXISTS clickstream.%(table)s_temp2;
    CREATE VIEW clickstream.%(table)s_temp2 AS
    SELECT
        curr, prev, COUNT(*) as n
    FROM
        clickstream.%(table)s_temp1
    GROUP BY 
        curr, prev;


    -- resolve redirects
    DROP VIEW IF EXISTS clickstream.%(table)s_temp3;
    CREATE VIEW clickstream.%(table)s_temp3 AS
    SELECT 
        CASE
            WHEN prev  in ('other-empty', 'other-internal', 'other-external', 'other-search', 'other-other') THEN prev
            WHEN pr.rd_to IS NULL THEN prev
            ELSE pr.rd_to
        END AS prev,
        CASE
            WHEN cr.rd_to IS NULL THEN curr
            ELSE cr.rd_to
        END AS curr,
        n
    FROM
        clickstream.%(table)s_temp2
    LEFT JOIN
        clickstream.%(lang)s_redirect pr ON (prev = pr.rd_from)
    LEFT JOIN
        clickstream.%(lang)s_redirect cr ON (curr = cr.rd_from);

    -- re-aggregate after resolving redirects and filter out pairs that occur infrequently
    DROP VIEW IF EXISTS clickstream.%(table)s_temp4;
    CREATE VIEW clickstream.%(table)s_temp4 AS
    SELECT
        curr, prev, SUM(n) as n
    FROM
        clickstream.%(table)s_temp3
    GROUP BY
        curr, prev
    HAVING
        SUM(n) > %(min_count)s;

    -- only include main namespace articles
    DROP VIEW IF EXISTS clickstream.%(table)s_temp5;
    CREATE VIEW clickstream.%(table)s_temp5 AS
    SELECT 
        curr, prev, n
    FROM
        clickstream.%(table)s_temp4
    LEFT JOIN
        clickstream.%(lang)s_page_raw pp ON (prev = pp.page_title)
    LEFT JOIN
        clickstream.%(lang)s_page_raw cp ON (curr = cp.page_title)
    WHERE
        cp.page_title is not NULL
        AND ( pp.page_title is NOT NULL
              OR prev  in ('other-empty', 'other-internal', 'other-external', 'other-search', 'other-other')
            );

    -- annotate link types
    DROP VIEW IF EXISTS clickstream.%(table)s_temp6;
    CREATE VIEW clickstream.%(table)s_temp6 AS
    SELECT
        prev,
        curr,
        CASE
            WHEN prev  in ('other-empty', 'other-internal', 'other-external', 'other-search', 'other-other') THEN 'external'
            WHEN l.pl_from IS NOT NULL AND l.pl_to IS NOT NULL THEN 'link'
            ELSE 'other'
        END AS type,
        n
    FROM
        clickstream.%(table)s_temp5
    LEFT JOIN
        clickstream.%(lang)s_pagelinks l ON (prev = l.pl_from AND curr = l.pl_to);


    -- create table
    DROP TABLE IF EXISTS clickstream.%(table)s;
    CREATE TABLE clickstream.%(table)s
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE AS
    SELECT
        *
    FROM
        clickstream.%(table)s_temp6
    WHERE 
        curr != prev;

    DROP VIEW clickstream.%(table)s_temp1;
    DROP VIEW clickstream.%(table)s_temp2;
    DROP VIEW clickstream.%(table)s_temp3;
    DROP VIEW clickstream.%(table)s_temp4;
    DROP VIEW clickstream.%(table)s_temp5;
    DROP VIEW clickstream.%(table)s_temp6;
    """

    print(query % params)
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


