from db_utils import exec_hive_stat2
from db_utils import execute_hive_expression,get_hive_timespan
import argparse
from sqoop_utils import sqoop_prod_dbs


"""
Usage:

python get_clickstream.py \
    --start 2016-03-01 \
    --stop  2016-03-31 \
    --table 2016_03 \
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
    DROP TABLE IF EXISTS clickstream.%(table)s;
    CREATE TABLE clickstream.%(table)s
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    AS SELECT
        prev,
        curr,
        CASE
            WHEN prev  in ('other-wikipedia', 'other-google', 'other-yahoo', 'other-empty', 'other-other', 'other-facebook', 'other-twitter', 'other-bing', 'other-internal') THEN 'external'
            WHEN l.pl_from IS NOT NULL AND l.pl_to IS NOT NULL THEN 'link'
            ELSE 'other'
        END AS type,
        n
    FROM
        (SELECT 
            curr, prev, n
        FROM
            (SELECT
                curr, prev, SUM(n) as n
            FROM
                (SELECT 
                    CASE
                        WHEN prev  in ('other-wikipedia', 'other-google', 'other-yahoo', 'other-empty', 'other-other', 'other-facebook', 'other-twitter', 'other-bing', 'other-internal') THEN prev
                        WHEN pr.rd_to IS NULL THEN prev
                        ELSE pr.rd_to
                    END AS prev,
                    CASE
                        WHEN cr.rd_to IS NULL THEN curr
                        ELSE cr.rd_to
                    END AS curr,
                    n
                FROM
                    (SELECT
                        curr, prev, COUNT(*) as n
                    FROM
                        (SELECT 
                            REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1) as curr,
                            CASE
                                WHEN referer == '' THEN 'other-empty'
                                WHEN referer == '-' THEN 'other-empty'
                                WHEN referer IS NULL THEN 'other-empty'
                                WHEN parse_url(referer,'HOST') is NULL THEN 'other-empty'
                                WHEN parse_url(referer,'HOST') RLIKE 'google.' THEN 'other-google'
                                WHEN parse_url(referer,'HOST') RLIKE 'yahoo.' THEN 'other-yahoo'
                                WHEN parse_url(referer,'HOST') RLIKE 'facebook.' THEN 'other-facebook'
                                WHEN parse_url(referer,'HOST') RLIKE 'twitter.' THEN 'other-twitter'
                                WHEN parse_url(referer,'HOST') RLIKE 't.co' THEN 'other-twitter'
                                WHEN parse_url(referer,'HOST') RLIKE 'bing.' THEN 'other-bing'
                                WHEN 
                                    parse_url(referer,'HOST') in ('%(lang)s.wikipedia.org', '%(lang)s.m.wikipedia.org')
                                    AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
                                THEN REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)
                                WHEN parse_url(referer,'HOST') RLIKE '.wikipedia.org' THEN 'other-wikipedia'
                                WHEN parse_url(referer,'HOST') RLIKE '\\.wiki.*\\.org' THEN 'other-internal'
                                ELSE 'other-other'
                            END as prev
                            FROM wmf.webrequest
                        WHERE 
                            %(time_conditions)s
                            AND webrequest_source = 'text'
                            AND normalized_host.project_class = 'wikipedia'
                            AND normalized_host.project = '%(lang)s'
                            AND is_pageview 
                            AND LENGTH(REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1)) > 0
                            AND agent_type = 'user'
                        ) pc0 -- extract pageviews and re-map referer
                    GROUP BY 
                        curr, prev
                    ) pc1  -- group by curr, prev
                LEFT JOIN
                    clickstream.%(lang)s_redirect pr ON (pc1.prev = pr.rd_from)
                LEFT JOIN
                    clickstream.%(lang)s_redirect cr ON (pc1.curr = cr.rd_from)
                ) pc2 --resolve redirects
            GROUP BY
                curr, prev
            HAVING
                SUM(n) > %(min_count)s
            ) pc3 -- re-aggregate and drop pairs under min count
        LEFT JOIN
            clickstream.%(lang)s_page_raw pp ON (pc3.prev = pp.page_title)
        LEFT JOIN
            clickstream.%(lang)s_page_raw cp ON (pc3.curr = cp.page_title)
        WHERE
            cp.page_title is not NULL
            AND ( pp.page_title is NOT NULL
                  OR prev  in ('other-wikipedia', 'other-google', 'other-yahoo', 'other-empty', 'other-other', 'other-facebook', 'other-twitter', 'other-bing', 'other-internal')
                )
        ) pc4 -- only include main namespace articles
    LEFT JOIN
        clickstream.%(lang)s_pagelinks l ON (pc4.prev = l.pl_from AND pc4.curr = l.pl_to)
    WHERE 
        curr != prev -- drop self loops
    -- annotate link types
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


