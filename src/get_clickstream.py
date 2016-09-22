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

    -- series of helper tables to simplify process of restricting 
    -- to main namespace, resolving redirects and annotating link types


    -- page table
    -- only contains main name space pages (redirects or articles)

    DROP TABLE IF EXISTS clickstream.%(table)s_page_helper;
    CREATE TABLE clickstream.%(table)s_page_helper AS
    SELECT
        page_title
    FROM
        clickstream.%(lang)s_page
    WHERE
        page_namespace = 0
    ;




    -- article table
    -- only contains main name space articles ( no redirects )

    DROP TABLE IF EXISTS clickstream.%(table)s_article_helper;
    CREATE TABLE clickstream.%(table)s_article_helper AS
    SELECT
        page_title
    FROM
        clickstream.%(lang)s_page
    WHERE
        page_namespace = 0
        AND page_is_redirect = false
    ;



    -- redirect table
    -- only contains redirects between a main ns pages

    DROP TABLE IF EXISTS clickstream.%(table)s_redirect_helper;
    CREATE TABLE clickstream.%(table)s_redirect_helper AS
    SELECT
        rd_from_page_title,
        rd_to_page_title
    FROM
        clickstream.%(lang)s_redirect 
    WHERE
        rd_from_page_namespace = 0
        AND rd_to_page_namespace = 0
    ;



    -- pagelinks table
    -- contains links between main namespace articles
    -- the page linked from is always an article
    -- if the page linked to is a redirect, it is resolved using redirect_helper


    DROP TABLE IF EXISTS clickstream.%(table)s_pagelinks_helper;
    CREATE TABLE clickstream.%(table)s_pagelinks_helper AS
    SELECT
        pl_from_page_title,
        pl_to_page_title
    FROM
        (SELECT
            pl_from_page_title,
            CASE
                WHEN r.rd_to_page_title IS NULL THEN pl_to_page_title
                ELSE rd_to_page_title
            END AS pl_to_page_title
        FROM
            clickstream.%(lang)s_pagelinks l
        LEFT JOIN
            clickstream.%(table)s_redirect_helper r ON (r.rd_from_page_title = l.pl_to_page_title)
        WHERE
            pl_from_page_is_redirect = false
            AND pl_from_page_namespace = 0
            AND pl_to_page_namespace = 0
        ) a
    GROUP BY
        pl_from_page_title,
        pl_to_page_title
    ;

    -- ############################################




    -- extract raw prev, curr pairs

    DROP TABLE IF EXISTS clickstream.%(table)s_temp1;
    CREATE TABLE clickstream.%(table)s_temp1 AS
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
        AND hour = 1
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



    -- we enforce that prev is an article and not a redirect: the reader has to be on a real page to click a link or do a search.
    -- we enforce that curr is a main namespace article or redirect
    -- the joins accomplish this because, in the logs, the non main namespace pages have the namespace prepended

    DROP TABLE IF EXISTS clickstream.%(table)s_temp3;
    CREATE TABLE clickstream.%(table)s_temp3 AS
    SELECT 
        prev,
        curr,
        n
    FROM
        clickstream.%(table)s_temp2
    LEFT JOIN
        clickstream.%(table)s_article_helper a ON (prev = a.page_title)
    JOIN
        clickstream.%(table)s_page_helper p ON (curr = p.page_title)
    WHERE
        ( a.page_title is NOT NULL
          OR 
          prev  in ('other-empty', 'other-internal', 'other-external', 'other-search', 'other-other')
        )
    ;



    -- resolve curr redirects, one step
    -- note that prev cannot be a redirect, so it does not need to be resolved

    DROP TABLE IF EXISTS clickstream.%(table)s_temp4;
    CREATE TABLE clickstream.%(table)s_temp4 AS
    SELECT 
        prev,
        CASE
            WHEN rd_to_page_title IS NULL THEN curr
            ELSE rd_to_page_title
        END AS curr,
        n
    FROM
        clickstream.%(table)s_temp3
    LEFT JOIN
        clickstream.%(table)s_redirect_helper ON (curr = rd_from_page_title)
    ;

    --  remove any rows where curr is still a redirect 
    --  this means  curr is the start of a redirect chain of length >= 2

    DROP TABLE IF EXISTS clickstream.%(table)s_temp5;
    CREATE TABLE clickstream.%(table)s_temp5 AS
    SELECT 
        prev,
        curr,
        n
    FROM
        clickstream.%(table)s_temp4
    LEFT JOIN
        clickstream.%(table)s_redirect_helper ON (curr = rd_from_page_title)
    WHERE
        rd_from_page_title is NULL
    ;




    -- re-aggregate after resolving redirects and filter out pairs that occur infrequently
    DROP TABLE IF EXISTS clickstream.%(table)s_temp6;
    CREATE TABLE clickstream.%(table)s_temp6 AS
    SELECT
        prev, curr, SUM(n) as n
    FROM
        clickstream.%(table)s_temp5
    GROUP BY
        prev, curr
    HAVING
        SUM(n) > %(min_count)s
    ;



    -- annotate link types

    DROP TABLE IF EXISTS clickstream.%(table)s_temp7;
    CREATE TABLE clickstream.%(table)s_temp7 AS
    SELECT
        prev,
        curr,
        CASE
            WHEN prev in ('other-empty', 'other-internal', 'other-external', 'other-search', 'other-other') THEN 'external'
            WHEN (pl_from_page_title IS NOT NULL AND pl_to_page_title IS NOT NULL) THEN 'link'
            ELSE 'other'
        END AS type,
        n
    FROM
        clickstream.%(table)s_temp6
    LEFT JOIN
        clickstream.%(table)s_pagelinks_helper ON (prev = pl_from_page_title AND curr = pl_to_page_title)
    ;


    -- create final table
    -- remove self loops

    DROP TABLE IF EXISTS clickstream.%(table)s;
    CREATE TABLE clickstream.%(table)s
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE AS
    SELECT
        *
    FROM
        clickstream.%(table)s_temp7
    WHERE 
        curr != prev
    ;

    DROP TABLE clickstream.%(table)s_temp1;
    DROP TABLE clickstream.%(table)s_temp2;
    DROP TABLE clickstream.%(table)s_temp3;
    DROP TABLE clickstream.%(table)s_temp4;
    DROP TABLE clickstream.%(table)s_temp5;
    DROP TABLE clickstream.%(table)s_temp6
    DROP TABLE clickstream.%(table)s_page_helper;
    DROP TABLE clickstream.%(table)s_article_helper;
    DROP TABLE clickstream.%(table)s_redirect_helper;
    DROP TABLE clickstream.%(table)s_pagelinks_helper;
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


