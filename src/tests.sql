python get_clickstream.py \
     --start 2016-09-01     \
     --stop  2016-09-01     \
     --table test_en     \
     --lang en     \
     --min_count 0 \
     --priority

select * from test_en_temp1 limit 10;
select count(*) from test_en_temp1;
9895753

select * from test_en_temp2 limit 10;
select sum(n) from test_en_temp2;
9895753

# expect reduction temp3 due to loss of non main pvs

select * from test_en_temp3 limit 10;
select sum(n) from test_en_temp3;
9403089

# expect no reduction, just resolving curr redirects

select * from test_en_temp4 limit 10;
select sum(n) from test_en_temp4;
9403089

# check that the redirect 'Obama' gets resolved

select * from en_redirect where rd_from_page_title = 'Obama' and rd_from_page_namespace = 0;
12736609    0   true    Obama   534366  0   false   Barack_Obama

select * from test_en_temp3 where curr = 12736609;
#lots of rows
select * from test_en_temp4 where curr = 12736609;
# no rows




# normally expect large reduction to due removal of infrequent pairs. Here
# we filtered a 0, so there should be no reduction.

select * from test_en_temp5 limit 10;
select sum(n) from test_en_temp5;
9403089



# check if resolving redirects led to higher count for Barack_Obama article
select * from test_en_temp3 where curr = 534366 and prev = -1;
-1  534366  334

select * from test_en_temp5 where curr = 534366 and prev = -1;
-1  534366  366


# should be same as temp5, just annotating link types
select * from test_en_temp6 limit 10;
select sum(n) from test_en_temp6;
9403089

select type, sum(n) from test_en_temp6 group by type;
external    7468650
link    1755049
other   179390




# check that link merging gets done properly


# pick a pair where curr is a redirect
select  * from test_en_temp3, en_pagelinks
where prev = pl_from_page_id and curr = pl_to_page_id
and pl_to_page_is_redirect = True and pl_from_page_is_redirect = False
limit 10;
594 9316461 1   594 0   false   Apollo  9316461 0   true    First_Vatican_Mythographer

# see what curr redirects to
select * from en_redirect where rd_from_page_title = 'First_Vatican_Mythographer' and rd_from_page_namespace = 0;
9316461 0   true    First_Vatican_Mythographer  9316394 0   false   Vatican_Mythographers


# make sure there is not a link from prev to the resolved curr
select * from en_pagelinks where pl_from_page_id =  594 and pl_to_page_id = 9316394;
# should be empty,

# see if unresolved pair is still in the data after resolving redirects
select * from test_en_temp6 where prev = 594 and curr = 9316461;
# should be empty, since 9316461 is a redirect

# see if resolved pair is in the data after resolving redirects
select * from test_en_temp6 where prev = 594 and curr = 9316394;
# should have one line and link type should  be link
594 9316394 link    1

select * from test_en where prev = 'Apollo' and curr = 'Vatican_Mythographers';
# should have one line and link type should  be link
Apollo  Vatican_Mythographers   link    1




# expect small reduction, removal of self loops
select * from test_en limit 10;
select sum(n) from test_en;
9357342

select prev, sum(n) from test_en where type = 'external' group by prev;
prev    _c1
other-empty 2951764
other-external  216751
other-internal  145121
other-other 252
other-search    4154596



# Make sure none of the page titles are redirects
select count(*) from test_en join en_redirect on curr == rd_from_page_title where rd_from_page_namespace = 0;
_c0
0
select count(*) from test_en join en_redirect on prev == rd_from_page_title where rd_from_page_namespace = 0 ;
_c0
0