select * from test_en_temp1 limit 10;
select count(*) from test_en_temp1;
9895753

select * from test_en_temp2 limit 10;
select sum(n) from test_en_temp2;
9895753

# expect reduction temp3 due to loss of non main pvs

select * from test_en_temp3 limit 10;
select sum(n) from test_en_temp3;
9388785

# expect no reduction, just resolving curr redirects

select * from test_en_temp4 limit 10;
select sum(n) from test_en_temp4;
9388785

# check that the redirect '!!_(disambiguation)' gets resolved
select * from test_en_temp3 where curr = '!!_(disambiguation)';
other-empty !!_(disambiguation) 3

select * from test_en_temp4 where curr = '!!_(disambiguation)';
-

# expect small reduction to due removal of redirect chains
select * from test_en_temp5 limit 10;
select sum(n) from test_en_temp5;
9388702


# expect large reduction to due removal of infrequent pairs
select * from test_en_temp6 limit 10;
select sum(n) from test_en_temp6;
4675717



# check if resolving redirects led to higher count for one article
select * from test_en_temp6 where curr = 'United_States' and prev = 'other-empty';
other-empty United_States   901


select * from test_en_temp3 where curr = 'United_States' and prev = 'other-empty';
other-empty United_States   670



# should be same as temp6, just annotating link types
select * from test_en_temp7 limit 10;
select sum(n) from test_en_temp7;
4675717

select type, sum(n) from test_en_temp7 group by type;
external    4457704
link    189467
other   28546

select * from test_en_temp7 where type = 'other' order by n desc limit 10;

# expect small reduction, removal of self loops
select * from test_en limit 10;
select sum(n) from test_en;
4664050

# Make sure none of the page titles are redirects
select count(*) from test_en join en_redirect on curr == rd_from_page_title where rd_from_page_namespace = 0;
_c0
0

select count(*) from test_en join en_redirect on prev == rd_from_page_title where rd_from_page_namespace = 0;
_c0
0

