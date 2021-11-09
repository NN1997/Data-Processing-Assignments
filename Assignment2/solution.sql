create table query1(name varchar(200),moviecount bigint) as (select g.name as name, count(h.movieid) as moviecount from genres g left join hasagenre h on g.genreid = h.genreid group by h.genreid); 

create table query2(name varchar(200), rating decimal(20,16)) as (select g.name as name, avg(r.rating) as rating from hasagenre h join ratings r on r.movieid=h.movieid right join genres g on h.genreid=g.genreid group by h.genreid);

create table temp3(title varchar(200), countofratings bigint) as (select m.title,count(r.rating) as countofratings from movies m, ratings r where r.movieid=m.movieid group by r.movieid); 
create table query3(title varchar(200), countofratings bigint) as (select * from temp3 where countofratings>=10);

create table query4(movieid integer,title varchar(200)) as select movieid,title from movies where movieid in(select movieid from hasagenre where genreid=(select genreid from genres where name='Comedy'));

create table query5(title varchar(200),average decimal(20,16)) as (select m.title,avg(r.rating) as average from ratings r, movies m where r.movieid=m.movieid group by r.movieid);

create table query6(average decimal(20,16)) as (select avg(r.rating) as average from ratings r, hasagenre h where r.movieid=(select h.movieid where h.genreid=(select genreid from genres where name='Comedy')));

create table query7(average decimal(20,16)) as (select avg(r.rating) as average from ratings r, hasagenre h where r.movieid in(select movieid from hasagenre where genreid=(select genreid from genres where name='Comedy') intersect select movieid from hasagenre where genreid=(select genreid from genres where name='Romance')));

create table query8(average decimal(20,16)) as (select avg(rating) as average from ratings where movieid in (select distinct movieid from hasagenre where movieid in (select movieid from hasagenre where genreid=(select genreid from genres where name='Romance')) and movieid not in (select movieid from hasagenre where genreid=(select genreid from genres where name='Comedy'))));

set @v1=6;
create table query9(movieid integer, rating decimal(20,16)) as (select movieid, rating from ratings where userid = @v1);