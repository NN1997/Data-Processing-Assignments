CREATE TABLE users 
(
userid INTEGER, 
name CHAR(40) NOT NULL, 
PRIMARY KEY (userid)
);


CREATE TABLE movies 
(
movieid INTEGER, 
title CHAR(40) NOT NULL, 
PRIMARY KEY (movieid)
);


CREATE TABLE taginfo 
(
tagid INTEGER, 
content CHAR(100) NOT NULL, 
PRIMARY KEY (tagid)
);


CREATE TABLE genres 
(
genreid INTEGER, 
name CHAR(12) NOT NULL, 
PRIMARY KEY(genreid)
);


CREATE TABLE ratings
(
userid INTEGER, 
movieid INTEGER, 
rating FLOAT NOT NULL CHECK (0<=rating AND rating<=5), 
timestamp bigint NOT NULL DEFAULT(UTC_TIMESTAMP()), 
FOREIGN KEY(userid) REFERENCES users(userid),
FOREIGN KEY(movieid) REFERENCES movies(movieid), 
UNIQUE(userid,movieid)
);


CREATE TABLE tags 
(
userid INTEGER , 
movieid INTEGER, 
tagid INTEGER, 
timestamp bigint NOT NULL DEFAULT(UTC_TIMESTAMP()),
FOREIGN KEY(userid) REFERENCES users(userid), 
FOREIGN KEY(movieid) REFERENCES movies(movieid),
FOREIGN KEY(tagid) REFERENCES taginfo(tagid)
);
 
 
CREATE TABLE hasagenre
(
movieid INTEGER,
genreid INTEGER,
FOREIGN KEY(movieid) REFERENCES movies(movieid),
FOREIGN KEY(genreid) REFERENCES genres(genreid)
);