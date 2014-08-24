-- San Francisco
CREATE VIEW sfroom as 
select * from room 
where active = 1 
  and price is not null;

-- View by hosts
CREATE VIEW sfhost as 
select host_id, 
count(*) rooms, 
case when count(*) > 1 then 1 else 0 end as multilister,
sum(reviews) revs, 
count(distinct address) addresses, 
sum(reviews * price) income1, 
sum(reviews * price * minstay) income2 
from sfroom group by host_id;

-- Breakdown of hosts by number of listings per host
select multilister, count(*) hosts
from sfhost
group by multilister
order by multilister asc;

-- Breakdown of listings by listings per host
select multilister, sum(rooms) total_rooms 
from sfhost
group by multilister
order by rooms asc;

-- Breakdown of bookings by listings per host
select multilister, sum(revs) bookings 
from sfhost
group by multilister
order by rooms asc;

-- Breakdown of listings by room type
select room_type, count(*) listings
from sfroom
group by room_type;

-- Breakdown of bookings by room type
select room_type, sum(reviews) bookings
from sfroom
group by room_type;

-- Breakdown of revenue by room type
select room_type, 
    sum(reviews * price) revenue_1, 
    sum(reviews * price * minstay) revenue_2
from sfroom
group by room_type;

-- Average number of bookings
select room_type, avg(reviews) avg_bookings
from sfroom
group by room_type;

-- Stories: max number of listings
select host_id, rooms
from sfhost
order by rooms desc
limit 10;

-- Stories: max number of bookings
select host_id, revs
from sfhost
order by revs desc
limit 10;

-- Stories: max revenue estimate
select host_id, sum(reviews * price * minstay) revenue_2
from sfhost
order by revs desc
limit 10;
