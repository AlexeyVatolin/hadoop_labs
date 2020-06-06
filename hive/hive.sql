with questions as (
    select id as question_id, AcceptedAnswerId, 
    unix_timestamp(creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") as q_creationdate 
    from posts
    where posttypeid = 1
),
answers as (
    select id as answer_id, unix_timestamp(creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") as a_creationdate, 
    owneruserid from posts
    where posttypeid = 2
)
select qa.*, u.displayname, u.reputation from (
    select owneruserid, avg((a_creationdate - q_creationdate) / 60) as mean_time_delta,
    count(*) as answers_count from questions q
    inner join answers a on AcceptedAnswerId=answer_id
    where (a_creationdate - q_creationdate) / 60 > 0.3
    group by owneruserid
    having count(*) > 1
) as qa
inner join users u on u.id=qa.owneruserid
order by mean_time_delta

-- 10 минут 40 секунд